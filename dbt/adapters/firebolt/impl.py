import re
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, List, Optional, Union

import agate
import dbt.exceptions
import dbt.utils
from dbt.adapters.base import available
from dbt.adapters.base.impl import AdapterConfig
from dbt.adapters.base.relation import BaseRelation
from dbt.adapters.sql import SQLAdapter
from dbt.dataclass_schema import ValidationError, dbtClassMixin

from dbt.adapters.firebolt.column import FireboltColumn
from dbt.adapters.firebolt.connections import FireboltConnectionManager
from dbt.adapters.firebolt.relation import FireboltRelation


@dataclass
class FireboltIndexConfig(dbtClassMixin):
    index_type: str
    join_columns: Optional[Union[str, List[str]]] = None
    key_columns: Optional[Union[str, List[str]]] = None
    dimension_column: Optional[Union[str, List[str]]] = None
    aggregation: Optional[Union[str, List[str]]] = None

    def render_name(self, relation):
        """
        Name an index according to the following format, joined by `_`:
        index type, relation name, key/join columns, timestamp (unix & UTC)
        example index name: join_my_model_customer_id_1633504263.
        """
        now_unix = str(int(time.mktime(datetime.utcnow().timetuple())))
        # If column_names is a list with > 1 members, join with _,
        # otherwise do not. We were getting index names like
        # join__idx__emf_customers__f_i_r_s_t___n_a_m_e__165093112.
        column_names = self.key_columns or self.join_columns
        spine_col = (
            '_'.join(column_names) if isinstance(column_names, list) else column_names
        )
        inputs = [
            f'{self.index_type}_idx',
            relation.identifier,
            spine_col,
            now_unix,
        ]
        string = '__'.join(inputs)
        return string

    @classmethod
    def parse(cls, raw_index) -> Optional['FireboltIndexConfig']:
        """
        Validate the JSON format of the provided index config.
        Ensure the config has the right elements.
        Return a valid indexable (pun intended) config object/dictionary.
        """
        if raw_index is None:
            return None
        try:
            cls.validate(raw_index)
            index_config = cls.from_dict(raw_index)
            if index_config.index_type.upper() not in ['JOIN', 'AGGREGATING']:
                dbt.exceptions.raise_compiler_error(
                    'Invalid index type:\n'
                    f'  Got: {index_config.index_type}.\n'
                    '  Type should be either: "join" or "aggregating."'
                )
            if index_config.index_type.upper() == 'JOIN' and not (
                index_config.join_columns and index_config.dimension_column
            ):
                dbt.exceptions.raise_compiler_error(
                    'Invalid join index definition:\n'
                    f'  Got: {index_config}.\n'
                    '  join_columns and dimension_column must be specified '
                    'for join indexes.'
                )
            if index_config.index_type.upper() == 'AGGREGATING' and not (
                index_config.key_columns and index_config.aggregation
            ):
                dbt.exceptions.raise_compiler_error(
                    'Invalid aggregating index definition:\n'
                    f'  Got: {index_config}.\n'
                    '  key_columns and aggregation must be specified '
                    'for aggregating indexes.'
                )
            return index_config
        except ValidationError as exc:
            msg = dbt.exceptions.validator_error_message(exc)
            dbt.exceptions.raise_compiler_error(f'Could not parse index config: {msg}.')


@dataclass
class FireboltConfig(AdapterConfig):
    indexes: Optional[List[FireboltIndexConfig]] = None


class FireboltAdapter(SQLAdapter):
    Relation = FireboltRelation
    ConnectionManager = FireboltConnectionManager
    Column = FireboltColumn

    def is_cancelable(self):
        return False

    @classmethod
    def date_function(cls):
        return 'now()'

    @available
    def parse_index(self, raw_index: Any) -> Optional[FireboltIndexConfig]:
        return FireboltIndexConfig.parse(raw_index)

    @classmethod
    def convert_number_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        decimals = agate_table.aggregate(agate.MaxPrecision(col_idx))
        return 'FLOAT' if decimals else 'INT'

    @classmethod
    def convert_boolean_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return 'INT'

    @classmethod
    def convert_datetime_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        # there's an issue with timestamp currently
        return 'DATE'

    @classmethod
    def convert_time_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        raise dbt.exceptions.NotImplementedException(
            '`convert_time_type` is not implemented for this adapter!'
        )

    @available.parse_none
    def make_field_partition_pairs(self, columns, partitions) -> List[str]:
        """
        Return a list of strings of form "column column_type" or
        "column column_type PARTITION(regex)" where the partitions
        fall at the end of the list.
        """
        unpartitioned_columns = []
        partitioned_columns = []
        for column in columns:
            # Don't need to check for name, as missing name fails at yaml parse time.
            if column.get('data_type', None) is None:
                raise dbt.exceptions.RuntimeException(
                    f'Data type is missing for column `{column["name"]}`.'
                )
            unpartitioned_columns.append(
                self.quote(column['name']) + ' ' + column['data_type']
            )
        if partitions:  # partitions may be empty.
            for partition in partitions:
                # Don't need to check for name, as missing name fails at
                # yaml parse time.
                if partition.get('data_type', None) is None:
                    raise dbt.exceptions.RuntimeException(
                        f'Data type is missing for partition `{partition["name"]}`.'
                    )
                if partition.get('regex', None) is None:
                    raise dbt.exceptions.RuntimeException(
                        f'Regex is missing for partition `{partition["name"]}`.'
                    )
                partitioned_columns.append(
                    self.quote(partition['name'])
                    + ' '
                    + partition['data_type']
                    + " PARTITION ('"
                    + partition['regex']
                    + "')"
                )
        return unpartitioned_columns + partitioned_columns

    @available.parse_none
    def stack_tables(self, tables_list) -> agate.Table:
        """
        Given a list of agate_tables with the same column names & types
        return a single unioned agate table.
        """
        non_empty_tables = [table for table in tables_list if len(table.rows) > 0]

        if len(non_empty_tables) == 0:
            return tables_list[0]
        else:
            return (
                agate.TableSet(non_empty_tables, keys=range(len(non_empty_tables)))
                .merge()
                .exclude(['group'])
            )

    @available.parse_none
    def filter_table(cls, agate_table, col_name, re_match_exp) -> agate.Table:
        """
        Filter agate table by column name and regex match expression.
        https://agate.readthedocs.io/en/latest/cookbook/filter.html#by-regex
        """
        return agate_table.where(lambda row: re.match(re_match_exp, str(row[col_name])))

    @available.parse_none
    def get_rows_different_sql(
        self,
        relation_a: BaseRelation,
        relation_b: BaseRelation,
        column_names: Optional[List[str]] = None,
        except_operator: str = 'EXCEPT',
    ) -> str:
        """
        Generate SQL for a query that returns a single row with a two
        columns: the number of rows that are different between the two
        relations and the number of mismatched rows.
        """
        # This method only really exists for test reasons.
        names: List[str]
        if column_names is None:
            columns = self.get_columns_in_relation(relation_a)
            names = sorted((self.quote(c.name) for c in columns))
        else:
            names = sorted((self.quote(n) for n in column_names))

        where_expressions = [
            f'{relation_a}.{name} = {relation_b}.{name}' for name in names
        ]
        where_clause = ' AND '.join(where_expressions)

        columns_csv = ', '.join(names)

        sql = COLUMNS_EQUAL_SQL.format(
            columns=columns_csv,
            relation_a=str(relation_a),
            relation_b=str(relation_b),
            where_clause=where_clause,
        )

        return sql


COLUMNS_EQUAL_SQL = """
WITH diff_count AS (
    SELECT
        1 AS id,
        COUNT(*) AS num_missing FROM (
            (SELECT {columns} FROM {relation_a}
            WHERE NOT EXISTS (
                SELECT NULL FROM {relation_b}
                WHERE {where_clause}
            ))
            UNION ALL
            (SELECT {columns} FROM {relation_b}
            WHERE NOT EXISTS (
                SELECT NULL FROM {relation_a}
                WHERE {where_clause}
            ))
        ) AS a
),
table_a AS (
    SELECT COUNT(*) AS num_rows FROM {relation_a}
),
table_b AS (
    SELECT COUNT(*) AS num_rows FROM {relation_b}
),
row_count_diff AS (
    SELECT
        1 AS id,
        table_a.num_rows - table_b.num_rows AS difference
    FROM table_a, table_b
)
SELECT
    row_count_diff.difference AS row_count_difference,
    diff_count.num_missing AS num_mismatched
FROM row_count_diff
JOIN diff_count
  ON diff_count.id = row_count_diff.id
""".strip()
