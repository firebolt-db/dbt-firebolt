import re
import time
from dataclasses import dataclass
from typing import Any, List, Mapping, Optional, Union

import agate
from dbt.adapters.base.impl import ConstraintSupport
from dbt.adapters.base.meta import available
from dbt.adapters.base.relation import BaseRelation
from dbt.adapters.capability import (
    Capability,
    CapabilityDict,
    CapabilitySupport,
    Support,
)
from dbt.adapters.protocol import AdapterConfig
from dbt.adapters.sql.impl import SQLAdapter
from dbt_common.contracts.constraints import ConstraintType
from dbt_common.dataclass_schema import ValidationError, dbtClassMixin
from dbt_common.exceptions import (
    CompilationError,
    DbtRuntimeError,
    NotImplementedError,
)

from dbt.adapters.firebolt.column import FireboltColumn
from dbt.adapters.firebolt.connections import FireboltConnectionManager
from dbt.adapters.firebolt.relation import FireboltRelation


def quote_columns(columns: Union[str, List[str]]) -> Union[str, List[str]]:
    if isinstance(columns, str):
        return f'"{columns}"'
    quoted_columns = []
    for col in columns:
        if col.startswith('"') and col.endswith('"'):
            quoted_columns.append(col)
        else:
            quoted_columns.append(f'"{col}"')
    return quoted_columns


@dataclass
class FireboltIndexConfig(dbtClassMixin):
    index_type: str
    index_name: Optional[str] = None
    join_columns: Optional[Union[str, List[str]]] = None
    key_columns: Optional[Union[str, List[str]]] = None
    dimension_column: Optional[Union[str, List[str]]] = None
    aggregation: Optional[Union[str, List[str]]] = None

    def __post_init__(self) -> None:
        # quote unquoted columns
        if self.join_columns:
            self.join_columns = quote_columns(self.join_columns)
        if self.key_columns:
            self.key_columns = quote_columns(self.key_columns)
        if self.dimension_column:
            self.dimension_column = quote_columns(self.dimension_column)

    def render_name(self, relation: FireboltRelation) -> str:
        """
        Name an index according to the following format, joined by `_`:
        index type, relation name, key/join columns, timestamp (unix & UTC)
        example index name: join_my_model_customer_id_1633504263.
        """
        if self.index_name:
            return self.index_name
        now_unix = str(int(time.time()))
        # If column_names is a list with > 1 members, join with _,
        # otherwise do not. We were getting index names like
        # join__idx__emf_customers__f_i_r_s_t___n_a_m_e__165093112.
        column_names = self.key_columns or self.join_columns
        spine_col = (
            '_'.join(column_names) if isinstance(column_names, list) else column_names
        )
        # Additional hash to ensure uniqueness after spaces are removed
        column_hash = str(hash(spine_col))[:5]
        inputs = [
            f'{self.index_type}_idx',
            relation.identifier,
            spine_col,
            column_hash,
            now_unix,
        ]
        index_name = '__'.join([x for x in inputs if x is not None])
        if len(index_name) > 255:
            # Firebolt index names must be <= 255 characters.
            # If the index name is too long, hash it.
            return f'{self.index_type}_idx_{hash(index_name)}'
        # Remove any spaces or quotes from the index name.
        index_name = index_name.replace(' ', '_').replace('"', '')
        return index_name

    @classmethod
    def parse(cls, raw_index: Optional[Mapping]) -> Optional['FireboltIndexConfig']:
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
                raise CompilationError(
                    'Invalid index type:\n'
                    f'  Got: {index_config.index_type}.\n'
                    '  Type should be either: "join" or "aggregating."'
                )
            if index_config.index_type.upper() == 'JOIN' and not (
                index_config.join_columns and index_config.dimension_column
            ):
                raise CompilationError(
                    'Invalid join index definition:\n'
                    f'  Got: {index_config}.\n'
                    '  join_columns and dimension_column must be specified '
                    'for join indexes.'
                )
            if index_config.index_type.upper() == 'AGGREGATING' and not (
                index_config.aggregation
            ):
                raise CompilationError(
                    'Invalid aggregating index definition:\n'
                    f'  Got: {index_config}.\n'
                    '  aggregation must be specified for aggregating indexes.'
                )
            return index_config
        except ValidationError as exc:
            msg = DbtRuntimeError('').validator_error_message(exc)
            raise CompilationError(f'Could not parse index config: {msg}.')
        return None


@dataclass
class FireboltConfig(AdapterConfig):
    indexes: Optional[List[FireboltIndexConfig]] = None


class FireboltAdapter(SQLAdapter):
    Relation = FireboltRelation
    ConnectionManager = FireboltConnectionManager
    Column = FireboltColumn

    CONSTRAINT_SUPPORT = {
        ConstraintType.check: ConstraintSupport.NOT_SUPPORTED,
        ConstraintType.not_null: ConstraintSupport.ENFORCED,
        ConstraintType.unique: ConstraintSupport.NOT_ENFORCED,
        ConstraintType.primary_key: ConstraintSupport.NOT_SUPPORTED,
        ConstraintType.foreign_key: ConstraintSupport.NOT_SUPPORTED,
    }

    _capabilities: CapabilityDict = CapabilityDict(
        {
            Capability.SchemaMetadataByRelations: CapabilitySupport(
                support=Support.Full
            ),
            Capability.TableLastModifiedMetadata: CapabilitySupport(
                support=Support.Unsupported
            ),
        }
    )

    @classmethod
    def is_cancelable(cls) -> bool:
        return False

    @classmethod
    def date_function(cls) -> str:
        return 'now()'

    @available
    def parse_index(self, raw_index: Any) -> Optional[FireboltIndexConfig]:
        return FireboltIndexConfig.parse(raw_index)

    @classmethod
    def convert_number_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        decimals = agate_table.aggregate(agate.MaxPrecision(col_idx))
        return 'FLOAT' if decimals else 'INT'

    @classmethod
    def convert_datetime_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return 'DATETIME'

    @classmethod
    def convert_time_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        raise NotImplementedError(
            '`convert_time_type` is not implemented for this adapter!'
        )

    @available.parse_none
    def make_field_partition_pairs(
        self, columns: agate.Column, partitions: FireboltRelation
    ) -> List[str]:
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
                raise DbtRuntimeError(
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
                    raise DbtRuntimeError(
                        f'Data type is missing for partition `{partition["name"]}`.'
                    )
                if partition.get('regex', None) is None:
                    raise DbtRuntimeError(
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
    def stack_tables(self, tables_list: List[agate.Table]) -> agate.Table:
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
    def raise_grant_error(self) -> None:
        """
        Grant is not currently supported so this function
            raises an error.
        """
        raise CompilationError(
            'Firebolt does not support table-level permission grants.'
            ' Please remove grants section from the config.'
        )

    @available.parse_none
    def resolve_special_columns(self, column: str) -> str:
        """
        Resolve special columns types that dbt is unable to parse natively.
        """

        def strip_suffix(string: str, suffix: str) -> str:
            """
            Defined for backwards-compatibility with python 3.7
            """
            if string.endswith(suffix):
                return string[: -len(suffix)]
            return string

        type_code_str = '{}'
        while column.startswith('ARRAY'):
            type_code_str = f'ARRAY({type_code_str})'
            column = column[6:-1]  # Strip ARRAY()
            column = strip_suffix(column, ' NOT NULL')
            column = strip_suffix(column, ' NULL')
        return type_code_str.format(column)

    @available.parse_none
    def filter_table(
        cls, agate_table: agate.Table, col_name: str, re_match_exp: str
    ) -> agate.Table:
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

    @available.parse_none
    def annotate_date_columns_for_partitions(
        self,
        vals: str,
        col_names: Union[List[str], str],
        col_types: List[FireboltColumn],
    ) -> str:
        """
        Return a list of partition values as a single string. All columns with
        date types will be be suffixed with ::DATE.
        Args:
          vals: a string of values separated by commas
          col_names: either a list of strings or a single string, of the
            names of the columns
          col_types: Each FireboltColumn has fields for the name of the column
            and its type.
        """
        vals_list = vals.split(',')
        # It's possible that col_names will be single column, in which case
        # it might come in as a string.
        if type(col_names) is str:
            col_names = [col_names]
        # Now map from column name to column type.
        type_dict = {c.name: c.dtype for c in col_types}
        for i in range(len(vals_list)):
            if col_names[i] in type_dict and type(type_dict[col_names[i]]) in [
                'datetime',
                'date',
            ]:
                vals_list[i] += '::DATE'
        return ','.join(vals_list)

    def valid_incremental_strategies(self) -> List[str]:
        return ['append', 'insert_overwrite']

    def get_columns_in_relation(
        self, relation: BaseRelation
    ) -> Union[agate.Table, List]:
        try:
            return super().get_columns_in_relation(relation)
        except DbtRuntimeError as e:
            if 'Did not find a table or view' in str(e):
                return []
            else:
                raise

    @available.parse_none
    def get_column_class(self) -> type:
        """
        Method to expose FireboltColumn to jinja
        """
        return self.Column


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
table_a__dbt_tmp AS (
    SELECT COUNT(*) AS num_rows FROM {relation_a}
),
table_b__dbt_tmp AS (
    SELECT COUNT(*) AS num_rows FROM {relation_b}
),
row_count_diff AS (
    SELECT
        1 AS id,
        table_a__dbt_tmp.num_rows -
            table_b__dbt_tmp.num_rows AS difference
    FROM table_a__dbt_tmp, table_b__dbt_tmp
)
SELECT
    row_count_diff.difference AS row_count_difference,
    diff_count.num_missing AS num_mismatched
FROM row_count_diff
JOIN diff_count
  ON diff_count.id = row_count_diff.id
""".strip()
