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
from firebolt.async_db._types import ARRAY
from firebolt.async_db._types import Column as SDKColumn

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

    def render_name(self, relation: FireboltRelation) -> str:
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
    def parse(cls, raw_index: Optional[str]) -> Optional['FireboltIndexConfig']:
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
        return None


@dataclass
class FireboltConfig(AdapterConfig):
    indexes: Optional[List[FireboltIndexConfig]] = None


class FireboltAdapter(SQLAdapter):
    Relation = FireboltRelation
    ConnectionManager = FireboltConnectionManager
    Column = FireboltColumn

    def is_cancelable(self) -> bool:
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
    def convert_boolean_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return 'INT'

    @classmethod
    def convert_datetime_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        """
        Return the type in the database that best maps to the agate.DateTime
        type for the given agate table and column index.

        :param agate_table: The table
        :param col_idx: The index into the agate table for the column.
        :return: The name of the type in the database

        As there's only one available type, don't need the agate table, class, etc.
        """
        return 'DATETIME'

    @classmethod
    def convert_date_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        """
        Return the type in the database that best maps to the agate.Date
        type for the given agate table and column index.

        :param agate_table: The table
        :param col_idx: The index into the agate table for the column.
        :return: The name of the type in the database

        As there's only one type available type, don't need the agate table, class, etc.
        """
        return 'DATE'

    @classmethod
    def convert_time_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        """
        Return the type in the database that best maps to the
        agate.TimeDelta type for the given agate table and column index.
        Args:
          agate_table: The table
          col_idx: The index into the agate table for the column.

        Firebolt doesn't have a TIME type. Closest is DATETIME. Likewise, as
        there's only one available type, don't need the agate table, class, etc.
        """
        return 'DATETIME'

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
    def stack_tables(self, tables_list: List[agate.Table]) -> agate.Table:
        """
        Given a list of agate tables with the same column names & types
        return a single unioned agate table.
        """
        non_empty_tables = [table for table in tables_list if len(table.rows) > 0]
        print('\n\n** stack_tables', len(non_empty_tables))
        if len(non_empty_tables) == 0:
            return tables_list[0]
        else:
            print('non_empty_tables type', type(non_empty_tables))
            for col in non_empty_tables[:1]:
                print(col)
                for c in col:
                    for cc in c:
                        print(type(cc), cc)
            return (
                agate.TableSet(non_empty_tables[:2], keys=range(len(non_empty_tables)))
                .merge()
                .exclude(['group'])
            )
            # final_table = agate.TableSet(non_empty_tables).merge()
            # for row in final_table.rows:
            #     print('\n\n** final_table: ', row.values(), '\n', final_table)
            # for (i, table) in enumerate(non_empty_tables[1:]):
            #     for row in table.rows:
            #         print(f'** stack_tables {i}: ', '\n', row.values(), table)
            #         for item in row:
            #             print(f'** type: {type(item)}')
            #     final_table = agate.Table.merge([final_table, table])
            # # print(ret_val)
            # return final_table

    @available.parse_none
    def create_relation_agate_table(
        self,
        database_name: str,
        schema: str,
        table_type: str,
        table_name: str,
        relation_type: str,
        columns: List[FireboltColumn],
    ) -> agate.Table:
        """
        Given list of columns, return a single agate table with those columns.
        The returned table will be a description of the columns in a view or
        table in the database. The column keys are standardized for all
        relation types.
        Args:
          database_name: name of the DB the table exists on
          schema: the DB schema
          table_type: FACT or DIMENSION
          table_name: name of the relation
          relation_type: TABLE or VIEW
          columns: list of columns in the relation
        """
        # print('\n\n** create agate table inputs', database_name,
        # schema,
        # table_type,
        # table_name,
        # relation_type,
        # columns,
        # sep='\n')
        column_names = [
            'table_database',
            'table_schema',
            'table_type',
            'table_name',
            'column_name',
            'column_type',
            'column_index',
            'relation_type',
        ]
        column_types = [
            agate.Text(),
            agate.Text(),
            agate.Text(),
            agate.Text(),
            agate.Text(),
            agate.Text(),
            agate.Number(),
            agate.Text(),
        ]

        rows = [
            (
                database_name,
                schema,
                'VIEW' if relation_type.lower() == 'view' else table_type,
                table_name,
                c.name,
                c.dtype,
                i if relation_type.lower() == 'view' else int(c.column_index),
                relation_type,
            )
            for (i, c) in enumerate(columns)
        ]

        for row in rows:
            print(row)
        return agate.Table(rows, column_names, column_types)

    @available.parse_none
    def sdk_column_list_to_firebolt_column_list(
        self, columns: List[SDKColumn]
    ) -> List[FireboltColumn]:
        """
        Extract and return list of FireboltColumns with names and data types
        extracted from SDKColumns. Impotant! Note that SDKColumns do not have
        a column index field, so it will be filled via enumerate(). Therefore,
        do not call this function if canonical column indices are necessary.
        Args:
          columns: list of Column types as defined in the Python SDK
        """
        print('\n\n****made it here!!!!')
        for (i, val) in enumerate(columns):
            print(val.name, i, self.create_type_string(val.type_code))
        return [
            FireboltColumn(
                column_name=col.name,
                column_index=i,
                dtype=self.col.type_code,
            )
            for (i, col) in enumerate(columns)
        ]

    @available.parse_none
    def create_type_string(self, type_code: Any) -> str:
        """
        Return properly formatted type string for SQL DDL.
        Args: type_code is technically a type, but mypy complained that `type`
          does not have an attribute `subtype`.
        """
        types = {
            'str': 'TEXT',
            'int': 'LONG',
            'float': 'DOUBLE',
            'date': 'DATE',
            'datetime': 'DATE',
            'bool': 'BOOLEAN',
            'Decimal': 'DECIMAL',
        }
        type_code_str = '{}'
        while isinstance(type_code, ARRAY):
            type_code_str = f'ARRAY({type_code_str})'
            type_code = type_code.subtype
        return type_code_str.format(types[type_code.__name__])

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
