from datetime import datetime
from multiprocessing import get_context
from unittest.mock import MagicMock, patch

import agate
from dbt.adapters.contracts.connection import Connection
from dbt_common.exceptions import DbtRuntimeError
from firebolt.client.auth import ClientCredentials, UsernamePassword
from firebolt.db import ARRAY, DECIMAL
from firebolt.db.connection import Connection as SDKConnection
from firebolt.utils.exception import InterfaceError
from pytest import fixture, mark, raises

from dbt.adapters.firebolt import (
    FireboltAdapter,
    FireboltConnectionManager,
    FireboltCredentials,
)
from dbt.adapters.firebolt.column import FireboltColumn
from dbt.adapters.firebolt.connections import _determine_auth
from dbt.adapters.firebolt.relation import FireboltRelation
from tests.functional.adapter.test_basic import AnySpecifiedType


@fixture
def connection():
    credentials = FireboltCredentials(
        user='test-user',
        password='test-password',
        database='test-db',
        schema='test-schema',
        retries=2,
    )
    connection = Connection('firebolt', None, credentials)
    return connection


@fixture
def adapter():
    return FireboltAdapter(MagicMock(), get_context('spawn'))


def test_open(connection):
    successful_attempt = MagicMock(spec=SDKConnection)

    connect = MagicMock(
        side_effect=[
            InterfaceError('Connection has failed'),
            InterfaceError('Connection has failed'),
            successful_attempt,
        ]
    )

    with patch(
        'dbt.adapters.firebolt.connections.sdk_connect', wraps=connect
    ) as mock_connect:
        FireboltConnectionManager.open(connection)

    assert mock_connect.call_count == 3
    assert connect.call_count == 3
    assert connection.state == 'open'
    assert connection.handle == successful_attempt


@mark.parametrize(
    'column,expected',
    [
        ('INT', 'INT'),
        ('INTEGER', 'INTEGER'),
        ('SOME FUTURE TYPE', 'SOME FUTURE TYPE'),
        ('ARRAY(INT NULL)', 'ARRAY(INT)'),
        ('ARRAY(INT NOT NULL)', 'ARRAY(INT)'),
        ('ARRAY(ARRAY(INT NOT NULL))', 'ARRAY(ARRAY(INT))'),
        ('ARRAY(ARRAY(INT NULL))', 'ARRAY(ARRAY(INT))'),
        ('ARRAY(ARRAY(INT NULL) NULL)', 'ARRAY(ARRAY(INT))'),
    ],
)
def test_resolve_columns(adapter, column, expected):
    assert adapter.resolve_special_columns(column) == expected


@mark.parametrize(
    'input_type, expected_output',
    [
        (str, 'text'),
        (int, 'integer'),
        (float, 'float'),
        (bool, 'boolean'),
        (datetime, 'timestamp'),
        (bytes, 'bytea'),
        (DECIMAL(1, 23), 'Decimal(1, 23)'),
        (ARRAY(int), 'array[integer]'),
        (dict, 'text'),  # Test for a type that is not handled and defaults to 'text'
    ],
)
def test_data_type_code_to_name(input_type, expected_output):
    assert (
        FireboltConnectionManager.data_type_code_to_name(input_type) == expected_output
    )


@mark.parametrize(
    'profile,expected',
    [
        (MagicMock(query_comment=MagicMock(append=None)), True),
        (MagicMock(query_comment=MagicMock(append=True)), True),
        (MagicMock(query_comment=MagicMock(append=False)), True),
        (MagicMock(query_comment=MagicMock(append='dummy')), True),
    ],
)
def test_setting_append(profile, expected):
    adapter = FireboltAdapter(profile, get_context('spawn'))
    assert adapter.config.query_comment.append == expected


@mark.parametrize(
    'c_type,expected',
    [
        ('STRING', 'TEXT'),
        ('TIMESTAMP', 'TIMESTAMP'),
        ('FLOAT', 'FLOAT'),
        ('INTEGER', AnySpecifiedType(['INT', 'INTEGER'])),
        ('BOOLEAN', 'BOOLEAN'),
        ('DUMMY_TYPE', 'DUMMY_TYPE'),
        ('TEXT', 'TEXT'),
    ],
)
def test_column_translate_type(adapter, c_type, expected):
    Column = adapter.get_column_class()
    assert Column.translate_type(c_type) == expected


def test_column_class_init(adapter):
    Column = adapter.get_column_class()
    assert Column == FireboltColumn
    assert Column('Name', 'TEXT') == Column.from_description('Name', 'TEXT')
    assert Column('Name', 'INT') == Column.from_description('Name', 'INT')


def test_column_string_type(adapter):
    Column = adapter.get_column_class()
    assert Column.string_type(111) == 'text'


def test_determine_auth_with_client_credentials():
    credentials = FireboltCredentials(
        client_id='your_client_id',
        client_secret='your_client_secret',
        database='your_database',
        schema='your_schema',
    )
    auth = _determine_auth(credentials)
    assert isinstance(auth, ClientCredentials)
    assert auth.client_id == 'your_client_id'
    assert auth.client_secret == 'your_client_secret'


def test_determine_auth_with_email_auth():
    credentials = FireboltCredentials(
        user='your_email@example.com',
        password='your_password',
        database='your_database',
        schema='your_schema',
    )
    auth = _determine_auth(credentials)
    assert isinstance(auth, UsernamePassword)
    assert auth.username == 'your_email@example.com'
    assert auth.password == 'your_password'


def test_determine_auth_with_id_and_secret():
    credentials = FireboltCredentials(
        user='your_user_id',
        password='your_user_secret',
        database='your_database',
        schema='your_schema',
    )
    auth = _determine_auth(credentials)
    assert isinstance(auth, ClientCredentials)
    assert auth.client_id == 'your_user_id'
    assert auth.client_secret == 'your_user_secret'


def test_make_field_partition_pairs_valid(adapter):
    columns = [
        {'name': 'col1', 'data_type': 'INT'},
        {'name': 'col2', 'data_type': 'STRING'},
    ]
    partitions = [
        {'name': 'part1', 'data_type': 'DATE', 'regex': 'regex1'},
        {'name': 'part2', 'data_type': 'DATE', 'regex': 'regex2'},
    ]
    result = adapter.make_field_partition_pairs(columns, partitions)
    expected = [
        '"col1" INT',
        '"col2" STRING',
        '"part1" DATE PARTITION (\'regex1\')',
        '"part2" DATE PARTITION (\'regex2\')',
    ]
    assert result == expected


def test_make_field_partition_pairs_missing_column_data_type(adapter):
    columns = [
        {'name': 'col1', 'data_type': 'INT'},
        {'name': 'col2'},  # Missing data_type
    ]
    partitions = []
    with raises(DbtRuntimeError, match='Data type is missing for column `col2`.'):
        adapter.make_field_partition_pairs(columns, partitions)


def test_make_field_partition_pairs_missing_partition_data_type(adapter):
    columns = [{'name': 'col1', 'data_type': 'INT'}]
    partitions = [{'name': 'part1', 'regex': 'regex1'}]  # Missing data_type
    with raises(DbtRuntimeError, match='Data type is missing for partition `part1`.'):
        adapter.make_field_partition_pairs(columns, partitions)


def test_make_field_partition_pairs_missing_partition_regex(adapter):
    columns = [{'name': 'col1', 'data_type': 'INT'}]
    partitions = [{'name': 'part1', 'data_type': 'DATE'}]  # Missing regex
    with raises(DbtRuntimeError, match='Regex is missing for partition `part1`.'):
        adapter.make_field_partition_pairs(columns, partitions)


def test_make_field_partition_pairs_empty_partitions(adapter):
    columns = [
        {'name': 'col1', 'data_type': 'INT'},
        {'name': 'col2', 'data_type': 'STRING'},
    ]
    partitions = []
    result = adapter.make_field_partition_pairs(columns, partitions)
    expected = ['"col1" INT', '"col2" STRING']
    assert result == expected


def test_stack_tables_valid(adapter):
    table1 = agate.Table.from_object(
        [{'col1': 1, 'col2': 'a'}, {'col1': 2, 'col2': 'b'}]
    )
    table2 = agate.Table.from_object(
        [{'col1': 3, 'col2': 'c'}, {'col1': 4, 'col2': 'd'}]
    )
    result = adapter.stack_tables([table1, table2])
    expected = agate.Table.from_object(
        [
            {'col1': 1, 'col2': 'a'},
            {'col1': 2, 'col2': 'b'},
            {'col1': 3, 'col2': 'c'},
            {'col1': 4, 'col2': 'd'},
        ]
    )
    assert str(result) == str(expected)


def test_stack_tables_empty(adapter):
    result = adapter.stack_tables([agate.Table.from_object([])])
    expected = agate.Table.from_object([])
    assert str(result) == str(expected)


@mark.parametrize(
    'table1,table2',
    [
        ({'col1': 1, 'col2': 'a'}, {'col3': 2, 'col4': 'b'}),
        ({'col1': 1, 'col2': 'a'}, {'col1': 2}),
    ],
)
def test_stack_tables_different_schemas(table1, table2, adapter):
    table1 = agate.Table.from_object([table1])
    table2 = agate.Table.from_object([table2])
    with raises(ValueError, match='Not all tables have the same column types!'):
        adapter.stack_tables([table1, table2])


def test_get_rows_different_sql_valid(adapter):
    relation_a = FireboltRelation.create(
        database='db', schema='public', identifier='table_a'
    )
    relation_b = FireboltRelation.create(
        database='db', schema='public', identifier='table_b'
    )
    column_names = ['col1', 'col2']
    sql = adapter.get_rows_different_sql(relation_a, relation_b, column_names)
    assert 'SELECT "col1", "col2" FROM table_a' in sql
    assert 'SELECT "col1", "col2" FROM table_b' in sql
    assert 'WHERE table_a."col1" = table_b."col1"' in sql
    assert 'AND table_a."col2" = table_b."col2"' in sql


def test_get_rows_different_sql_empty_columns(adapter):
    def mock_column(name: str) -> MagicMock:
        col = MagicMock()
        col.name = name
        return col

    relation_a = FireboltRelation.create(
        database='db', schema='public', identifier='table_a'
    )
    relation_b = FireboltRelation.create(
        database='db', schema='public', identifier='table_b'
    )
    column_names = None
    adapter.get_columns_in_relation = MagicMock(
        return_value=[mock_column('col1'), mock_column('col2')]
    )
    sql = adapter.get_rows_different_sql(relation_a, relation_b, column_names)
    print(sql)
    assert 'SELECT "col1", "col2" FROM table_a' in sql
    assert 'SELECT "col1", "col2" FROM table_b' in sql
    assert 'WHERE table_a."col1" = table_b."col1"' in sql
    assert 'AND table_a."col2" = table_b."col2"' in sql


def test_annotate_date_columns_for_partitions_valid(adapter):
    vals = '1,2,3'
    cols = ['col1', 'col2', 'col3']
    col_types = [
        FireboltColumn('col1', 'int'),
        FireboltColumn('col2', 'date'),
        FireboltColumn('col3', 'date'),
    ]
    expected = '1,2,3'
    # It is expected that there's no cast to DATE here, as the logic is incorrect
    # I'm not sure we even need it anymore
    result = adapter.annotate_date_columns_for_partitions(vals, cols, col_types)
    assert result == expected
