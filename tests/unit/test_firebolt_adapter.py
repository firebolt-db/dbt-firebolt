from unittest.mock import MagicMock, patch

from dbt.contracts.connection import Connection
from firebolt.db.connection import Connection as SDKConnection
from firebolt.utils.exception import InterfaceError
from pytest import fixture, mark

from dbt.adapters.firebolt import (
    FireboltAdapter,
    FireboltConnectionManager,
    FireboltCredentials,
)
from dbt.adapters.firebolt.column import FireboltColumn


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


@fixture(scope='module')
def adapter():
    return FireboltAdapter(MagicMock())


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
    'profile,expected',
    [
        (MagicMock(query_comment=MagicMock(append=None)), True),
        (MagicMock(query_comment=MagicMock(append=True)), True),
        (MagicMock(query_comment=MagicMock(append=False)), True),
        (MagicMock(query_comment=MagicMock(append='dummy')), True),
    ],
)
def test_setting_append(profile, expected):
    adapter = FireboltAdapter(profile)
    assert adapter.config.query_comment.append == expected


@mark.parametrize(
    'c_type,expected',
    [
        ('STRING', 'TEXT'),
        ('TIMESTAMP', 'TIMESTAMP'),
        ('FLOAT', 'FLOAT'),
        ('INTEGER', 'INT'),
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
