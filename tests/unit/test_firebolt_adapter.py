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
