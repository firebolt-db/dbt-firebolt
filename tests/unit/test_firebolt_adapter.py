from unittest.mock import MagicMock, patch

from dbt.contracts.connection import Connection
from firebolt.db.connection import Connection as SDKConnection
from firebolt.utils.exception import InterfaceError
from pytest import fixture

from dbt.adapters.firebolt import (
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