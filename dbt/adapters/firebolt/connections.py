from contextlib import contextmanager
from dataclasses import dataclass
from typing import Generator, Optional, Tuple

import dbt.exceptions
from dbt.adapters.base import Credentials
from dbt.adapters.sql import SQLConnectionManager
from dbt.contracts.connection import AdapterResponse, Connection
from dbt.contracts.graph.manifest import Manifest
from dbt.events import AdapterLogger  # type: ignore
from dbt.exceptions import RuntimeException
from firebolt.client import DEFAULT_API_URL
from firebolt.client.auth import UsernamePassword
from firebolt.db import connect as sdk_connect
from firebolt.db.connection import Connection as SDKConnection
from firebolt.db.cursor import Cursor
from firebolt.utils.exception import ConnectionError as FireboltConnectionError
from firebolt.utils.exception import FireboltDatabaseError, InterfaceError

logger = AdapterLogger('Firebolt')


@dataclass
class FireboltCredentials(Credentials):
    # These values all come from either profiles.yml or dbt_project.yml.
    user: str
    password: str
    api_endpoint: Optional[str] = DEFAULT_API_URL
    driver: str = 'com.firebolt.FireboltDriver'
    engine_name: Optional[str] = None
    account_name: Optional[str] = None
    retries: int = 1

    @property
    def type(self) -> str:
        return 'firebolt'

    def _connection_keys(self) -> Tuple[str, str, str, str, str, str, str]:
        """
        Return tuple of keys (i.e. not values) to display
        in the `dbt debug` output.
        """
        return (
            'api_endpoint',
            'account_name',
            'user',
            'schema',
            'database',
            'engine_name',
            'params',
        )

    @property
    def unique_field(self) -> str:
        """
        Return a field that can be hashed to uniquely identify one
        team/organization building with this adapter. This is called by
        `hashed_unique_field()`.
        """
        return self.database


class FireboltConnectionManager(SQLConnectionManager):
    """
    Methods to implement:
    - exception_handler
    - cancel_open
    - open
    - begin
    - commit
    - clear_transaction
    - execute
    """

    TYPE = 'firebolt'

    def __str__(self) -> str:
        return 'FireboltConnectionManager()'

    @classmethod
    def open(cls, connection: Connection) -> Connection:
        if connection.state == 'open':
            return connection
        credentials = connection.credentials

        def connect() -> SDKConnection:
            handle = sdk_connect(
                auth=UsernamePassword(credentials.user, credentials.password),
                engine_name=credentials.engine_name,
                database=credentials.database,
                api_endpoint=credentials.api_endpoint,
                account_name=credentials.account_name,
            )
            handle.cursor().execute('SELECT 1')
            return handle

        retryable_exceptions = [
            FireboltDatabaseError,
            FireboltConnectionError,
            InterfaceError,
        ]

        return cls.retry_connection(
            connection,
            connect=connect,
            logger=logger,
            retry_limit=credentials.retries,
            retryable_exceptions=retryable_exceptions,
        )

    @contextmanager
    def exception_handler(self, sql: str) -> Generator:
        try:
            yield
        except Exception as e:
            self.release()
            raise RuntimeException(str(e))

    @classmethod
    def get_response(cls, cursor: Cursor) -> AdapterResponse:
        """
        Return an AdapterResponse object. Note that I can't overload/extend it
        as it's defined in dbt core and other internal fns complain if it has extra
        fields. `code` field is missing for Firebolt adapter, as it's not returned
        from the SDK/API.
        """
        rowcount = cursor.rowcount
        if cursor.rowcount == -1:
            rowcount = 0
        return AdapterResponse(
            _message='SUCCESS',
            rows_affected=rowcount,
            code=None,
        )

    def begin(self) -> None:
        """
        Passing `SQLConnectionManager.begin()` because
        Firebolt does not yet support transactions.
        """

    def commit(self) -> None:
        """
        Passing `SQLConnectionManager.commit()` because
        Firebolt does not yet support transactions.
        """

    def cancel(self, connection: Connection) -> None:
        """Cancel the last query on the given connection."""
        raise dbt.exceptions.NotImplementedException(
            '`cancel` is not implemented for this adapter!'
        )

    def set_query_header(self, manifest: Manifest) -> None:
        self.query_header = None
