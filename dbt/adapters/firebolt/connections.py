from contextlib import contextmanager
from dataclasses import dataclass
from typing import Optional, Tuple

import dbt.exceptions
from dbt.adapters.base import Credentials
from dbt.adapters.sql import SQLConnectionManager
from dbt.contracts.connection import AdapterResponse
from dbt.contracts.graph.manifest import Manifest
from dbt.exceptions import RuntimeException
from firebolt.client import DEFAULT_API_URL
from firebolt.client.auth import UsernamePassword
from firebolt.db import connect


@dataclass
class FireboltCredentials(Credentials):
    # These values all come from either profiles.yml or dbt_project.yml.
    user: str
    password: str
    api_endpoint: Optional[str] = DEFAULT_API_URL
    driver: str = 'com.firebolt.FireboltDriver'
    engine_name: Optional[str] = None
    account_name: Optional[str] = None
    keep_connection_open: Optional[bool] = True

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
    def unique_field(self) -> Optional[str]:
        """
        Return a field that can be hashed to uniquely identify one
        team/organization building with this adapter. This is called by
        `hashed_unique_field()`.
        """
        # Is this safe, or is it too much information? It should only be
        # called by `hashed_unique_field()` as stated in the docstring,
        # but I'm asking here for noting in the PR of this branch.
        return self.engine_name


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

    _keep_connection_open: Optional[bool] = True

    TYPE = 'firebolt'

    def __str__(self) -> str:
        return self._message

    @classmethod
    def open(cls, connection: SQLConnectionManager) -> SQLConnectionManager:
        if connection.state == 'open':
            return connection
        creds = connection.credentials
        print('\n** credentials', creds.keep_connection_open)
        if creds.keep_connection_open is not None and not creds.keep_connection_open:
            cls._keep_connection_open = True
        # Create a connection based on provided credentials.
        print("\'\n** keep connection open", cls._keep_connection_open, '****\n')
        connection.handle = connect(
            auth=UsernamePassword(creds.user, creds.password),
            engine_name=creds.engine_name,
            database=creds.database,
            api_endpoint=creds.api_endpoint,
            account_name=creds.account_name,
        )
        connection.state = 'open'
        return connection

    @contextmanager
    def exception_handler(self, sql: str) -> RuntimeException:
        try:
            yield
        except Exception as e:
            self.release()
            raise RuntimeException(str(e))

    @classmethod
    def get_response(cls, cursor: SQLConnectionManager) -> AdapterResponse:
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

    def cancel(self, connection: SQLConnectionManager) -> None:
        """Cancel the last query on the given connection."""
        raise dbt.exceptions.NotImplementedException(
            '`cancel` is not implemented for this adapter!'
        )

    def set_query_header(self, manifest: Manifest) -> None:
        self.query_header = None

    def release(self) -> None:
        # if self._keep_connection_open:
        #     print(f'\n** kept {self.thread_connections} open\n')
        #     return

        with self.lock:
            print(f'\n** Locked {self.thread_connections}\n')
            conn = self.get_if_exists()
            print('\n** Conn:', conn)
            if conn is None:
                return

        try:
            # always close the connection.
            print(f'\n** Before closing {self.thread_connections}\n')
            self.close(conn)
            print(f'\n** Closed {self.thread_connections}\n')
        except Exception:
            # if rollback or close failed, remove our busted connection
            self.clear_thread_connection()
            raise
