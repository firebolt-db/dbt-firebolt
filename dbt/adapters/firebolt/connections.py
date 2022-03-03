from contextlib import contextmanager
from dataclasses import dataclass
from typing import Optional

import dbt.exceptions
from dbt.adapters.base import Credentials
from dbt.adapters.sql import SQLConnectionManager
from dbt.contracts.connection import AdapterResponse
from dbt.contracts.graph.manifest import Manifest
from dbt.exceptions import RuntimeException
from firebolt.client import DEFAULT_API_URL
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

    @property
    def type(self):
        return 'firebolt'

    def _connection_keys(self):
        """
        Return list of keys (i.e. not values) to display
        in the `dbt debug` output.
        """
        return (
            'host',
            'account_name',
            'user',
            'schema',
            'database',
            'engine_name',
            'params',
        )

    @property
    def unique_field(self):
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
    """Methods to implement:
    - exception_handler
    - cancel_open
    - open
    - begin
    - commit
    - clear_transaction
    - execute
    """

    TYPE = 'firebolt'

    @classmethod
    def open(cls, connection):
        if connection.state == 'open':
            return connection
        credentials = connection.credentials
        # Create a connection based on provided credentials.
        connection.handle = connect(
            engine_name=credentials.engine_name,
            database=credentials.database,
            username=credentials.user,
            password=credentials.password,
            api_endpoint=credentials.api_endpoint,
            account_name=credentials.account_name,
        )
        connection.state = 'open'
        return connection

    @contextmanager
    def exception_handler(self, sql: str):
        try:
            yield
        except Exception as e:
            self.release()
            raise RuntimeException(str(e))

    # TODO: Decide how much metadata we want to return.
    @classmethod
    def get_response(cls, cursor) -> AdapterResponse:
        """
        Return adapter-specific information about the last executed
        command. Ideally, the return value is an AdapterResponse object
        that includes items such as code, rows_affected, bytes_processed,
        and a summary _message for logging to stdout.
        For now, return "_message" hard-coded as "OK", and
        the rows_affected, which I suspect isn't working properly.
        """
        return AdapterResponse(
            # TODO: get an actual status message and "code" from the cursor
            _message='OK',
            # code=code,
            rows_affected=cursor.rowcount,
        )

    def begin(self):
        """
        Passing `SQLConnectionManager.begin()` because
        Firebolt does not yet support transactions.
        """

    def commit(self):
        """
        Passing `SQLConnectionManager.begin()` because
        Firebolt does not yet support transactions.
        """

    def cancel(self, connection):
        """Cancel the last query on the given connection."""
        raise dbt.exceptions.NotImplementedException(
            '`cancel` is not implemented for this adapter!'
        )

    def set_query_header(self, manifest: Manifest) -> None:
        self.query_header = None
