import json
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import quote, urlencode

import agate
import dbt.exceptions
import jaydebeapi
from dbt.adapters.base import Credentials
from dbt.adapters.sql import SQLConnectionManager
from dbt.clients.agate_helper import table_from_rows
from dbt.contracts.connection import AdapterResponse
from dbt.contracts.graph.manifest import Manifest
from dbt.exceptions import RuntimeException


@dataclass
class FireboltCredentials(Credentials):
    # These values all come from either profiles.yml or dbt_project.yml.
    user: str
    password: str
    jar_path: str
    params: Optional[Dict[str, str]] = None
    host: Optional[str] = 'api.app.firebolt.io'
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
            'account',
            'user',
            'schema',
            'database',
            'engine',
            'jar_path',
            'params',
        )

    @property
    def unique_field(self):
        """
        Return a field that can be hashed to uniquely identify one
        team/organization building with this adapter. This is called by
        `hashed_unique_field()`.
        """
        # Is this safe, or is it too much information. It should only be
        # called by `hashed_unique_field()` as stated in the docstring
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
        jdbc_url = cls.make_jdbc_url(cls, credentials)

        try:
            connection.handle = jaydebeapi.connect(
                credentials.driver,
                jdbc_url,
                [credentials.user, credentials.password],
                credentials.jar_path,
            )
            connection.state = 'open'
        except Exception as e:
            connection.handle = None
            connection.state = 'fail'
            # If we get a 502 or 503 error, maybe engine isn't running.
            if '50' in f'{e}':
                if credentials.engine_name is None:
                    error_msg_append = (
                        '\nTo specify a non-default engine, '
                        'add an engine_name field into the appropriate '
                        'target in your '
                        'profiles.yml file.'
                    )
                else:
                    engine_name = credentials.engine_name
                    error_msg_append = ''
                raise EngineOfflineException(
                    f'Failed to connect via JDBC. Is the {engine_name} '
                    + f'engine for {credentials.database} running? '
                    + error_msg_append
                )

            raise dbt.exceptions.FailedToConnectException(str(e))
        return connection

    def make_jdbc_url(self, credentials):
        jdbc_url = f'jdbc:firebolt://{credentials.host}/{credentials.database}'
        if credentials.params:
            jdbc_url += ''.join(
                map(
                    lambda kv: '&' + quote(kv[0]) + '=' + quote(kv[1]),
                    credentials.params.items(),
                )
            )
        # For both engine and account names, if there's not a value specified
        # it uses whatever Firebolt has set as default for this DB. So just
        # fill in url variables that are not None.
        # Hack: remove "_name" from keys so url is correctly formed.
        url_vars = {
            key[:-5]: quote(getattr(credentials, key).lower())
            for key in ['engine_name', 'account_name']
            if getattr(credentials, key)
        }
        # If params, then add them, too.
        if credentials.params:
            url_vars.update(
                {
                    key: quote(value).lower()
                    for key, value in credentials.params.items()
                    if value
                }
            )
        if url_vars:
            jdbc_url += '?' + urlencode(url_vars)
        return jdbc_url

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
        Returns adapter-specific information about the last executed
        command. Ideally, the return value is an AdapterResponse object
        that includes items such as code, rows_affected, bytes_processed,
        and a summary _message for logging to stdout.
        For now, returning "_message" hard-coded as "OK", and
        the rows_affected, which I suspect isn't working properly
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

    @classmethod
    def get_status(cls, cursor):
        return 'OK'

    @classmethod
    def get_result_from_cursor(cls, cursor: Any) -> agate.Table:
        data: List[Any] = []
        column_names: List[str] = []

        if cursor.description is not None:
            # convert Java Strings to Python strings
            column_names = [str(col[0]) for col in cursor.description]
            rows = cursor.fetchall()
            data = cls.process_results(column_names, rows)

        return cls.table_from_data_flat(data, column_names)

    @classmethod
    def table_from_data_flat(cls, data, column_names: Iterable[str]) -> agate.Table:
        """Convert list of dictionaries into an Agate table"""
        rows = []
        for _row in data:
            row = []
            for value in list(_row.values()):
                if isinstance(value, (dict, list, tuple)):
                    out = json.dumps(value, cls=dbt.utils.JSONEncoder)
                elif str(type(value)) == "<java class 'java.lang.Integer'>":
                    out = int(value)
                # stolen from this abandoned jaydebeapi PR:
                # https://github.com/baztian/jaydebeapi/commit/ba8e93fe8828fb87236ee64e05e82e2b13d66034
                # might fail with very large numbers?
                elif str(type(value)) == "<java class 'java.math.BigInteger'>":
                    out = int(getattr(value, 'toString')())
                else:
                    out = value
                row.append(out)
            rows.append(row)
        return table_from_rows(rows=rows, column_names=column_names)

    def set_query_header(self, manifest: Manifest) -> None:
        self.query_header = None


class EngineOfflineException(Exception):
    CODE = 10003
    MESSAGE = 'Connection Error'

    def process_stack(self):
        lines = []

        if hasattr(self.node, 'build_path') and self.node.build_path:
            lines.append('compiled SQL at {}'.format(self.node.build_path))

        return lines + RuntimeException.process_stack(self)

    @property
    def type(self):
        return 'firebolt'
