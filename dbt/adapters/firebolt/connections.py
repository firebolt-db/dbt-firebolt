from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from multiprocessing.context import SpawnContext
from typing import Generator, Optional, Tuple, Union

from dbt.adapters.contracts.connection import (
    AdapterRequiredConfig,
    AdapterResponse,
    Connection,
    Credentials,
    QueryComment,
)
from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.sql.connections import SQLConnectionManager
from dbt_common.exceptions import (
    DbtConfigError,
    DbtRuntimeError,
    NotImplementedError,
)
from firebolt.client import DEFAULT_API_URL
from firebolt.client.auth import Auth, ClientCredentials, UsernamePassword
from firebolt.db import ARRAY, DECIMAL, ExtendedType
from firebolt.db import connect as sdk_connect
from firebolt.db.connection import Connection as SDKConnection
from firebolt.db.cursor import Cursor
from firebolt.utils.exception import ConnectionError as FireboltConnectionError
from firebolt.utils.exception import FireboltDatabaseError, InterfaceError

logger = AdapterLogger('Firebolt')


@dataclass
class FireboltCredentials(Credentials):
    # These values all come from either profiles.yml or dbt_project.yml.
    user: Optional[str] = None
    password: Optional[str] = None
    # New way to authenticate
    client_id: Optional[str] = None
    client_secret: Optional[str] = None
    api_endpoint: Optional[str] = DEFAULT_API_URL
    engine_name: Optional[str] = None
    account_name: Optional[str] = None
    retries: int = 1

    _ALIASES = {
        'host': 'api_endpoint',
    }

    def __post_init__(self) -> None:
        # If user and password are not provided, assume client_id and client_secret
        # are provided instead
        if not self.user and not self.password:
            if not self.client_id or not self.client_secret:
                raise DbtConfigError(
                    'Either user and password or client_id and client_secret'
                    ' must be provided'
                )
        else:
            if self.client_id or self.client_secret:
                raise DbtConfigError(
                    'Either user and password or client_id and client_secret'
                    ' must be provided'
                )

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

    def __init__(self, profile: AdapterRequiredConfig, mp_context: SpawnContext):
        # Query comment in appent mode only
        # This allows clearer view of queries in query_history
        if not hasattr(profile, 'query_comment'):
            setattr(profile, 'query_comment', QueryComment())
        profile.query_comment.append = True
        super().__init__(profile, mp_context)

    def __str__(self) -> str:
        return 'FireboltConnectionManager()'

    @classmethod
    def open(cls, connection: Connection) -> Connection:
        if connection.state == 'open':
            return connection
        credentials = connection.credentials
        auth: Auth = _determine_auth(credentials)

        def connect() -> SDKConnection:
            handle = sdk_connect(
                auth=auth,
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
            raise DbtRuntimeError(str(e))

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
        raise NotImplementedError('`cancel` is not implemented for this adapter!')

    @classmethod
    def data_type_code_to_name(
        cls, type_code: Union[type, ExtendedType]  # type: ignore[override] # FIR-29423
    ) -> str:
        """
        Convert a Firebolt data type code to a string representing the data type.
        type_code: code type retrieved from the cursor description
        """
        if isinstance(type_code, ARRAY):
            return f'array[{cls.data_type_code_to_name(type_code.subtype)}]'
        elif isinstance(type_code, DECIMAL):
            return str(type_code)
        elif isinstance(type_code, type):
            if issubclass(type_code, str):
                return 'text'
            elif issubclass(type_code, bool):
                # This has to be before int, as bool is a subclass of int
                return 'boolean'
            elif issubclass(type_code, int):
                return 'integer'
            elif issubclass(type_code, float):
                return 'float'
            elif issubclass(type_code, datetime):
                return 'timestamp'
            elif issubclass(type_code, bytes):
                return 'bytea'
            else:
                return 'text'
        return 'text'


def _determine_auth(credentials: FireboltCredentials) -> Auth:
    if credentials.client_id and credentials.client_secret:
        return ClientCredentials(credentials.client_id, credentials.client_secret)
    elif '@' in credentials.user:  # type: ignore # checked in the dataclass
        # email auth can only be used with UsernamePassword
        return UsernamePassword(
            credentials.user,  # type: ignore[arg-type]
            credentials.password,  # type: ignore[arg-type]
        )
    else:
        # assume user provided id and secret in the user/password fields
        return ClientCredentials(
            credentials.user,  # type: ignore[arg-type]
            credentials.password,  # type: ignore[arg-type]
        )
