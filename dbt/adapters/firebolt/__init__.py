from dbt.adapters.base import AdapterPlugin

from dbt.adapters.firebolt.connections import (
    FireboltConnectionManager,
    FireboltCredentials,
)
from dbt.adapters.firebolt.impl import FireboltAdapter
from dbt.include import firebolt

__version__ = "1.9.4"

Plugin = AdapterPlugin(
    adapter=FireboltAdapter,  # type: ignore
    credentials=FireboltCredentials,
    include_path=firebolt.PACKAGE_PATH,
)
