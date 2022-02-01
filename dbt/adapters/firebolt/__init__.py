from dbt.adapters.base import AdapterPlugin

from dbt.adapters.firebolt.connections import FireboltCredentials
from dbt.adapters.firebolt.impl import FireboltAdapter
from dbt.include import firebolt

__version__ = '0.21.9'

Plugin = AdapterPlugin(
    adapter=FireboltAdapter,
    credentials=FireboltCredentials,
    include_path=firebolt.PACKAGE_PATH,
)
