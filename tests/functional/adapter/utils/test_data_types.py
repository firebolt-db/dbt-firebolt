import pytest
from dbt.tests.adapter.utils.data_types.test_type_bigint import BaseTypeBigInt
from dbt.tests.adapter.utils.data_types.test_type_bigint import (
    models__actual_sql as bigint_model,
)
from dbt.tests.adapter.utils.data_types.test_type_bigint import (
    models__expected_sql as bigint_expected,
)
from dbt.tests.adapter.utils.data_types.test_type_boolean import (
    BaseTypeBoolean,
)
from dbt.tests.adapter.utils.data_types.test_type_boolean import (
    models__actual_sql as bool_model,
)
from dbt.tests.adapter.utils.data_types.test_type_float import BaseTypeFloat
from dbt.tests.adapter.utils.data_types.test_type_float import (
    models__actual_sql as float_model,
)
from dbt.tests.adapter.utils.data_types.test_type_int import BaseTypeInt
from dbt.tests.adapter.utils.data_types.test_type_int import (
    models__actual_sql as int_model,
)
from dbt.tests.adapter.utils.data_types.test_type_numeric import (
    BaseTypeNumeric,
)
from dbt.tests.adapter.utils.data_types.test_type_numeric import (
    models__actual_sql as num_model,
)
from dbt.tests.adapter.utils.data_types.test_type_string import BaseTypeString
from dbt.tests.adapter.utils.data_types.test_type_string import (
    models__actual_sql as string_model,
)
from dbt.tests.adapter.utils.data_types.test_type_timestamp import (
    BaseTypeTimestamp,
)
from dbt.tests.adapter.utils.data_types.test_type_timestamp import (
    models__actual_sql as ts_model,
)
from firebolt import __version__ as sdk_version

schema_actual_table_yml = """
version: 2
models:
  - name: actual
    config:
      materialized: table
"""

schema_expected_table_yml = """
version: 2
models:
  - name: expected
    config:
      materialized: table
"""


class TestTypeBigInt(BaseTypeBigInt):
    @pytest.fixture(scope='class')
    def models(self):
        return {
            'expected.yml': schema_expected_table_yml,
            'expected.sql': bigint_expected,
            'actual.yml': schema_actual_table_yml,
            'actual.sql': self.interpolate_macro_namespace(bigint_model, 'type_bigint'),
        }


class TestTypeFloat(BaseTypeFloat):
    @pytest.fixture(scope='class')
    def models(self):
        return {
            'actual.sql': self.interpolate_macro_namespace(float_model, 'type_float'),
            'actual.yml': schema_actual_table_yml,
        }


class TestTypeInt(BaseTypeInt):
    @pytest.fixture(scope='class')
    def models(self):
        return {
            'actual.sql': self.interpolate_macro_namespace(int_model, 'type_int'),
            'actual.yml': schema_actual_table_yml,
        }


@pytest.mark.skipif(
    sdk_version <= '0.15.0', reason='Decimal type implemented in firebolt-sdk>0.15.0'
)
class TestTypeNumeric(BaseTypeNumeric):
    @pytest.fixture(scope='class')
    def models(self):
        return {
            'actual.sql': self.interpolate_macro_namespace(num_model, 'type_numeric'),
            'actual.yml': schema_actual_table_yml,
        }


class TestTypeString(BaseTypeString):
    @pytest.fixture(scope='class')
    def models(self):
        return {
            'actual.sql': self.interpolate_macro_namespace(string_model, 'type_string'),
            'actual.yml': schema_actual_table_yml,
        }


class TestTypeTimestamp(BaseTypeTimestamp):
    @pytest.fixture(scope='class')
    def models(self):
        return {
            'actual.sql': self.interpolate_macro_namespace(ts_model, 'type_timestamp'),
            'actual.yml': schema_actual_table_yml,
        }


class TestTypeBoolean(BaseTypeBoolean):
    @pytest.fixture(scope='class')
    def models(self):
        return {
            'actual.sql': self.interpolate_macro_namespace(bool_model, 'type_boolean'),
            'actual.yml': schema_actual_table_yml,
        }
