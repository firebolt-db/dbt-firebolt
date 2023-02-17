import pytest
from dbt.tests.adapter.utils.test_timestamps import BaseCurrentTimestamps

# current_timestamp is a keyword, need to quote it
_MODEL_CURRENT_TIMESTAMP = """
select {{ current_timestamp() }} as 'current_timestamp',
    {{ current_timestamp_in_utc_backcompat() }} as current_timestamp_in_utc_backcompat,
    {{ current_timestamp_backcompat() }} as current_timestamp_backcompat
"""

_MODEL_EXPECTED_SQL = """
select now() as 'current_timestamp',
    current_timestamp::TIMESTAMP as current_timestamp_in_utc_backcompat,
    current_timestamp::TIMESTAMP as current_timestamp_backcompat
"""


class TestCurrentTimestampFirebolt(BaseCurrentTimestamps):
    @pytest.fixture(scope='class')
    def models(self):
        return {'get_current_timestamp.sql': _MODEL_CURRENT_TIMESTAMP}

    @pytest.fixture(scope='class')
    def expected_schema(self):
        # TODO: add Timezone-aware types here
        return {
            'current_timestamp': 'TIMESTAMP',
            'current_timestamp_in_utc_backcompat': 'TIMESTAMP',
            'current_timestamp_backcompat': 'TIMESTAMP',
        }

    @pytest.fixture(scope='class')
    def expected_sql(self):
        return _MODEL_EXPECTED_SQL
