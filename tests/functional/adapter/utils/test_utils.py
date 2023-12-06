import pytest
from dbt.tests.adapter.utils.fixture_bool_or import (
    models__test_bool_or_sql,
    models__test_bool_or_yml,
)
from dbt.tests.adapter.utils.fixture_concat import (
    models__test_concat_sql,
    models__test_concat_yml,
)
from dbt.tests.adapter.utils.fixture_date_trunc import (
    models__test_date_trunc_sql,
    models__test_date_trunc_yml,
)
from dbt.tests.adapter.utils.fixture_dateadd import (
    models__test_dateadd_sql,
    models__test_dateadd_yml,
)
from dbt.tests.adapter.utils.fixture_datediff import models__test_datediff_yml
from dbt.tests.adapter.utils.fixture_hash import (
    models__test_hash_sql,
    models__test_hash_yml,
)
from dbt.tests.adapter.utils.fixture_last_day import (
    models__test_last_day_sql,
    models__test_last_day_yml,
)
from dbt.tests.adapter.utils.fixture_replace import (
    models__test_replace_sql,
    models__test_replace_yml,
)
from dbt.tests.adapter.utils.fixture_right import (
    models__test_right_sql,
    models__test_right_yml,
)
from dbt.tests.adapter.utils.fixture_safe_cast import (
    models__test_safe_cast_sql,
    models__test_safe_cast_yml,
)
from dbt.tests.adapter.utils.fixture_split_part import (
    models__test_split_part_sql,
    models__test_split_part_yml,
)
from dbt.tests.adapter.utils.test_any_value import BaseAnyValue
from dbt.tests.adapter.utils.test_array_append import (
    BaseArrayAppend,
    models__array_append_actual_sql,
    models__array_append_expected_sql,
)
from dbt.tests.adapter.utils.test_array_concat import (
    BaseArrayConcat,
    models__array_concat_actual_sql,
    models__array_concat_expected_sql,
)
from dbt.tests.adapter.utils.test_array_construct import (
    BaseArrayConstruct,
    models__array_construct_actual_sql,
    models__array_construct_expected_sql,
)
from dbt.tests.adapter.utils.test_bool_or import BaseBoolOr
from dbt.tests.adapter.utils.test_cast_bool_to_text import BaseCastBoolToText
from dbt.tests.adapter.utils.test_concat import BaseConcat
from dbt.tests.adapter.utils.test_current_timestamp import (
    BaseCurrentTimestampAware,
)
from dbt.tests.adapter.utils.test_date_trunc import BaseDateTrunc
from dbt.tests.adapter.utils.test_dateadd import BaseDateAdd
from dbt.tests.adapter.utils.test_datediff import BaseDateDiff
from dbt.tests.adapter.utils.test_escape_single_quotes import (
    BaseEscapeSingleQuotesBackslash,
)
from dbt.tests.adapter.utils.test_except import BaseExcept
from dbt.tests.adapter.utils.test_hash import BaseHash
from dbt.tests.adapter.utils.test_intersect import BaseIntersect
from dbt.tests.adapter.utils.test_last_day import BaseLastDay
from dbt.tests.adapter.utils.test_length import BaseLength
from dbt.tests.adapter.utils.test_listagg import BaseListagg
from dbt.tests.adapter.utils.test_position import BasePosition
from dbt.tests.adapter.utils.test_replace import BaseReplace
from dbt.tests.adapter.utils.test_right import BaseRight
from dbt.tests.adapter.utils.test_safe_cast import BaseSafeCast
from dbt.tests.adapter.utils.test_split_part import BaseSplitPart
from dbt.tests.adapter.utils.test_string_literal import BaseStringLiteral
from pytest import mark

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


class TestAnyValue(BaseAnyValue):
    pass


schema_seed_bool_or_yml = """
version: 2
seeds:
  - name: data_bool_or
    config:
      column_types:
        key: TEXT
        val1: INT NULL
        val2: INT NULL
"""


class TestBoolOr(BaseBoolOr):
    @pytest.fixture(scope='class')
    def models(self):
        return {
            'test_bool_or.yml': models__test_bool_or_yml,
            'test_bool_or.sql': self.interpolate_macro_namespace(
                models__test_bool_or_sql, 'bool_or'
            ),
            'seeds.yml': schema_seed_bool_or_yml,
        }


class TestCastBoolToText(BaseCastBoolToText):
    pass


schema_seed_concat_yml = """
version: 2
seeds:
  - name: data_concat
    config:
      column_types:
        input_1: TEXT NULL
        input_2: TEXT NULL
        output: TEXT NULL
"""


class TestConcat(BaseConcat):
    @pytest.fixture(scope='class')
    def models(self):
        return {
            'test_concat.yml': models__test_concat_yml,
            'test_concat.sql': self.interpolate_macro_namespace(
                models__test_concat_sql, 'concat'
            ),
            'seeds.yml': schema_seed_concat_yml,
        }


schema_seed_date_add_yml = """
version: 2
seeds:
  - name: data_dateadd
    config:
      column_types:
        from_time: TIMESTAMP NULL
        interval_length: INT
        datepart: TEXT
        result: TIMESTAMP NULL
"""


class TestDateAdd(BaseDateAdd):
    @pytest.fixture(scope='class')
    def models(self):
        return {
            'test_dateadd.yml': models__test_dateadd_yml,
            'test_dateadd.sql': self.interpolate_macro_namespace(
                models__test_dateadd_sql, 'dateadd'
            ),
            'seeds.yml': schema_seed_date_add_yml,
        }


schema_seed_date_diff_yml = """
version: 2
seeds:
  - name: data_datediff
    config:
      column_types:
        first_date: TIMESTAMP NULL
        second_date: TIMESTAMP NULL
        datepart: TEXT
        result: INT NULL
"""

# microsecond and millisecond are not supported
models__overridden_test_datediff_sql = """
with data as (

    select * from {{ ref('data_datediff') }}

)

select

    case
        when datepart = 'second' then {{ datediff('first_date', 'second_date', 'second') }}
        when datepart = 'minute' then {{ datediff('first_date', 'second_date', 'minute') }}
        when datepart = 'hour' then {{ datediff('first_date', 'second_date', 'hour') }}
        when datepart = 'day' then {{ datediff('first_date', 'second_date', 'day') }}
        when datepart = 'week' then {{ datediff('first_date', 'second_date', 'week') }}
        when datepart = 'month' then {{ datediff('first_date', 'second_date', 'month') }}
        when datepart = 'year' then {{ datediff('first_date', 'second_date', 'year') }}
        else null
    end as actual,
    result as expected

from data

-- Also test correct casting of literal values.

union all select {{ datediff("'1999-12-31 23:59:59.999999'", "'2000-01-01 00:00:00.000000'", "second") }} as actual, 1 as expected
union all select {{ datediff("'1999-12-31 23:59:59.999999'", "'2000-01-01 00:00:00.000000'", "minute") }} as actual, 1 as expected
union all select {{ datediff("'1999-12-31 23:59:59.999999'", "'2000-01-01 00:00:00.000000'", "hour") }} as actual, 1 as expected
union all select {{ datediff("'1999-12-31 23:59:59.999999'", "'2000-01-01 00:00:00.000000'", "day") }} as actual, 1 as expected
union all select {{ datediff("'1999-12-31 23:59:59.999999'", "'2000-01-03 00:00:00.000000'", "week") }} as actual, 1 as expected
union all select {{ datediff("'1999-12-31 23:59:59.999999'", "'2000-01-01 00:00:00.000000'", "month") }} as actual, 1 as expected
union all select {{ datediff("'1999-12-31 23:59:59.999999'", "'2000-01-01 00:00:00.000000'", "quarter") }} as actual, 1 as expected
union all select {{ datediff("'1999-12-31 23:59:59.999999'", "'2000-01-01 00:00:00.000000'", "year") }} as actual, 1 as expected
"""


class TestDateDiff(BaseDateDiff):
    @pytest.fixture(scope='class')
    def models(self):
        return {
            'test_datediff.yml': models__test_datediff_yml,
            'test_datediff.sql': self.interpolate_macro_namespace(
                models__overridden_test_datediff_sql, 'datediff'
            ),
            'seeds.yml': schema_seed_date_diff_yml,
        }


schema_seed_date_trunc_yml = """
version: 2
seeds:
  - name: data_date_trunc
    config:
      column_types:
        updated_at: TIMESTAMP NULL
        day: DATE NULL
        month: DATE NULL
"""


class TestDateTrunc(BaseDateTrunc):
    @pytest.fixture(scope='class')
    def models(self):
        return {
            'test_date_trunc.yml': models__test_date_trunc_yml,
            'test_date_trunc.sql': self.interpolate_macro_namespace(
                models__test_date_trunc_sql, 'date_trunc'
            ),
            'seeds.yml': schema_seed_date_trunc_yml,
        }


class TestEscapeSingleQuotes(BaseEscapeSingleQuotesBackslash):
    pass


@mark.skip('Except is not supported yet')
class TestExcept(BaseExcept):
    pass


schema_seed_hash_yml = """
version: 2
seeds:
  - name: data_hash
    config:
      column_types:
        input_1: TEXT NULL
        output: TEXT
"""


class TestHash(BaseHash):
    @pytest.fixture(scope='class')
    def models(self):
        return {
            'test_hash.yml': models__test_hash_yml,
            'test_hash.sql': self.interpolate_macro_namespace(
                models__test_hash_sql, 'hash'
            ),
            'seeds.yml': schema_seed_hash_yml,
        }


@mark.skip('Intersect is not supported yet')
class TestIntersect(BaseIntersect):
    pass


schema_seed_last_day_yml = """
version: 2
seeds:
  - name: data_last_day
    config:
      column_types:
        date_day: DATE NULL
        date_part: TEXT
        result: DATE NULL
"""


class TestLastDay(BaseLastDay):
    @pytest.fixture(scope='class')
    def models(self):
        return {
            'test_last_day.yml': models__test_last_day_yml,
            'test_last_day.sql': self.interpolate_macro_namespace(
                models__test_last_day_sql, 'last_day'
            ),
            'seeds.yml': schema_seed_last_day_yml,
        }


class TestLength(BaseLength):
    pass


@mark.skip('ORDER BY in ARRAY_AGG is not supported. FIR-8828')
class TestListagg(BaseListagg):
    pass


class TestPosition(BasePosition):
    pass


schema_seed_replace_yml = """
version: 2
seeds:
  - name: data_replace
    config:
      column_types:
        string_text: TEXT
        search_chars: TEXT
        replace_chars: TEXT NULL
        result: TEXT
"""


class TestReplace(BaseReplace):
    @pytest.fixture(scope='class')
    def models(self):
        return {
            'test_replace.yml': models__test_replace_yml,
            'test_replace.sql': self.interpolate_macro_namespace(
                models__test_replace_sql, 'replace'
            ),
            'seeds.yml': schema_seed_replace_yml,
        }


schema_seed_right_yml = """
version: 2
seeds:
  - name: data_right
    config:
      column_types:
        string_text: TEXT
        length_expression: INT
        output: TEXT NULL
"""


class TestRight(BaseRight):
    @pytest.fixture(scope='class')
    def models(self):
        return {
            'test_right.yml': models__test_right_yml,
            'test_right.sql': self.interpolate_macro_namespace(
                models__test_right_sql, 'right'
            ),
            'seeds.yml': schema_seed_right_yml,
        }


schema_seed_safe_cast_yml = """
version: 2
seeds:
  - name: data_safe_cast
    config:
      column_types:
        field: TEXT NULL
        output: TEXT NULL
"""


class TestSafeCast(BaseSafeCast):
    @pytest.fixture(scope='class')
    def models(self):
        return {
            'test_safe_cast.yml': models__test_safe_cast_yml,
            'test_safe_cast.sql': self.interpolate_macro_namespace(
                self.interpolate_macro_namespace(
                    models__test_safe_cast_sql, 'safe_cast'
                ),
                'type_string',
            ),
            'seeds.yml': schema_seed_safe_cast_yml,
        }


schema_seed_split_part_yml = """
version: 2
seeds:
  - name: data_split_part
    config:
      column_types:
        parts: TEXT NULL
        split_on: TEXT
        result_1: TEXT NULL
        result_2: TEXT NULL
        result_3: TEXT NULL
"""


@mark.skip('Firebolt does not support reading delimiter from a column. FIR-16783')
class TestSplitPart(BaseSplitPart):
    @pytest.fixture(scope='class')
    def models(self):
        return {
            'test_split_part.yml': models__test_split_part_yml,
            'test_split_part.sql': self.interpolate_macro_namespace(
                models__test_split_part_sql, 'split_part'
            ),
            'seeds.yml': schema_seed_split_part_yml,
        }


class TestStringLiteral(BaseStringLiteral):
    pass


class TestCurrentTimestamp(BaseCurrentTimestampAware):
    pass


class TestArrayAppend(BaseArrayAppend):
    @pytest.fixture(scope='class')
    def models(self):
        return {
            'actual.yml': schema_actual_table_yml,
            'actual.sql': models__array_append_actual_sql,
            'expected.yml': schema_expected_table_yml,
            'expected.sql': models__array_append_expected_sql,
        }


class TestArrayConcat(BaseArrayConcat):
    @pytest.fixture(scope='class')
    def models(self):
        return {
            'actual.yml': schema_actual_table_yml,
            'actual.sql': models__array_concat_actual_sql,
            'expected.yml': schema_expected_table_yml,
            'expected.sql': models__array_concat_expected_sql,
        }


class TestArrayConstruct(BaseArrayConstruct):
    @pytest.fixture(scope='class')
    def models(self):
        return {
            'actual.yml': schema_actual_table_yml,
            'actual.sql': models__array_construct_actual_sql,
            'expected.yml': schema_expected_table_yml,
            'expected.sql': models__array_construct_expected_sql,
        }
