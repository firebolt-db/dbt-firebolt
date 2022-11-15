import pytest
from dbt.tests.adapter.utils.fixture_bool_or import (
    models__test_bool_or_sql,
    models__test_bool_or_yml,
)
from dbt.tests.adapter.utils.fixture_concat import (
    models__test_concat_sql,
    models__test_concat_yml,
)
from dbt.tests.adapter.utils.fixture_dateadd import (
    models__test_dateadd_sql,
    models__test_dateadd_yml,
)
from dbt.tests.adapter.utils.fixture_datediff import models__test_datediff_yml
from dbt.tests.adapter.utils.test_any_value import BaseAnyValue
from dbt.tests.adapter.utils.test_bool_or import BaseBoolOr
from dbt.tests.adapter.utils.test_cast_bool_to_text import BaseCastBoolToText
from dbt.tests.adapter.utils.test_concat import BaseConcat
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


class TestDateTrunc(BaseDateTrunc):
    pass


class TestEscapeSingleQuotes(BaseEscapeSingleQuotesBackslash):
    pass


class TestExcept(BaseExcept):
    pass


class TestHash(BaseHash):
    pass


class TestIntersect(BaseIntersect):
    pass


class TestLastDay(BaseLastDay):
    pass


class TestLength(BaseLength):
    pass


class TestListagg(BaseListagg):
    pass


class TestPosition(BasePosition):
    pass


class TestReplace(BaseReplace):
    pass


class TestRight(BaseRight):
    pass


class TestSafeCast(BaseSafeCast):
    pass


class TestSplitPart(BaseSplitPart):
    pass


class TestStringLiteral(BaseStringLiteral):
    pass
