from dbt.tests.adapter.column_types.fixtures import schema_yml
from dbt.tests.adapter.column_types.test_column_types import BaseColumnTypes
from pytest import fixture

# Override since there is no smallint in Firebolt
model_sql = """
select
    1::integer as smallint_col,
    2::integer as int_col,
    3::bigint as bigint_col,
    4.0::real as real_col,
    5.0::double precision as double_col,
    6.0::numeric as numeric_col,
    '7'::text as text_col,
    '8'::varchar(20) as varchar_col
"""


class TestFireboltColumnTypes(BaseColumnTypes):
    @fixture(scope='class')
    def models(self):
        return {'model.sql': model_sql, 'schema.yml': schema_yml}

    def test_run_and_test(self, project):
        self.run_and_test()
