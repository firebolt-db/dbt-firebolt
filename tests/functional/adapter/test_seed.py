from dbt.tests.adapter.simple_seed.test_seed import SeedUniqueDelimiterTestBase
from dbt.tests.util import run_dbt
from pytest import fixture

seeds__expected_sql = """
create table {schema}.seed_expected (
seed_id INTEGER,
first_name TEXT,
email TEXT,
ip_address TEXT,
birthday TIMESTAMPNTZ
);"""


class TestSeedWithWrongDelimiter(SeedUniqueDelimiterTestBase):
    @fixture(scope='class', autouse=True)
    def setUp(self, project):
        """Create table for ensuring seeds and models used in tests build correctly"""
        project.run_sql(seeds__expected_sql)

    @fixture(scope='class')
    def project_config_update(self):
        return {
            'seeds': {'quote_columns': False, 'delimiter': ';'},
        }

    def test_seed_with_wrong_delimiter(self, project):
        """Testing failure of running dbt seed with a wrongly configured delimiter"""
        seed_result = run_dbt(['seed'], expect_pass=False)
        assert 'syntax error' in seed_result.results[0].message.lower()  # type: ignore


class TestSeedWithEmptyDelimiter(SeedUniqueDelimiterTestBase):
    @fixture(scope='class', autouse=True)
    def setUp(self, project):
        """Create table for ensuring seeds and models used in tests build correctly"""
        project.run_sql(seeds__expected_sql)

    @fixture(scope='class')
    def project_config_update(self):
        return {
            'seeds': {'quote_columns': False, 'delimiter': ''},
        }

    def test_seed_with_empty_delimiter(self, project):
        """
        Testing failure of running dbt seed with an empty configured delimiter value
        """
        seed_result = run_dbt(['seed'], expect_pass=False)
        message = seed_result.results[0].message.lower()  # type: ignore
        assert 'compilation error' in message
