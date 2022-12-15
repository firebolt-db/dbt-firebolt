from dbt.tests.adapter.grants.base_grants import BaseGrants
from dbt.tests.adapter.grants.test_incremental_grants import (
    BaseIncrementalGrants,
)
from dbt.tests.adapter.grants.test_invalid_grants import BaseInvalidGrants
from dbt.tests.adapter.grants.test_model_grants import (
    BaseModelGrants,
    model_schema_yml,
    my_model_sql,
    table_model_schema_yml,
)
from dbt.tests.adapter.grants.test_seed_grants import BaseSeedGrants
from dbt.tests.adapter.grants.test_snapshot_grants import BaseSnapshotGrants
from dbt.tests.util import run_dbt_and_capture, write_file
from pytest import fixture, mark


class BaseGrantsFirebolt(BaseGrants):
    pass


class TestGrantsFailWithException(BaseGrants):
    """
    Verify compile-time errors when trying to use
    grant functionality in Firebolt.
    """

    @fixture(scope='class')
    def models(self):
        updated_schema = self.interpolate_name_overrides(model_schema_yml)
        return {
            'my_model.sql': my_model_sql,
            'schema.yml': updated_schema,
        }

    def test_view_table_grants(self, project, get_test_users):
        # View materialization, single select grant
        (results, log_output) = run_dbt_and_capture(
            ['--debug', 'run'], expect_pass=False
        )
        assert 'Firebolt does not support table-level permission grants' in log_output

        # Table materialization, single select grant
        updated_yaml = self.interpolate_name_overrides(table_model_schema_yml)
        write_file(updated_yaml, project.project_root, 'models', 'schema.yml')
        (results, log_output) = run_dbt_and_capture(
            ['--debug', 'run'], expect_pass=False
        )
        assert 'Firebolt does not support table-level permission grants' in log_output


@mark.skip('Table-level grants are not supported yet in Firebolt')
class TestModelGrantsFirebolt(BaseGrantsFirebolt, BaseModelGrants):
    pass


@mark.skip('Table-level grants are not supported yet in Firebolt')
class TestIncrementalGrantsFirebolt(BaseGrantsFirebolt, BaseIncrementalGrants):
    pass


@mark.skip('Table-level grants are not supported yet in Firebolt')
class TestSeedGrantsFirebolt(BaseGrantsFirebolt, BaseSeedGrants):
    pass


@mark.skip('Table-level grants are not supported yet in Firebolt')
class TestSnapshotGrantsFirebolt(BaseGrantsFirebolt, BaseSnapshotGrants):
    pass


@mark.skip('Table-level grants are not supported yet in Firebolt')
class TestInvalidGrantsFirebolt(BaseGrantsFirebolt, BaseInvalidGrants):
    pass
