
from pytest import fixture
from dbt.tests.util import (
    run_dbt,
    check_result_nodes_by_name,
)
from dbt.tests.adapter.basic.files import (
    seeds_base_csv,
    base_table_sql,
    model_base,
    schema_base_yml
)

config_materialized_view = """
  {{ config(materialized="materialized_view") }}
"""
materialized_view_sql = config_materialized_view + model_base


class TestMaterializedViews:
    @fixture(scope="class")
    def models(self):
        return {
            "view_model.sql": materialized_view_sql,
            "table_model.sql": base_table_sql,
            "schema.yml": schema_base_yml,
        }

    @fixture(scope="class")
    def seeds(self):
        return {
            "base.csv": seeds_base_csv,
        }

    @fixture(scope="class")
    def project_config_update(self):
        return {"name": "materialized_views"}

    def test_singular_tests(self, project):
        run_dbt(["seed"], expect_pass=True)
        # test command
        results = run_dbt(["run"], expect_pass=False)
        assert len(results) == 2

        # We have the right result nodes
        check_result_nodes_by_name(results, ["table_model", "view_model"])

        # Check result status
        for result in results:
            if result.node.name == "table_model":
                assert result.status == "success"
            if result.node.name == "view_model":
                assert result.status == "error"
