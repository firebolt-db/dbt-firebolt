from typing import Any

from dbt.tests.adapter.simple_copy.test_simple_copy import (
    EmptyModelsArentRunBase,
    SimpleCopyBase,
)
from pytest import fixture, mark


class TestSimpleCopyBase(SimpleCopyBase):
    @mark.skip(reason="Firebolt doesn't support materialized views")
    def test_simple_copy_with_materialized_views(self, project):
        super().test_simple_copy_with_materialized_views(project)


class TestEmptyModelsArentRun(EmptyModelsArentRunBase):
    @fixture(scope='class')
    def project_config_update(
        self, project_config_update: dict[str, Any]
    ) -> dict[str, Any]:
        project_config_update['seeds'] = project_config_update['seeds'] | {
            'quote_columns': False
        }
        return project_config_update
