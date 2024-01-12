from dbt.tests.adapter.simple_copy.test_simple_copy import (
    EmptyModelsArentRunBase,
    SimpleCopyBase,
)
from pytest import mark


class TestSimpleCopyBase(SimpleCopyBase):
    @mark.skip(reason="Firebolt doesn't support materialized views")
    def test_simple_copy_with_materialized_views(self, project):
        super().test_simple_copy_with_materialized_views(project)


class TestEmptyModelsArentRun(EmptyModelsArentRunBase):
    pass
