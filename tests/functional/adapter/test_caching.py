from dbt.tests.adapter.caching.test_caching import (
    TestCachingLowerCaseModel,
    TestCachingSelectedSchemaOnly,
    TestCachingUppercaseModel,
    TestNoPopulateCache,
)
from pytest import fixture


# TODO: remove this override once schema support is added.
@fixture(scope='module')
def unique_schema() -> str:
    return 'public'


class TestNoPopulateCacheFirebolt(TestNoPopulateCache):
    pass


class TestCachingLowerCaseModelFirebolt(TestCachingLowerCaseModel):
    pass


class TestCachingUppercaseModelFirebolt(TestCachingUppercaseModel):
    pass


class TestCachingSelectedSchemaOnlyFirebolt(TestCachingSelectedSchemaOnly):
    pass
