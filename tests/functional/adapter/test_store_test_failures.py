import os

from dbt.tests.adapter.store_test_failures_tests.basic import (
    StoreTestFailuresAsExceptions,
    StoreTestFailuresAsGeneric,
    StoreTestFailuresAsInteractions,
    StoreTestFailuresAsProjectLevelEphemeral,
    StoreTestFailuresAsProjectLevelOff,
    StoreTestFailuresAsProjectLevelView,
)
from pytest import mark


@mark.skipif(bool(os.getenv('PASSWORD')), reason='Not supported in FB 1.0')
class TestStoreTestFailuresAsInteractions(StoreTestFailuresAsInteractions):
    pass


@mark.skipif(bool(os.getenv('PASSWORD')), reason='Not supported in FB 1.0')
class TestStoreTestFailuresAsProjectLevelOff(StoreTestFailuresAsProjectLevelOff):
    pass


@mark.skipif(bool(os.getenv('PASSWORD')), reason='Not supported in FB 1.0')
class TestStoreTestFailuresAsProjectLevelView(StoreTestFailuresAsProjectLevelView):
    pass


@mark.skipif(bool(os.getenv('PASSWORD')), reason='Not supported in FB 1.0')
class TestStoreTestFailuresAsGeneric(StoreTestFailuresAsGeneric):
    pass


@mark.skipif(bool(os.getenv('PASSWORD')), reason='Not supported in FB 1.0')
class TestStoreTestFailuresAsProjectLevelEphemeral(
    StoreTestFailuresAsProjectLevelEphemeral
):
    pass


@mark.skipif(bool(os.getenv('PASSWORD')), reason='Not supported in FB 1.0')
class TestStoreTestFailuresAsExceptions(StoreTestFailuresAsExceptions):
    pass
