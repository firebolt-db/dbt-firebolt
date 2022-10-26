from dbt.tests.adapter.basic.test_adapter_methods import BaseAdapterMethod
from dbt.tests.adapter.basic.test_base import BaseSimpleMaterializations
from dbt.tests.adapter.basic.test_empty import BaseEmpty
from dbt.tests.adapter.basic.test_ephemeral import BaseEphemeral
from dbt.tests.adapter.basic.test_generic_tests import BaseGenericTests
from dbt.tests.adapter.basic.test_incremental import BaseIncremental
from dbt.tests.adapter.basic.test_singular_tests import BaseSingularTests
from dbt.tests.adapter.basic.test_singular_tests_ephemeral import (
    BaseSingularTestsEphemeral,
)
from dbt.tests.adapter.basic.test_snapshot_check_cols import (
    BaseSnapshotCheckCols,
)
from dbt.tests.adapter.basic.test_snapshot_timestamp import (
    BaseSnapshotTimestamp,
)
from pytest import mark


class TestSimpleMaterializationsFirebolt(BaseSimpleMaterializations):
    pass


class TestSingularTestsFirebolt(BaseSingularTests):
    pass


class TestSingularTestsEphemeralFirebolt(BaseSingularTestsEphemeral):
    pass


class TestEmptyFirebolt(BaseEmpty):
    pass


class TestEphemeralFirebolt(BaseEphemeral):
    pass


class TestIncrementalFirebolt(BaseIncremental):
    pass


class TestGenericTestsFirebolt(BaseGenericTests):
    pass


@mark.skip('Not implemented for v1')
class TestSnapshotCheckColsFirebolt(BaseSnapshotCheckCols):
    pass


@mark.skip('Not implemented for v1')
class TestSnapshotTimestampFirebolt(BaseSnapshotTimestamp):
    pass


@mark.skip('Requires investigation')
class TestBaseAdapterMethod(BaseAdapterMethod):
    pass
