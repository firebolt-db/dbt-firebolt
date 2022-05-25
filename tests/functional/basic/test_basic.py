from dbt.tests.adapter.basic.test_adapter_methods import BaseAdapterMethod

# from dbt.tests.adapter.basic.test_base import BaseSimpleMaterializations
# from dbt.tests.adapter.basic.test_ephemeral import BaseEphemeral
# from dbt.tests.adapter.basic.test_incremental import BaseIncremental
# from dbt.tests.adapter.basic.test_snapshot_check_cols import (
#     BaseSnapshotCheckCols,
# )
# from dbt.tests.adapter.basic.test_snapshot_timestamp import (
#     BaseSnapshotTimestamp,
# )


"""
######################## These tests are all passing. ########################
"""


# class TestSingularTestsFirebolt(BaseSingularTests):
#     pass


# class TestSingularTestsEphemeralFirebolt(BaseSingularTestsEphemeral):
#     pass


# class TestEmptyFirebolt(BaseEmpty):
#     pass


# class TestGenericTestsFirebolt(BaseGenericTests):
#     pass


"""
######################## These tests are all failing. ########################
"""


class TestBaseAdapterMethod(BaseAdapterMethod):
    pass


# class TestSimpleMaterializationsFirebolt(BaseSimpleMaterializations):
#     pass


# class TestEphemeralFirebolt(BaseEphemeral):
#     pass


# class TestIncrementalFirebolt(BaseIncremental):
#     pass


# class TestSnapshotCheckColsFirebolt(BaseSnapshotCheckCols):
#     pass


# class TestSnapshotTimestampFirebolt(BaseSnapshotTimestamp):
#     pass
