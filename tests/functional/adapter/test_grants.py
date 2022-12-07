from dbt.tests.adapter.grants.base_grants import BaseGrants
from dbt.tests.adapter.grants.test_incremental_grants import (
    BaseIncrementalGrants,
)
from dbt.tests.adapter.grants.test_invalid_grants import BaseInvalidGrants
from dbt.tests.adapter.grants.test_model_grants import BaseModelGrants
from dbt.tests.adapter.grants.test_seed_grants import BaseSeedGrants
from dbt.tests.adapter.grants.test_snapshot_grants import BaseSnapshotGrants
from pytest import mark


class BaseGrantsFirebolt(BaseGrants):
    pass


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
