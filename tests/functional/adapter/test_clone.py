from dbt.tests.adapter.dbt_clone.test_dbt_clone import BaseCloneNotPossible
from pytest import skip

# Test clone works by copying tables to a new schema
@skip("Can't test this before schemas are implemented")
class TestFireboltCloneNotPossible(BaseCloneNotPossible):
    pass
