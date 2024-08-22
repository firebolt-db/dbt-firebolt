from dbt.tests.adapter.dbt_clone.test_dbt_clone import (
    BaseClone,
    BaseCloneNotPossible,
)
from dbt.tests.util import run_dbt_and_capture
from pytest import mark


# Test clone works by copying tables to a new schema
@mark.skip("Can't test this before schemas are implemented")
class TestFireboltCloneNotPossible(BaseCloneNotPossible):
    pass


class TestCloneSameTargetAndState(BaseClone):
    def test_clone_same_target_and_state(self, project, unique_schema, other_schema):
        project.create_test_schema(other_schema)
        self.run_and_save_state(project.project_root)

        clone_args = [
            'clone',
            '--state',
            'target',
        ]

        results, output = run_dbt_and_capture(clone_args, expect_pass=False)
        assert (
            "Warning: The state and target directories are the same: 'target'" in output
        )
