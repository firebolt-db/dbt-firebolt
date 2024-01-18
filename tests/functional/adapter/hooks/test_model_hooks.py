from dbt.tests.adapter.hooks.test_model_hooks import (
    TestDuplicateHooksInConfigs as DuplicateHooksInConfigs,
)
from dbt.tests.adapter.hooks.test_model_hooks import TestHookRefs as HookRefs
from dbt.tests.adapter.hooks.test_model_hooks import (
    TestHooksRefsOnSeeds as HooksRefsOnSeeds,
)
from dbt.tests.adapter.hooks.test_model_hooks import (
    TestPrePostModelHooks as PrePostModelHooks,
)
from dbt.tests.adapter.hooks.test_model_hooks import (
    TestPrePostModelHooksInConfig as PrePostModelHooksInConfig,
)
from dbt.tests.adapter.hooks.test_model_hooks import (
    TestPrePostModelHooksInConfigKwargs as PrePostModelHooksInConfigKwargs,
)
from dbt.tests.adapter.hooks.test_model_hooks import (
    TestPrePostModelHooksInConfigWithCount as PrePostModelHooksInConfigWithCount,
)
from dbt.tests.adapter.hooks.test_model_hooks import (
    TestPrePostModelHooksOnSeeds as PrePostModelHooksOnSeeds,
)
from dbt.tests.adapter.hooks.test_model_hooks import (
    TestPrePostModelHooksOnSeedsPlusPrefixed as PrePostModelHooksOnSeedsPlusPrefixed,
)

# isort: off
from dbt.tests.adapter.hooks.test_model_hooks import (
    TestPrePostModelHooksOnSeedsPlusPrefixedWhitespace as PrePostModelHooksOnSeedsPlusPrefixedWhitespace,  # noqa: E501
)

# isort: on
from dbt.tests.adapter.hooks.test_model_hooks import (
    TestPrePostModelHooksOnSnapshots as PrePostModelHooksOnSnapshots,
)
from dbt.tests.adapter.hooks.test_model_hooks import (
    TestPrePostModelHooksUnderscores as PrePostModelHooksUnderscores,
)
from dbt.tests.adapter.hooks.test_model_hooks import (
    TestPrePostSnapshotHooksInConfigKwargs as PrePostSnapshotHooksInConfigKwargs,
)
from pytest import fixture


@fixture(scope='module')
def unique_schema():
    return 'public'


class FireboltCheckerMixin:
    def check_hooks(self, state, project, host, count=1):
        ctxs = self.get_ctx_vars(state, count=count, project=project)
        for ctx in ctxs:
            # Removing some of the checks from the original test
            # because they don't apply to the current implementation
            assert ctx['test_state'] == state
            assert ctx['target_name'] == 'default'
            assert ctx['target_type'] == 'firebolt'
            assert ctx['target_schema'] == project.test_schema

            assert (
                ctx['run_started_at'] is not None and len(ctx['run_started_at']) > 0
            ), 'run_started_at was not set'
            assert (
                ctx['invocation_id'] is not None and len(ctx['invocation_id']) > 0
            ), 'invocation_id was not set'


# Override to check existing column instead, since we can't add a new column
properties__seed_models = """
version: 2
seeds:
- name: example_seed
  columns:
  - name: c
    tests:
    - accepted_values:
        values: [0]
"""

properties__test_snapshot_models = """
version: 2
snapshots:
- name: example_snapshot
  columns:
  - name: c
    tests:
    - accepted_values:
        values: [0]
"""


class FireboltProjectUpdateMixin:
    # Override because we don't support ALTER TABLE
    @fixture(scope='class')
    def project_config_update(self):
        return {
            'seed-paths': ['seeds'],
            'models': {},
            'seeds': {
                'post-hook': [
                    'update {{ this }} set c = 0',
                    # call any macro to track dependency:
                    # https://github.com/dbt-labs/dbt-core/issues/6806
                    'select null::{{ dbt.type_int() }} as id',
                ],
                'quote_columns': False,
            },
        }

    @fixture(scope='class')
    def models(self):
        return {'schema.yml': properties__seed_models}


class TestPrePostModelHooksFirebolt(FireboltCheckerMixin, PrePostModelHooks):
    pass


class TestPrePostModelHooksUnderscoresFirebolt(
    FireboltCheckerMixin, PrePostModelHooksUnderscores
):
    pass


class TestHookRefsFirebolt(FireboltCheckerMixin, HookRefs):
    pass


class TestPrePostModelHooksOnSeedsFirebolt(
    FireboltProjectUpdateMixin, FireboltCheckerMixin, PrePostModelHooksOnSeeds
):
    pass


class TestHooksRefsOnSeedsFirebolt(FireboltCheckerMixin, HooksRefsOnSeeds):
    pass


class TestPrePostModelHooksOnSeedsPlusPrefixedFirebolt(
    FireboltProjectUpdateMixin,
    FireboltCheckerMixin,
    PrePostModelHooksOnSeedsPlusPrefixed,
):
    pass


class TestPrePostModelHooksOnSeedsPlusPrefixedWhitespaceFirebolt(
    FireboltProjectUpdateMixin,
    FireboltCheckerMixin,
    PrePostModelHooksOnSeedsPlusPrefixedWhitespace,
):
    pass


class TestPrePostModelHooksOnSnapshotsFirebolt(
    FireboltCheckerMixin, PrePostModelHooksOnSnapshots
):
    # Override because we don't support ALTER TABLE
    @fixture(scope='class')
    def project_config_update(self):
        return {
            'seed-paths': ['seeds'],
            'snapshot-paths': ['test-snapshots'],
            'models': {},
            'snapshots': {
                'post-hook': [
                    'update {{ this }} set c = 0',
                ]
            },
            'seeds': {
                'quote_columns': False,
            },
        }

    @fixture(scope='class')
    def models(self):
        return {'schema.yml': properties__test_snapshot_models}


class TestPrePostModelHooksInConfigFirebolt(
    FireboltCheckerMixin, PrePostModelHooksInConfig
):
    pass


class TestPrePostModelHooksInConfigWithCountFirebolt(
    FireboltCheckerMixin, PrePostModelHooksInConfigWithCount
):
    pass


class TestPrePostModelHooksInConfigKwargsFirebolt(
    FireboltCheckerMixin, PrePostModelHooksInConfigKwargs
):
    pass


class TestPrePostSnapshotHooksInConfigKwargsFirebolt(
    FireboltCheckerMixin, PrePostSnapshotHooksInConfigKwargs
):
    @fixture(scope='class')
    def project_config_update(self):
        return {
            'seed-paths': ['seeds'],
            'snapshot-paths': ['test-kwargs-snapshots'],
            'models': {},
            'snapshots': {
                'post-hook': [
                    'update {{ this }} set c = 0',
                ]
            },
            'seeds': {
                'quote_columns': False,
            },
        }

    @fixture(scope='class')
    def models(self):
        return {'schema.yml': properties__test_snapshot_models}


class TestDuplicateHooksInConfigsFirebolt(
    FireboltCheckerMixin, DuplicateHooksInConfigs
):
    pass
