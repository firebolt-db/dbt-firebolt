from dbt.tests.adapter.basic.expected_catalog import (
    base_expected_catalog,
    expected_references_catalog,
    no_stats,
)
from dbt.tests.adapter.basic.files import (
    base_materialized_var_sql,
    base_view_sql,
    config_materialized_table,
    model_incremental,
    schema_base_yml,
)
from dbt.tests.adapter.basic.test_adapter_methods import BaseAdapterMethod
from dbt.tests.adapter.basic.test_base import BaseSimpleMaterializations
from dbt.tests.adapter.basic.test_docs_generate import (
    BaseDocsGenerate,
    BaseDocsGenReferences,
    models__model_sql,
    models__readme_md,
    models__schema_yml,
)
from dbt.tests.adapter.basic.test_empty import BaseEmpty
from dbt.tests.adapter.basic.test_ephemeral import BaseEphemeral
from dbt.tests.adapter.basic.test_generic_tests import BaseGenericTests
from dbt.tests.adapter.basic.test_incremental import (
    BaseIncremental,
    BaseIncrementalNotSchemaChange,
)
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
from dbt.tests.util import run_dbt_and_capture
from pytest import fixture, mark

from tests.conftest import is2_0


class AnySpecifiedType:
    """AnySpecifiedType("AUTO")"""

    def __init__(self, types=[]):
        self.types = types

    def __eq__(self, other):
        if not isinstance(other, str):
            return False

        if not self.types:
            return False

        return other in self.types

    def __repr__(self):
        return 'AnySpecifiedType<{!r}>'.format(self.types)


class TestSimpleMaterializationsFirebolt(BaseSimpleMaterializations):
    # Adding comment to verify CTAS wrapping
    # more info in PR #122
    my_model_base = """
    select * from {{ source('raw', 'seed') }} -- Some Comment"""
    my_base_table_sql = config_materialized_table + my_model_base

    @fixture(scope='class')
    def models(self):
        return {
            'view_model.sql': base_view_sql,
            'table_model.sql': self.my_base_table_sql,
            'swappable.sql': base_materialized_var_sql,
            'schema.yml': schema_base_yml,
        }


class TestSingularTestsFirebolt(BaseSingularTests):
    pass


class TestSingularTestsEphemeralFirebolt(BaseSingularTestsEphemeral):
    pass


class TestEmptyFirebolt(BaseEmpty):
    pass


class TestEphemeralFirebolt(BaseEphemeral):
    pass


@mark.xfail(condition=not is2_0(), reason='Not supported in Firebolt 1.0 and Core')
class TestIncrementalMergeFirebolt(BaseIncremental):
    config_materialized_incremental = """
    {{ config(materialized="incremental", strategy="merge") }}
    """
    incremental_sql = config_materialized_incremental + model_incremental

    @fixture(scope='class')
    def models(self):
        return {'incremental.sql': self.incremental_sql, 'schema.yml': schema_base_yml}


class TestIncrementalFirebolt(BaseIncremental):
    pass


class TestGenericTestsFirebolt(BaseGenericTests):
    pass


class TestSnapshotCheckColsFirebolt(BaseSnapshotCheckCols):
    pass


class TestSnapshotTimestampFirebolt(BaseSnapshotTimestamp):
    pass


class TestBaseAdapterMethod(BaseAdapterMethod):
    pass


# Removing schema here in order for the second model to be written
# to catalog. Firebolt does not support any schemas apart from
# public. TODO: remove this override once schema support is added.
models__second_model_sql = """
{{
    config(
        materialized='view',
    )
}}

select * from {{ ref('seed') }}
"""


class TestDocsGenerateFirebolt(BaseDocsGenerate):

    # TODO: remove this override once schema support is added.
    @fixture(scope='class')
    def unique_schema(request, prefix) -> str:
        return 'public'

    @fixture(autouse=True)
    def clean_up(self, project):
        # No schema support so we can't clean up schemas
        yield

    @fixture(scope='class')
    def models(self):
        return {
            'schema.yml': models__schema_yml,
            'second_model.sql': models__second_model_sql,
            'readme.md': models__readme_md,
            'model.sql': models__model_sql,
        }

    @fixture(scope='class')
    def expected_catalog(self, project, profile_user):
        catalog = base_expected_catalog(
            project,
            role=None,  # No per-table roles in Firebolt
            id_type=AnySpecifiedType(['INT', 'INTEGER']),
            text_type='TEXT',
            time_type='TIMESTAMP',
            view_type='VIEW',
            table_type='BASE TABLE' if is2_0() else 'DIMENSION',
            model_stats=no_stats(),
        )
        # Can't have any other schema apart from public at the moment.
        # TODO: remove once Firebolt supports schemas
        catalog['nodes']['model.test.second_model']['metadata']['schema'] = 'public'
        return catalog


class TestDocsGenReferencesFirebolt(BaseDocsGenReferences):

    # TODO: remove this override once schema support is added.
    @fixture(scope='class')
    def unique_schema(request, prefix) -> str:
        return 'public'

    @fixture(scope='class')
    def expected_catalog(self, project, profile_user):
        return expected_references_catalog(
            project,
            role=None,  # No per-table roles in Firebolt
            id_type=AnySpecifiedType(['INT', 'INTEGER']),
            text_type='TEXT',
            time_type='TIMESTAMP',
            bigint_type=AnySpecifiedType(['BIGINT', 'LONG']),
            view_type='VIEW',
            table_type='BASE TABLE' if is2_0() else 'DIMENSION',
            model_stats=no_stats(),
        )


@mark.skip('Firebolt does not support schema change yet')
class TestIncrementalNotSchemaChange(BaseIncrementalNotSchemaChange):
    pass


class TestFireboltCorrectTypeMaterialisation(BaseGenericTests):
    @fixture(scope='class')
    def snapshots(self):
        return {
            'test_snapshot.sql': """
                {% snapshot test_snapshot %}
                    {{
                        config(
                            target_schema='public',
                            unique_key='id',
                            strategy='timestamp',
                            updated_at='some_date',
                        )
                    }}
                    select * from {{ ref('base') }}
                {% endsnapshot %}
            """
        }

    def test_table_type_based_on_is_firebolt_core(
        self, project, is_firebolt_core: bool
    ):
        # Determine expected and unexpected table types based on Firebolt type
        expected_table = 'FACT' if is_firebolt_core else 'DIMENSION'
        expected_table_query = f'CREATE {expected_table} TABLE'
        unexpected_table = 'DIMENSION' if is_firebolt_core else 'FACT'
        unexpected_table_query = f'CREATE {unexpected_table} TABLE'

        for command in ['seed', 'run', 'snapshot', 'test']:
            # Run the command with debug and JSON logging to capture SQL statements
            results, log_output = run_dbt_and_capture(
                ['--debug', '--log-format=json', command], expect_pass=True
            )
            assert results is not None, f'Command {command} failed'

            # Check for unexpected table type - fail immediately if found
            assert unexpected_table_query not in log_output, (
                f"Found unexpected '{unexpected_table_query}' in dbt " f'{command} logs'
            )

            # Check for expected table type
            if command in ['seed', 'run', 'snapshot']:
                assert expected_table_query in log_output, (
                    f"No instances of '{expected_table_query}' found "
                    f'in dbt {command} logs'
                )
