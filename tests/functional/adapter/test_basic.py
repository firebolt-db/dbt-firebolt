import os

from dbt.tests.adapter.basic.expected_catalog import (
    base_expected_catalog,
    expected_references_catalog,
    no_stats,
)
from dbt.tests.adapter.basic.files import (
    base_materialized_var_sql,
    base_view_sql,
    config_materialized_table,
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
from dbt.tests.util import check_relations_equal, relation_from_name, run_dbt
from pytest import fixture, mark


def is2_0():
    """Helper to check Firebolt version we're testing against"""
    if os.getenv('USER_NAME') and '@' in os.getenv('USER_NAME', ''):
        return False
    return True


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


class TestIncrementalFirebolt(BaseIncremental):
    def test_incremental(self, project):
        # seed command
        results = run_dbt(['seed'])
        assert len(results) == 2

        # base table rowcount
        relation = relation_from_name(project.adapter, 'base')
        result = project.run_sql(
            f'select count(*) as num_rows from {relation}', fetch='one'
        )
        assert result[0] == 10

        # added table rowcount
        relation = relation_from_name(project.adapter, 'added')
        result = project.run_sql(
            f'select count(*) as num_rows from {relation}', fetch='one'
        )
        assert result[0] == 20

        # run command
        # the 'seed_name' var changes the seed identifier in the schema file
        results = run_dbt(['run', '--vars', 'seed_name: base'])
        assert len(results) == 1

        # check relations equal
        check_relations_equal(project.adapter, ['base', 'incremental'])

        # change seed_name var
        # the 'seed_name' var changes the seed identifier in the schema file
        # adding --full-refresh because schema changes are not allowed
        results = run_dbt(['run', '--full-refresh', '--vars', 'seed_name: added'])
        assert len(results) == 1

        # check relations equal
        check_relations_equal(project.adapter, ['added', 'incremental'])

        # get catalog from docs generate
        catalog = run_dbt(['docs', 'generate'])
        assert len(catalog.nodes) == 3
        assert len(catalog.sources) == 1


class TestGenericTestsFirebolt(BaseGenericTests):
    pass


@mark.skip('Not implemented for v1')
class TestSnapshotCheckColsFirebolt(BaseSnapshotCheckCols):
    pass


@mark.skip('Not implemented for v1')
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
