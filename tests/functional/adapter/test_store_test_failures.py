import os
from typing import Any

from dbt.tests.adapter.store_test_failures_tests.basic import (
    StoreTestFailuresAsExceptions,
    StoreTestFailuresAsGeneric,
    StoreTestFailuresAsInteractions,
    StoreTestFailuresAsProjectLevelEphemeral,
    StoreTestFailuresAsProjectLevelOff,
    StoreTestFailuresAsProjectLevelView,
)
from pytest import fixture, mark


@mark.skipif(bool(os.getenv('PASSWORD')), reason='Not supported in FB 1.0')
class TestStoreTestFailuresAsInteractions(StoreTestFailuresAsInteractions):
    pass


@mark.skipif(bool(os.getenv('PASSWORD')), reason='Not supported in FB 1.0')
class TestStoreTestFailuresAsProjectLevelOff(StoreTestFailuresAsProjectLevelOff):
    @fixture(scope='class')
    def project_config_update(
        self, project_config_update: dict[str, Any], is_firebolt_core: bool
    ) -> dict[str, Any]:
        project_config_update['data_tests'] = {
            'store_failures': False,
            'table_type': 'FACT' if is_firebolt_core else 'DIMENSION',
        }
        # can't have data-tests and tests at the same time
        project_config_update.pop('tests', None)
        return project_config_update


@mark.skipif(bool(os.getenv('PASSWORD')), reason='Not supported in FB 1.0')
class TestStoreTestFailuresAsProjectLevelView(StoreTestFailuresAsProjectLevelView):
    @fixture(scope='class')
    def project_config_update(
        self, project_config_update: dict[str, Any]
    ) -> dict[str, Any]:
        project_config_update['data_tests'] = {
            'store_failures_as': 'view',
        }
        # can't have data-tests and tests at the same time
        project_config_update.pop('tests', None)
        return project_config_update


@mark.skipif(bool(os.getenv('PASSWORD')), reason='Not supported in FB 1.0')
class TestStoreTestFailuresAsGeneric(StoreTestFailuresAsGeneric):
    pass


@mark.skipif(bool(os.getenv('PASSWORD')), reason='Not supported in FB 1.0')
class TestStoreTestFailuresAsProjectLevelEphemeral(
    StoreTestFailuresAsProjectLevelEphemeral
):
    @fixture(scope='class')
    def project_config_update(
        self, project_config_update: dict[str, Any]
    ) -> dict[str, Any]:
        project_config_update['data_tests'] = {
            'store_failures_as': 'ephemeral',
            'store_failures': True,
        }
        # can't have data-tests and tests at the same time
        project_config_update.pop('tests', None)
        return project_config_update


@mark.skipif(bool(os.getenv('PASSWORD')), reason='Not supported in FB 1.0')
class TestStoreTestFailuresAsExceptions(StoreTestFailuresAsExceptions):
    pass
