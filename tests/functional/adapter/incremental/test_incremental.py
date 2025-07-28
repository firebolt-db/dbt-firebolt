from typing import Any, Dict

from dbt.tests.adapter.incremental.test_incremental_predicates import (
    BaseIncrementalPredicates,
)
from dbt.tests.adapter.incremental.test_incremental_unique_id import (
    BaseIncrementalUniqueKey,
)
from pytest import fixture, mark


class TestIncrementalPredicatesDeleteInsertFirebolt(BaseIncrementalPredicates):
    @fixture(scope='class')
    def project_config_update(
        self, project_config_update: Dict[str, Any]
    ) -> Dict[str, Any]:  # type: ignore
        project_config_update['models'] = project_config_update['models'] | {
            '+predicates': ['id != 2'],
            '+incremental_strategy': 'delete+insert',
        }
        return project_config_update


@mark.skip('No support for unique keys in default incremental strategy')
class TestIncrementalUniqueKeyFirebolt(BaseIncrementalUniqueKey):
    pass


class TestUniqueKeyDeleteInsertFirebolt(BaseIncrementalUniqueKey):
    @fixture(scope='class')
    def project_config_update(
        self, project_config_update: Dict[str, Any]
    ) -> Dict[str, Any]:
        project_config_update['models'] = project_config_update['models'] | {
            '+incremental_strategy': 'delete+insert'
        }
        return project_config_update
