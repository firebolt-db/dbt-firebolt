from dbt.tests.adapter.incremental.test_incremental_predicates import (
    BaseIncrementalPredicates,
)
from dbt.tests.adapter.incremental.test_incremental_unique_id import (
    BaseIncrementalUniqueKey,
    models__duplicated_unary_unique_key_list_sql,
    models__empty_str_unique_key_sql,
    models__empty_unique_key_list_sql,
    models__expected__one_str__overwrite_sql,
    models__expected__unique_key_list__inplace_overwrite_sql,
    models__no_unique_key_sql,
    models__nontyped_trinary_unique_key_list_sql,
    models__not_found_unique_key_list_sql,
    models__not_found_unique_key_sql,
    models__str_unique_key_sql,
    models__trinary_unique_key_list_sql,
    models__unary_unique_key_list_sql,
)
from pytest import fixture, mark


class TestIncrementalPredicatesDeleteInsertFirebolt(BaseIncrementalPredicates):
    @fixture(scope='class')
    def project_config_update(self):
        return {
            'models': {
                '+predicates': ['id != 2'],
                '+incremental_strategy': 'delete+insert',
            }
        }


@mark.skip('No support for unique keys in default incremental strategy')
class TestIncrementalUniqueKeyFirebolt(BaseIncrementalUniqueKey):
    pass


schema_seed_delete_insert_yml = """
version: 2
seeds:
  - name: seed
    config:
      column_types:
        state: TEXT
        county: TEXT
        city: TEXT NULL
        last_visit_date: TIMESTAMP
"""


class TestUniqueKeyDeleteInsertFirebolt(BaseIncrementalUniqueKey):
    @fixture(scope='class')
    def project_config_update(self):
        return {'models': {'+incremental_strategy': 'delete+insert'}}

    @fixture(scope='class')
    def models(self, is_firebolt_1_0: bool) -> dict[str, str]:
        """
        Override models in order to set seeds nullability for Firebolt 1.0.
        """
        model_dict = {
            'trinary_unique_key_list.sql': models__trinary_unique_key_list_sql,
            'nontyped_trinary_unique_key_list.sql': models__nontyped_trinary_unique_key_list_sql,  # noqa: E501
            'unary_unique_key_list.sql': models__unary_unique_key_list_sql,
            'not_found_unique_key.sql': models__not_found_unique_key_sql,
            'empty_unique_key_list.sql': models__empty_unique_key_list_sql,
            'no_unique_key.sql': models__no_unique_key_sql,
            'empty_str_unique_key.sql': models__empty_str_unique_key_sql,
            'str_unique_key.sql': models__str_unique_key_sql,
            'duplicated_unary_unique_key_list.sql': models__duplicated_unary_unique_key_list_sql,  # noqa: E501
            'not_found_unique_key_list.sql': models__not_found_unique_key_list_sql,
            'expected': {
                'one_str__overwrite.sql': models__expected__one_str__overwrite_sql,
                'unique_key_list__inplace_overwrite.sql': models__expected__unique_key_list__inplace_overwrite_sql,  # noqa: E501
            },
        }
        if is_firebolt_1_0:
            model_dict['seeds.yml'] = schema_seed_delete_insert_yml
        return model_dict
