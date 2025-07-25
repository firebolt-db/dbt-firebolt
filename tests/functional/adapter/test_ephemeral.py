import os
import re

import pytest
from dbt.tests.adapter.ephemeral.test_ephemeral import (
    BaseEphemeralMulti,
    models__base__base_copy_sql,
    models__base__base_sql,
    models__base__female_only_sql,
    models__dependent_sql,
    models__double_dependent_sql,
    models__super_dependent_sql,
)
from dbt.tests.util import check_relations_equal, run_dbt

schema_dependent_table_yml = """
version: 2
models:
  - name: dependent
    config:
      materialized: table
"""
schema_double_dependent_table_yml = """
version: 2
models:
  - name: double_dependent
    config:
      materialized: table
"""
schema_super_dependent_table_yml = """
version: 2
models:
  - name: super_dependent
    config:
      materialized: table
"""


class TestEphemeralMultiFirebolt(BaseEphemeralMulti):
    @pytest.fixture(scope='class')
    def models(self):
        return {
            'dependent.yml': schema_dependent_table_yml,
            'dependent.sql': models__dependent_sql,
            'double_dependent.yml': schema_double_dependent_table_yml,
            'double_dependent.sql': models__double_dependent_sql,
            'super_dependent.yml': schema_super_dependent_table_yml,
            'super_dependent.sql': models__super_dependent_sql,
            'base': {
                'female_only.sql': models__base__female_only_sql,
                'base.sql': models__base__base_sql,
                'base_copy.sql': models__base__base_copy_sql,
            },
        }

    def test_ephemeral_multi(self, project, is_firebolt_core):
        run_dbt(['seed'])
        results = run_dbt(['run'])
        assert len(results) == 3

        check_relations_equal(project.adapter, ['seed', 'dependent'])
        check_relations_equal(project.adapter, ['seed', 'double_dependent'])
        check_relations_equal(project.adapter, ['seed', 'super_dependent'])
        assert os.path.exists('./target/run/test/models/double_dependent.sql')
        with open('./target/run/test/models/double_dependent.sql', 'r') as fp:
            sql_file = fp.read()

        sql_file = re.sub(r'\d+', '', sql_file)
        table_type = 'DIMENSION' if not is_firebolt_core else 'FACT'
        expected_sql = (
            f'CREATE {table_type} TABLE IF NOT EXISTS double_dependent AS ('
            'with __dbt__cte__base as ('
            'select * from public.seed'
            '),  __dbt__cte__base_copy as ('
            'select * from __dbt__cte__base'
            ')-- base_copy just pulls from base. Make sure the listed'
            '-- graph of CTEs all share the same dbt_cte__base cte'
            "select * from __dbt__cte__base where gender = 'Male'"
            'union all'
            "select * from __dbt__cte__base_copy where gender = 'Female'"
            ')'
        )
        sql_file = ''.join(sql_file.split())
        expected_sql = ''.join(expected_sql.split())
        assert sql_file == expected_sql
