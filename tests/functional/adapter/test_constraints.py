from dbt.tests.adapter.constraints.test_constraints import (
    BaseConstraintQuotedColumn,
    BaseConstraintsRollback,
    BaseConstraintsRuntimeDdlEnforcement,
    BaseIncrementalConstraintsColumnsEqual,
    BaseIncrementalConstraintsRollback,
    BaseIncrementalConstraintsRuntimeDdlEnforcement,
    BaseModelConstraintsRuntimeEnforcement,
    BaseTableConstraintsColumnsEqual,
    BaseViewConstraintsColumnsEqual,
)
from pytest import fixture, mark

_expected_sql = """
create dimension table if not exists <model_identifier> (
    id integer not null unique,
    color text,
    date_day text
) ;
insert into <model_identifier> (
    id ,
    color ,
    date_day
)
    select
       id,
       color,
       date_day
       from
    (
        -- depends_on: <foreign_key_model_identifier>
        select
            'blue' as color,
            1 as id,
            '2019-01-01' as date_day
    ) as model_subq
"""


class FireboltTestTypesMixin:
    # Overriding as some of the data types are not supported by firebolt
    @fixture
    def data_types(self, schema_int_type, int_type, string_type):
        # sql_column_value, schema_data_type, error_data_type
        return [
            ['1', schema_int_type, int_type],
            ["'1'", string_type, string_type],
            ['true', 'bool', 'boolean'],
            ["'2013-11-03 00:00:00-07'::timestamptz", 'timestamptz', 'timestamp'],
            ["ARRAY['a','b','c']", 'text[]', 'array'],
            ['ARRAY[1,2,3]', 'int[]', 'array'],
            ["'1'::numeric", 'numeric', 'Decimal'],
        ]

    @fixture
    def string_type(self):
        return 'text'

    @fixture
    def int_type(self):
        return 'integer'


class TestTableConstraintsColumnsEqualFirebolt(
    FireboltTestTypesMixin, BaseTableConstraintsColumnsEqual
):
    pass


class TestViewConstraintsColumnsEqualFirebolt(
    FireboltTestTypesMixin, BaseViewConstraintsColumnsEqual
):
    pass


class TestIncrementalConstraintsColumnsEqualFirebolt(
    FireboltTestTypesMixin, BaseIncrementalConstraintsColumnsEqual
):
    pass


class TestConstraintsRuntimeDdlEnforcementFirebolt(
    BaseConstraintsRuntimeDdlEnforcement
):
    @fixture(scope='class')
    def expected_sql(self):
        return _expected_sql


@mark.skip(reason='Firebolt does not support commit/rollback')
class TestConstraintsRollbackFirebolt(BaseConstraintsRollback):
    pass


class TestIncrementalConstraintsRuntimeDdlEnforcementFirebolt(
    BaseIncrementalConstraintsRuntimeDdlEnforcement
):
    @fixture(scope='class')
    def expected_sql(self):
        return _expected_sql


@mark.skip(reason='Firebolt does not support commit/rollback')
class TestIncrementalConstraintsRollbackFirebolt(BaseIncrementalConstraintsRollback):
    pass


@mark.skip(reason='Firebolt does not support constraint clause in DDL')
class TestModelConstraintsRuntimeEnforcementFirebolt(
    BaseModelConstraintsRuntimeEnforcement
):
    pass


@mark.skip(reason='Firebolt does not support check constraint')
class TestConstraintQuotedColumnFirebolt(BaseConstraintQuotedColumn):
    pass
