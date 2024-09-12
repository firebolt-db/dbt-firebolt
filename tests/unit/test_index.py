import pytest
from dbt.adapters.contracts.relation import Path

from dbt.adapters.firebolt.impl import (
    FireboltAdapter,
    FireboltConfig,
    FireboltIndexConfig,
    FireboltRelation,
    quote_columns,
)


@pytest.mark.parametrize(
    'column, expected',
    [
        ('column', '"column"'),
        (['column1', 'column2'], ['"column1"', '"column2"']),
        (['"column1"', '"column2"'], ['"column1"', '"column2"']),
        (['column 1', 'column 2'], ['"column 1"', '"column 2"']),
    ],
)
def test_quote_columns(column, expected):
    assert quote_columns(column) == expected


def test_firebolt_index_config_render_name():
    relation = FireboltRelation(
        path=Path(database='my_database', schema='my_schema', identifier='my_model')
    )
    index_config = FireboltIndexConfig(
        index_type='join',
        join_columns=['column1', 'column2'],
        dimension_column='dimension',
    )
    assert index_config.render_name(relation).startswith(
        'join_idx__my_model__column1_column2'
    )


def test_firebolt_index_config_render_name_with_special_characters():
    relation = FireboltRelation(
        path=Path(database='my_database', schema='my_schema', identifier='my_model')
    )
    index_config = FireboltIndexConfig(
        index_type='join',
        join_columns=['"column 1"', '"column2"'],
        dimension_column='dimension',
    )
    assert index_config.render_name(relation).startswith(
        'join_idx__my_model__column_1_column2'
    )


def test_firebolt_index_config_render_with_long_name():
    relation = FireboltRelation(
        path=Path(database='my_database', schema='my_schema', identifier='my_model')
    )
    index_config = FireboltIndexConfig(
        index_type='join', join_columns=['column1'] * 50, dimension_column='dimension'
    )
    assert len(index_config.render_name(relation)) < 255


def test_firebolt_index_config_parse():
    raw_index = {
        'index_type': 'join',
        'join_columns': ['column1', 'column2'],
        'dimension_column': 'dimension',
    }
    index_config = FireboltIndexConfig.parse(raw_index)
    assert index_config.index_type == 'join'
    assert index_config.join_columns == ['"column1"', '"column2"']
    assert index_config.dimension_column == '"dimension"'


def test_firebolt_config():
    config = FireboltConfig(
        indexes=[
            FireboltIndexConfig(
                index_type='join',
                join_columns=['column1', 'column2'],
                dimension_column='dimension',
            ),
            FireboltIndexConfig(
                index_type='aggregating',
                key_columns=['key1', 'key2'],
                aggregation='aggregation',
            ),
        ]
    )
    assert len(config.indexes) == 2


def test_firebolt_adapter_is_cancelable():
    assert not FireboltAdapter.is_cancelable()


def test_firebolt_adapter_date_function():
    assert FireboltAdapter.date_function() == 'now()'
