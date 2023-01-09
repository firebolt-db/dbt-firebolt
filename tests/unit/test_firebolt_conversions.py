import string
from typing import List

import agate
from dbt.clients import agate_helper

from dbt.adapters.firebolt import FireboltAdapter


def _get_tester_for(column_type: type) -> agate.TypeTester:
    if column_type is agate.TimeDelta:  # dbt never makes this!
        return agate.TimeDelta()

    for instance in agate_helper.DEFAULT_TYPE_TESTER._possible_types:
        if isinstance(instance, column_type):  # include child types
            return instance

    raise ValueError(f'no tester for {column_type}')


def _make_table_of(rows: List[List[str]], column_types: List[str]) -> agate.Table:
    column_names = list(string.ascii_letters[: len(rows[0])])
    if isinstance(column_types, type):
        column_types = [_get_tester_for(column_types) for _ in column_names]
    else:
        column_types = [_get_tester_for(typ) for typ in column_types]
    table = agate.Table(rows, column_names=column_names, column_types=column_types)
    return table


def test_convert_text_type():
    rows = [
        ['', 'a1', 'stringval1'],
        ['', 'a2', 'stringvalasdfasdfasdfa'],
        ['', 'a3', 'stringval3'],
    ]
    agate_table = _make_table_of(rows, agate.Text)
    expected = ['text', 'text', 'text']
    for col_idx, expect in enumerate(expected):
        assert FireboltAdapter.convert_text_type(agate_table, col_idx) == expect


def test_convert_number_type():
    rows = [
        ['', '23.98', '-1'],
        ['', '12.78', '-2'],
        ['', '79.41', '-3'],
    ]
    agate_table = _make_table_of(rows, agate.Number)
    expected = ['INT', 'FLOAT', 'INT']
    for col_idx, expect in enumerate(expected):
        assert FireboltAdapter.convert_number_type(agate_table, col_idx) == expect


def test_convert_boolean_type():
    rows = [
        ['', 'false', 'true'],
        ['', 'false', 'false'],
        ['', 'false', 'true'],
    ]
    agate_table = _make_table_of(rows, agate.Boolean)
    expected = ['boolean', 'boolean', 'boolean']
    for col_idx, expect in enumerate(expected):
        assert FireboltAdapter.convert_boolean_type(agate_table, col_idx) == expect


def test_convert_datetime_type():
    rows = [
        ['', '20190101T01:01:01Z', '2019-01-01 01:01:01'],
        ['', '20190102T01:01:01Z', '2019-01-01 01:01:01'],
        ['', '20190103T01:01:01Z', '2019-01-01 01:01:01'],
    ]
    agate_table = _make_table_of(
        rows, [agate.DateTime, agate_helper.ISODateTime, agate.DateTime]
    )
    expected = ['DATETIME', 'DATETIME', 'DATETIME']
    for col_idx, expect in enumerate(expected):
        assert FireboltAdapter.convert_datetime_type(agate_table, col_idx) == expect


def test_convert_date_type():
    rows = [
        ['', '2019-01-01', '2019-01-04'],
        ['', '2019-01-02', '2019-01-04'],
        ['', '2019-01-03', '2019-01-04'],
    ]
    agate_table = _make_table_of(rows, agate.Date)
    expected = ['date', 'date', 'date']
    for col_idx, expect in enumerate(expected):
        assert FireboltAdapter.convert_date_type(agate_table, col_idx) == expect
