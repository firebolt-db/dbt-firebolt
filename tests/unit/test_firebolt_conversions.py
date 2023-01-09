import agate
from dbt.clients import agate_helper

from dbt.adapters.firebolt import FireboltAdapter

from .utils import TestAdapterConversions  # noqa: I252


class TestFireboltAdapterConversions(TestAdapterConversions):
    def test_convert_text_type(self):
        rows = [
            ['', 'a1', 'stringval1'],
            ['', 'a2', 'stringvalasdfasdfasdfa'],
            ['', 'a3', 'stringval3'],
        ]
        agate_table = self._make_table_of(rows, agate.Text)
        expected = ['text', 'text', 'text']
        for col_idx, expect in enumerate(expected):
            assert FireboltAdapter.convert_text_type(agate_table, col_idx) == expect

    def test_convert_number_type(self):
        rows = [
            ['', '23.98', '-1'],
            ['', '12.78', '-2'],
            ['', '79.41', '-3'],
        ]
        agate_table = self._make_table_of(rows, agate.Number)
        expected = ['INT', 'FLOAT', 'INT']
        for col_idx, expect in enumerate(expected):
            assert FireboltAdapter.convert_number_type(agate_table, col_idx) == expect

    def test_convert_boolean_type(self):
        rows = [
            ['', 'false', 'true'],
            ['', 'false', 'false'],
            ['', 'false', 'true'],
        ]
        agate_table = self._make_table_of(rows, agate.Boolean)
        expected = ['boolean', 'boolean', 'boolean']
        for col_idx, expect in enumerate(expected):
            assert FireboltAdapter.convert_boolean_type(agate_table, col_idx) == expect

    def test_convert_datetime_type(self):
        rows = [
            ['', '20190101T01:01:01Z', '2019-01-01 01:01:01'],
            ['', '20190102T01:01:01Z', '2019-01-01 01:01:01'],
            ['', '20190103T01:01:01Z', '2019-01-01 01:01:01'],
        ]
        agate_table = self._make_table_of(
            rows, [agate.DateTime, agate_helper.ISODateTime, agate.DateTime]
        )
        expected = ['DATETIME', 'DATETIME', 'DATETIME']
        for col_idx, expect in enumerate(expected):
            assert FireboltAdapter.convert_datetime_type(agate_table, col_idx) == expect

    def test_convert_date_type(self):
        rows = [
            ['', '2019-01-01', '2019-01-04'],
            ['', '2019-01-02', '2019-01-04'],
            ['', '2019-01-03', '2019-01-04'],
        ]
        agate_table = self._make_table_of(rows, agate.Date)
        expected = ['date', 'date', 'date']
        for col_idx, expect in enumerate(expected):
            assert FireboltAdapter.convert_date_type(agate_table, col_idx) == expect
