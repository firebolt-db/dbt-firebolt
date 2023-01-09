import string
from unittest import TestCase

import agate


class TestAdapterConversions(TestCase):
    def _get_tester_for(self, column_type):
        from dbt.clients import agate_helper

        if column_type is agate.TimeDelta:  # dbt never makes this!
            return agate.TimeDelta()

        for instance in agate_helper.DEFAULT_TYPE_TESTER._possible_types:
            if isinstance(instance, column_type):  # include child types
                return instance

        raise ValueError(f'no tester for {column_type}')

    def _make_table_of(self, rows, column_types):
        column_names = list(string.ascii_letters[: len(rows[0])])
        if isinstance(column_types, type):
            column_types = [self._get_tester_for(column_types) for _ in column_names]
        else:
            column_types = [self._get_tester_for(typ) for typ in column_types]
        table = agate.Table(rows, column_names=column_names, column_types=column_types)
        return table
