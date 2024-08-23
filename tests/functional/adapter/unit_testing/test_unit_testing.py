from dbt.tests.adapter.unit_testing.test_case_insensitivity import (
    BaseUnitTestCaseInsensivity,
)
from dbt.tests.adapter.unit_testing.test_invalid_input import (
    BaseUnitTestInvalidInput,
)
from dbt.tests.adapter.unit_testing.test_types import BaseUnitTestingTypes
from pytest import fixture


class TestFireboltUnitTestCaseInsensitivity(BaseUnitTestCaseInsensivity):
    pass


class TestFireboltUnitTestInvalidInput(BaseUnitTestInvalidInput):
    pass


class TestFireboltUnitTestingTypes(BaseUnitTestingTypes):
    @fixture
    def data_types(self):
        # sql_value, yaml_value
        return [
            ["1", "1"],
            ["'1'", "1"],
            ["true", "true"],
            ["DATE '2020-01-02'", "2020-01-02"],
            ["TIMESTAMP '2013-11-03 00:00:00'", "2013-11-03 00:00:00"],
            ["TIMESTAMPTZ '2013-11-03 00:00:00-0'", "2013-11-03 00:00:00-0"],
            ["'1'::numeric", "1"],
            [
                """'{"bar": "baz", "balance": 7.77, "active": false}'""", # json
                """'{"bar": "baz", "balance": 7.77, "active": false}'""",
            ],
            ["ARRAY['a','b','c']", """'{"a", "b", "c"}'"""],
            ["ARRAY[1,2,3]", """'{1, 2, 3}'"""],
        ]
