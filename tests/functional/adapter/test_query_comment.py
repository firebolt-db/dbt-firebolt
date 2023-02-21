import json

from dbt.tests.adapter.query_comment.test_query_comment import (
    BaseEmptyQueryComments,
    BaseMacroArgsQueryComments,
    BaseMacroInvalidQueryComments,
    BaseMacroQueryComments,
    BaseNullQueryComments,
    BaseQueryComments,
)
from dbt.version import __version__ as dbt_version


class FireboltTestFixMixin:
    """
    Fixing Base class test_comments method to actually
    assert that the logs contain the expected string.
    """

    def test_comments(self, project):
        logs = self.run_assert_comments()
        self.matches_comment(logs)


class TestQueryCommentsFirebolt(FireboltTestFixMixin, BaseQueryComments):
    def matches_comment(self, logs) -> bool:
        assert '\\n/* dbt\\nrules! */' in logs


class TestMacroQueryCommentsFirebolt(FireboltTestFixMixin, BaseMacroQueryComments):
    def matches_comment(self, logs) -> bool:
        assert '\\n/* dbt macros\\nare pretty cool */' in logs


class TestMacroArgsQueryCommentsFirebolt(
    FireboltTestFixMixin, BaseMacroArgsQueryComments
):
    def matches_comment(self, logs) -> bool:
        expected_dct = {
            'app': 'dbt++',
            'dbt_version': dbt_version,
            'macro_version': '0.1.0',
            'message': 'blah: default',
        }
        expected = '/* {} */'.format(
            json.dumps(expected_dct, sort_keys=True).replace('"', '\\"')
        )
        assert expected in logs


class TestMacroInvalidQueryCommentsFirebolt(BaseMacroInvalidQueryComments):
    pass


class TestNullQueryCommentsFirebolt(FireboltTestFixMixin, BaseNullQueryComments):
    pass


class TestEmptyQueryCommentsFirebolt(FireboltTestFixMixin, BaseEmptyQueryComments):
    pass
