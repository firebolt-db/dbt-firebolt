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
    def test_comments(self, project):
        logs = self.run_assert_comments()
        self.matches_comment(logs)


class TestQueryCommentsFirebolt(BaseQueryComments, FireboltTestFixMixin):
    def matches_comment(self, logs) -> bool:
        assert '/* dbt\\nrules! */\\n' in logs


class TestMacroQueryCommentsFirebolt(BaseMacroQueryComments, FireboltTestFixMixin):
    def matches_comment(self, logs) -> bool:
        assert '/* dbt macros\\nare pretty cool */\\n' in logs


class TestMacroArgsQueryCommentsFirebolt(
    BaseMacroArgsQueryComments, FireboltTestFixMixin
):
    def matches_comment(self, logs) -> bool:
        expected_dct = {
            'app': 'dbt++',
            'dbt_version': dbt_version,
            'macro_version': '0.1.0',
            'message': 'blah: default2',
        }
        expected = '/* {} */\n'.format(json.dumps(expected_dct, sort_keys=True))
        assert expected in logs


class TestMacroInvalidQueryCommentsFirebolt(
    BaseMacroInvalidQueryComments, FireboltTestFixMixin
):
    pass


class TestNullQueryCommentsFirebolt(BaseNullQueryComments, FireboltTestFixMixin):
    pass


class TestEmptyQueryCommentsFirebolt(BaseEmptyQueryComments, FireboltTestFixMixin):
    pass
