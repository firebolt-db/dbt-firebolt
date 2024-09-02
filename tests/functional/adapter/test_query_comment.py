import json

from dbt.tests.adapter.query_comment.test_query_comment import (
    BaseEmptyQueryComments,
    BaseMacroArgsQueryComments,
    BaseMacroInvalidQueryComments,
    BaseMacroQueryComments,
    BaseNullQueryComments,
    BaseQueryComments,
)


class TestQueryCommentsFirebolt(BaseQueryComments):
    def test_matches_comment(self, project) -> bool:
        logs = self.run_get_json()
        assert '\\n/* dbt\\nrules! */' in logs


class TestMacroQueryCommentsFirebolt(BaseMacroQueryComments):
    def test_matches_comment(self, project) -> bool:
        logs = self.run_get_json()
        assert '\\n/* dbt macros\\nare pretty cool */' in logs


class TestMacroArgsQueryCommentsFirebolt(BaseMacroArgsQueryComments):
    def test_matches_comment(self, project) -> None:
        logs = self.run_get_json()
        expected_dct = {
            'app': 'dbt++',
            'macro_version': '0.1.0',
            'message': f'blah: {project.adapter.config.target_name}',
        }
        expected = '/* {} */'.format(
            json.dumps(expected_dct, sort_keys=True).replace('"', '\\"')
        )
        assert expected in logs


class TestMacroInvalidQueryCommentsFirebolt(BaseMacroInvalidQueryComments):
    pass


class TestNullQueryCommentsFirebolt(BaseNullQueryComments):
    pass


class TestEmptyQueryCommentsFirebolt(BaseEmptyQueryComments):
    pass
