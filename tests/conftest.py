import os

import pytest
from dbt.tests.util import write_file
from yaml import SafeDumper, dump_all

# Import the standard functional fixtures as a plugin
# Note: fixtures with session scope need to be local
pytest_plugins = ['dbt.tests.fixtures.project']


class Secret:
    """
    Class to hold sensitive data in testing. This prevents passwords
    and such to be printed in logs or any other reports.
    More info: https://github.com/pytest-dev/pytest/issues/8613
    NOTE: be careful, assert Secret('') == '' would still print
    on failure
    """

    def __init__(self, value):
        self.value = value

    def __repr__(self):
        return 'Secret(********)'

    def __str___(self):
        return '*******'


# The profile dictionary, used to write out profiles.yml
# dbt will supply a unique schema per test, so we do not specify 'schema' here
@pytest.fixture(scope='class')
def dbt_profile_target():
    profile = {
        'type': 'firebolt',
        'threads': 1,
        'api_endpoint': os.getenv('API_ENDPOINT'),
        'account_name': os.getenv('ACCOUNT_NAME'),
        'database': os.getenv('DATABASE_NAME'),
        'engine_name': os.getenv('ENGINE_NAME'),
        'port': 443,
    }
    # add credentials to the profile keys
    if os.getenv('USER_NAME') and os.getenv('PASSWORD'):
        profile['user'] = os.getenv('USER_NAME')
        profile['password'] = Secret(os.getenv('PASSWORD'))
    elif os.getenv('CLIENT_ID') and os.getenv('CLIENT_SECRET'):
        profile['client_id'] = os.getenv('CLIENT_ID')
        profile['client_secret'] = Secret(os.getenv('CLIENT_SECRET'))
    else:
        raise Exception('No credentials found in environment')
    return profile


# Overriding dbt_profile_data in order to set the schema to public.
# Firebolt does not have concept of schemas so this is necessary for
# the test to pass.
@pytest.fixture(scope='class')
def dbt_profile_data(dbt_profile_target, profiles_config_update):
    profile = {
        'config': {'send_anonymous_usage_stats': False},
        'test': {
            'outputs': {
                'default': {},
            },
            'target': 'default',
        },
    }
    target = dbt_profile_target
    target['schema'] = 'public'  # Different from dbt-core in here
    profile['test']['outputs']['default'] = target

    if profiles_config_update:
        profile.update(profiles_config_update)
    return profile


class SafeSecretDumper(SafeDumper):
    """
    A custom dumper that will not dump secrets
    """

    def represent_data(self, data):
        if isinstance(data, Secret):
            return self.represent_scalar('tag:yaml.org,2002:str', '********')
        return super().represent_data(data)


@pytest.fixture(scope='class')
def profiles_yml(profiles_root, dbt_profile_data):
    """Override dbt fixture to work with Secret class in profiles"""
    os.environ['DBT_PROFILES_DIR'] = str(profiles_root)
    write_file(
        dump_all([dbt_profile_data], Dumper=SafeSecretDumper),
        profiles_root,
        'profiles.yml',
    )
    yield dbt_profile_data
    del os.environ['DBT_PROFILES_DIR']


@pytest.fixture(scope='class')
def profile_user(dbt_profile_target):
    return dbt_profile_target.get('user', dbt_profile_target.get('client_id'))
