import os

import pytest

# Import the standard functional fixtures as a plugin
# Note: fixtures with session scope need to be local
pytest_plugins = ['dbt.tests.fixtures.project']


# The profile dictionary, used to write out profiles.yml
# dbt will supply a unique schema per test, so we do not specify 'schema' here
@pytest.fixture(scope='class')
def dbt_profile_target():
    return {
        'type': 'firebolt',
        'threads': 1,
        'api_endpoint': os.getenv('API_ENDPOINT'),
        'account_name': os.getenv('ACCOUNT_NAME'),
        'database': os.getenv('DATABASE_NAME'),
        'engine_name': os.getenv('ENGINE_NAME'),
        'user': os.getenv('USER_NAME'),
        'password': os.getenv('PASSWORD'),
        'port': 443,
    }


@pytest.fixture(scope="class")
def dbt_profile_data(dbt_profile_target, profiles_config_update):
    """
    Overriding dbt_profile_data in order to set the schema to public.
    Firebolt does not have concept of schemas so this is necessary for
    the test to pass.
    """
    profile = {
        "config": {"send_anonymous_usage_stats": False},
        "test": {
            "outputs": {
                "default": {},
            },
            "target": "default",
        },
    }
    target = dbt_profile_target
    target["schema"] = 'public' # Different from dbt-core in here
    profile["test"]["outputs"]["default"] = target

    if profiles_config_update:
        profile.update(profiles_config_update)
    return profile
