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
        'api_endpoint': os.getenv('API_ENDPOINT'),
        'account_name': os.getenv('ACCOUNT_NAME'),
        'database': os.getenv('DATABASE_NAME'),
        'engine_name': os.getenv('ENGINE_NAME'),
        'user': os.getenv('USER_NAME'),
        'password': os.getenv('PASSWORD'),
        'schema': 'test',
        'port': '443',
        'threads': '1',
    }
