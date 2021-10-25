#!/usr/bin/env python
from setuptools import find_namespace_packages, setup
import os
import re

this_directory = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(this_directory, 'README.md')) as f:
    long_description = f.read()

def _dbt_synapse_version():
    """
    get version from __version__.py so that version is accessible via:
    - `dbt --version` and
    -  dbt debug
    """
    _version_path = os.path.join(
        this_directory, 'dbt', 'adapters', 'firebolt', '__version__.py'
    )
    _version_pattern = r'''version\s*=\s*["'](.+)["']'''
    with open(_version_path) as f:
        match = re.search(_version_pattern, f.read().strip())
        if match is None:
            raise ValueError(f'invalid version at {_version_path}')
        return match.group(1)

package_name = "dbt-firebolt"
package_version = _dbt_synapse_version()
description = "The Firebolt adapter plugin for dbt (data build tool)"
dbt_version = '0.21.0'

# the package version should be the dbt version, with maybe some things on the
# ends of it. (0.18.1 vs 0.18.1a1, 0.18.1.1, ...)
if not package_version.startswith(dbt_version):
    raise ValueError(
        f'Invalid setup.py: package_version={package_version} must start with '
        f'dbt_version={dbt_version}'
    )

setup(
    name=package_name,
    version=package_version,
    description=description,
    long_description=description,
    long_description_content_type="text/markdown",
    author="Shahar Shalev, Eric Ford, and Anders Swanson",
    author_email="eric@firebolt.io",
    url="",  # url of dbt adapter installation
    packages=find_namespace_packages(include=['dbt', 'dbt.*']),
    include_package_data=True,
    install_requires=[
        "dbt-core~={}".format(dbt_version),
        "jaydebeapi",
    ],
)
