#!/usr/bin/env python
from setuptools import setup
from setuptools.config import read_configuration

config = read_configuration('setup.cfg')
version = config['metadata']['version']
dbt_version = version.rsplit('.', 1)[0]
install_requires = [f'dbt-core~={dbt_version}', config['options']['install_requires']]

setup(install_requires=install_requires)
