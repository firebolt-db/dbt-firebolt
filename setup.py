#!/usr/bin/env python
from setuptools.config import read_configuration

config = read_configuration('setup.cfg')
version = config['metadata']['version']
install_requires = [config['options']['install_requires']]

dbt_version = version.rsplit('.', 1)[0]
install_requires = [f'dbt-core~={dbt_version}', config['options']['install_requires']]
