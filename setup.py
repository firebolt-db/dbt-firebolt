#!/usr/bin/env python
from setuptools import setup
from setuptools.config import read_configuration

config = read_configuration('setup.cfg')
version = config['metadata']['version']
install_requires = config['options']['install_requires']

setup(install_requires=install_requires)
