#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Setup Script."""

from setuptools import setup, find_packages
from pathlib import Path


def _get_requirements():
    with open('requirements.txt') as _file:
        return _file.readlines()


def _get_long_description():
    cur_dir = Path(__file__).absolute().parent
    readme_file = cur_dir.joinpath('README').with_suffix('.md')
    with open(readme_file) as _file:
        return _file.read()


setup(
    name="ml-utils",
    version='0.1',
    description="ML utility library for fabric8-analytics",
    long_description=_get_long_description(),
    author='Ravindra Ratnawat',
    author_email="ravindra@redhat.com",
    license='APACHE 2.0',
    url='https://github.com/fabric8-analytics/fabric8-analytics-ML-utils',
    keywords=['Fabric8-Analytics', 'Machine-Learning', 'Utility'],
    py_modules=['ml_utils'],
    python_requires='>=3.4',
    packages=find_packages(),
    install_requires=_get_requirements(),
)
