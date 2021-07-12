# -*- coding: utf-8 -*-

from setuptools import setup

with open('README.md') as f:
    readme = f.read()

with open('LICENSE') as f:
    license = f.read()

setup(
    name='feast-hive',
    version='0.1',
    author='Benn Ma',
    author_email='bennmsg@gmail.com',
    description='Hive support for Feast offline store',
    long_description=readme,
    long_description_content_type="text/markdown",
    python_requires=">=3.7.0",
    url='https://github.com/baineng/feast-hive',
    license=license,
    packages=['feast_hive'],
    install_requires=[
        "feast>=0.11.0",
        "impyla==0.17.0",
    ],
)
