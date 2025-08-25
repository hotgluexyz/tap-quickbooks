#!/usr/bin/env python

from setuptools import find_packages, setup

def get_version():
    return '1.4.54'


with open('README.md', 'r') as f:
    readme = f.read()

setup(name='tap-quickbooks',
      version=get_version(),
      description='Singer.io tap for extracting data from the Quickbooks API',
      author='hotglue',
      url='http://hotglue.xyz/',
      classifiers=['Programming Language :: Python :: 3 :: Only'],

      install_requires=[
          'requests>=2.20.0',
          'singer-python==5.3.1',
          'xmltodict==0.11.0',
          'jsonpath-ng==1.4.3',
          'pytz==2018.4',
          'attrs==20.2.0'
      ],
      entry_points='''
          [console_scripts]
          tap-quickbooks=tap_quickbooks:main
      ''',
      packages=find_packages(exclude=['tests']),
      package_data={
          'tap_quickbooks.quickbooks': ['schemas/*.json'],
          'tap_quickbooks.quickbooks.reportstreams': ['*.py'],
          'tap_quickbooks.quickbooks.reportstreams.english_schemas': ['*.py']
      },
      include_package_data=True,
)
