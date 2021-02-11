#!/usr/bin/env python

from setuptools import find_packages, setup

def get_version():
    version = {}
    with open('tap_quickbooks_report/version.py') as fp:
        exec(fp.read(), version)
    return version['__version__']


with open('README.md', 'r') as f:
    readme = f.read()

setup(name='tap-quickbooks',
      version='1.4.29',
      description='Singer.io tap for extracting data from the Quickbooks API',
      author='HotGlue',
      url='http://hotglue.xyz/',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_quickbooks'],
      install_requires=[
          'requests==2.20.0',
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
          'tap_quickbooks.quickbooks': ['schemas/*.json', 'reportstreams/*']
      },
      include_package_data=True,
)
