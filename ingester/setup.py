# Copyright 2015 Planet Labs, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

from setuptools import setup
from setuptools import distutils
import os
import sys
import versioneer


def get_version_from_pkg_info():
    metadata = distutils.dist.DistributionMetadata("PKG-INFO")
    return metadata.version


def get_version():
    if os.path.exists("PKG-INFO"):
        return get_version_from_pkg_info()
    else:
        return versioneer.get_version()


setup(name='datalake_ingester',
      url='https://github.com/planetlabs/datalake-ingester',
      version=get_version(),
      description='datalake_ingester ingests datalake metadata records',
      author='Brian Cavagnolo',
      author_email='brian@planet.com',
      packages=['datalake_ingester'],
      install_requires=[
          'boto3>=1.20',
          'memoized_property>=1.0.2',
          'simplejson>=3.3.1',
          'sentry-sdk>=1.5.0',
          'click>=8.0',
      ],
      extras_require={
          'test': [
              'pytest',
              'pip',
              'wheel',
              'moto>=3.0.0',
              'flake8>=4.0.0',
              'freezegun>=1.1.0',
          ]
      },
      classifiers=[
          'Programming Language :: Python :: 3.7',
          'Programming Language :: Python :: 3.8',
          'Programming Language :: Python :: 3.9',
          'Programming Language :: Python :: 3.10',
      ],
      entry_points="""
      [console_scripts]
      datalake_tool=datalake_ingester.cli:cli
      """)
