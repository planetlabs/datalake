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


def get_version_from_pkg_info():
    metadata = distutils.dist.DistributionMetadata("PKG-INFO")
    return metadata.version


def get_version_from_pyver():
    try:
        import pyver
    except ImportError:
        if 'sdist' in sys.argv or 'bdist_wheel' in sys.argv:
            raise ImportError('You must install pyver to create a package')
        else:
            return 'noversion'
    version, version_info = pyver.get_version(pkg="datalake_api",
                                              public=True)
    return version


def get_version():
    if os.path.exists("PKG-INFO"):
        return get_version_from_pkg_info()
    else:
        return get_version_from_pyver()


setup(name='datalake_api',
      url='https://github.com/planetlabs/datalake-api',
      version=get_version(),
      description='datalake_api ingests datalake metadata records',
      author='Brian Cavagnolo',
      author_email='brian@planet.com',
      packages=['datalake_api'],
      install_requires=[
          'pyver>=1.0.18',
          'memoized_property>=1.0.2',
          'simplejson>=3.3.1',
          'datalake-common>=0.14',
          'Flask>=0.10.1',
          'flask-swagger==0.2.8',
          'boto3==1.1.3',
          'raven[flask]>=5.6.0',
          'blinker>=1.4',
      ],
      extras_require={
          'test': [
              'pytest==2.7.2',
              'flake8==2.5.0',
              'moto==0.4.23',
          ],
      },
      include_package_data=True)
