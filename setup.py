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

from setuptools import setup, find_packages
from setuptools import distutils
import os
import versioneer


def get_version():
    if os.path.exists("PKG-INFO"):
        metadata = distutils.dist.DistributionMetadata("PKG-INFO")
        return metadata.version
    else:
        return versioneer.get_version()


setup(name='datalake-common',
      url='https://github.com/planetlabs/datalake-common',
      version=get_version(),
      cmdclass=versioneer.get_cmdclass(),
      description='common datalake parts',
      author='Brian Cavagnolo',
      author_email='brian@planet.com',
      packages=find_packages(),
      install_requires=[
          'python-dateutil>=2.4.2',
          'pytz>=2015.4',
          'simplejson>=3.3.1',
          'python-dotenv>=0.1.3',
          'six>=1.10.0'
      ],
      extras_require={
          'test': [
              'pytest==3.0.2',
              'pip==7.1.0',
              'wheel==0.24.0',
              'flake8==2.5.0',
          ],
          's3': [
              'boto>=2.38.0',
          ],
          'test-s3': [
              'moto==0.4.25',
          ]
      })
