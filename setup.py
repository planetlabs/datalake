from setuptools import setup, find_packages
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
    version, version_info = pyver.get_version(pkg="datalake-common",
                                              public=True)
    return version

def get_version():
    if os.path.exists("PKG-INFO"):
        return get_version_from_pkg_info()
    else:
        return get_version_from_pyver()

setup(name='datalake-common',
      url='https://github.com/planetlabs/datalake-common',
      version=get_version(),
      description='common datalake parts',
      author='Brian Cavagnolo',
      author_email='brian@planet.com',
      packages=find_packages(),
      install_requires=[
          'python-dateutil>=2.4.2',
          'pytz>=2015.4',
          'pyver>=1.0.18',
          'simplejson>=3.3.1',
      ],
      extras_require={
          'test': [
              'pytest==2.7.2',
              'pip==7.1.0',
              'wheel==0.24.0',
          ],
          's3': [
              'boto>=2.38.0',
          ],
          'test-s3': [
              'moto==0.4.12',
          ]
      }
     )
