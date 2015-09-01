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
    version, version_info = pyver.get_version(pkg="datalake_backend", public=True)
    return version

def get_version():
    if os.path.exists("PKG-INFO"):
        return get_version_from_pkg_info()
    else:
        return get_version_from_pyver()

setup(name='datalake_backend',
      url='https://github.com/planetlabs/datalake-backend',
      version=get_version(),
      description='datalake_backend ingests datalake metadata records',
      author='Brian Cavagnolo',
      author_email='brian@planet.com',
      packages=['datalake_backend'],
      install_requires=[
          'pyver>=1.0.18',
          'boto>=2.38.0',
          'configargparse>=0.9.3',
          'memoized_property>=1.0.2',
          'simplejson>=3.3.1',
          'datalake-common>=0.3',
      ],
      extras_require={
          'test': [
              'pytest==2.7.2',
              'pip==7.1.0',
              'wheel==0.24.0',
              'moto==0.4.12',
          ]
      },
     )
