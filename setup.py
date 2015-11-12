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
    version, version_info = pyver.get_version(pkg="datalake", public=True)
    return version


def get_version():
    if os.path.exists("PKG-INFO"):
        return get_version_from_pkg_info()
    else:
        return get_version_from_pyver()


setup(name='datalake',
      url='https://github.com/planetlabs/datalake',
      version=get_version(),
      description='datalake: a metadata-aware archive',
      author='Brian Cavagnolo',
      author_email='brian@planet.com',
      packages=find_packages(exclude=['test']),
      install_requires=[
          'pyver>=1.0.18',
          'boto>=2.38.0',
          'memoized_property>=1.0.1',
          'simplejson>=3.7',
          'pyblake2>=0.9.3',
          'click>=4.1',
          'datalake-common>=0.4',
          'python-dotenv>=0.1.3',
      ],
      extras_require={
          'test': [
              'pytest==2.7.2',
              'moto==0.4.2',
              'twine==1.5.0',
              'pip==7.1.0',
              'wheel==0.24.0',
              'flake8==2.5.0',
          ],
          # the queuable feature allows users to offload their datalake pushes
          # to a separate uploader process.
          'queuable': [
              'pyinotify>=0.9.4',
              'xattr>=0.7.8',
          ]
      },
      entry_points="""
      [console_scripts]
      datalake=datalake.scripts.cli:cli
      """)
