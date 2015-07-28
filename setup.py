from setuptools import setup
from setuptools import distutils
from pip.req import parse_requirements
from pip.download import PipSession
import os

def get_requirements():
    path = os.path.join(os.path.dirname(__file__), 'requirements.txt')
    reqs = []
    if os.path.exists(path):
        reqs = parse_requirements(path, session=PipSession())
        reqs = [str(r.req) for r in reqs]
    return reqs

def get_version():
    if os.path.exists("PKG-INFO"):
        metadata = distutils.dist.DistributionMetadata("PKG-INFO")
        return metadata.version
    else:
        import pyver
        version, version_info = pyver.get_version(pkg="datalake", public=True)
        return version

setup(name='datalake',
      url='https://github.com/planetlabs/datalake',
      version=get_version(),
      description='datalake: a metadata-aware archive',
      author='Brian Cavagnolo',
      author_email='brian@planet.com',
      packages=['datalake'],
      install_requires=get_requirements(),
      scripts=['bin/datalake'],
     )
