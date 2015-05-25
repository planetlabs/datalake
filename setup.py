from setuptools import setup, find_packages
from setuptools import distutils
import subprocess
import sys
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
        version, version_info = pyver.get_version(pkg="atl", public=True)
        return version

setup(name='allthelogs',
      url='https://github.com/planetlabs/atl',
      version=get_version(),
      description='atl: "all the logs" (or "archive the log")',
      author='Brian Cavagnolo',
      author_email='brian.cavagnolo@planet.com',
      packages=['atl'],
      install_requires=get_requirements(),
     )
