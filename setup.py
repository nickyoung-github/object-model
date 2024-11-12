from dunamai import Version
from setuptools import setup

setup(version=Version.from_git().serialize(metadata=True))
