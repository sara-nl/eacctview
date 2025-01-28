#!/usr/bin/env python
from setuptools import setup, find_packages
from eacctview.info import PACKAGE_VERSION, PACKAGE_NAME, PACKAGE_HOMEPAGE

with open("README.md","r") as f:
    long_descrition = f.read()


setup(
    name=PACKAGE_NAME,
    version=PACKAGE_VERSION,
    description='Terminal plotting tool for energy aware runtime (EAR)',
    author='Benjamin Czaja',
    author_email='benjamin.czaja@surf.nl',
    url=PACKAGE_HOMEPAGE,
    install_requires=[
        'numpy',
        'plotext',
    ],
    packages=[PACKAGE_NAME],
    py_modules=[PACKAGE_NAME],
    include_package_data=True,
    license='MIT',
    long_description=long_descrition,
    long_description_content_type="text/markdown",
    entry_points={
        'console_scripts': [
            '{} = {}.{}:main'.format(PACKAGE_NAME, PACKAGE_NAME, PACKAGE_NAME),
        ],
    },
)