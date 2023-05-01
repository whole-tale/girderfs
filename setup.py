#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
from setuptools import setup

import girderfs


if sys.argv[-1] == "publish":
    os.system("python setup.py sdist upload")
    sys.exit()


with open(os.path.join(os.path.dirname(__file__), "README.rst")) as f:
    readme = f.read()

classifiers = [
    "Development Status :: 3 - Alpha",
    "Environment :: Console",
    "Intended Audience :: Developers",
    "License :: OSI Approved :: BSD License",
    "Operating System :: OS Independent",
    "Programming Language :: Python",
    "Programming Language :: Python :: 3.9",
    "Programming Language :: Python :: 3.10",
    "Topic :: System :: Filesystems",
]

setup(
    name="girderfs",
    version=girderfs.__version__,
    description="FUSE filesystem allowing to mount Girder's fs assetstore",
    long_description=readme,
    packages=["girderfs"],
    entry_points={
        "console_scripts": [
            "girderfs = girderfs.__main__:main",
            "girderfs-server = girderfs.server:main",
        ]
    },
    install_requires=[
        "fusepy",
        "girder-client",
        "requests",
        "python-dateutil",
        "diskcache",
    ],
    tests_require=["pytest"],
    author=girderfs.__author__,
    author_email="xarthisius.kk@gmail.com",
    url="https://github.com/whole-tale/girderfs",
    license="BSD",
    classifiers=classifiers,
)
