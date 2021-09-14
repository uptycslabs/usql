#!/usr/bin/env python
# -*- coding: utf-8 -*-

import ast
from io import open
import re
import sys
import subprocess
from setuptools import Command, setup, find_packages

_version_re = re.compile(r"__version__\s+=\s+(.*)")

with open("usql/__init__.py", "rb") as f:
    version = str(
        ast.literal_eval(_version_re.search(f.read().decode("utf-8")).group(1))
    )


def open_file(filename):
    """Open and read the file *filename*."""
    with open(filename) as f:
        return f.read()


readme = open_file("README.md")

install_requirements = [
    "click >= 4.1",
    "Pygments >= 1.6",
    "prompt_toolkit>=2.0.0",
    "sqlparse>=0.2.2",
    "configobj >= 5.0.5",
    "cli_helpers[styles] >= 1.0.1,<2",
    "PyJWT >= 0.5.2",
    "requests",
]


setup(
    name="usql",
    author="Vibhor Kumar",
    author_email="vibhor.aim@gmail.com",
    license="BSD",
    version=version,
    url="https://github.com/Uptycs/usql",
    packages=find_packages(),
    package_data={"usql": ["usqlrc", "AUTHORS"]},
    description="CLI for Uptycs platform with auto-completion and syntax "
    "highlighting.",
    long_description=readme,
    long_description_content_type="text/markdown",
    install_requires=install_requirements,
    # cmdclass={"test": test, "lint": lint},
    entry_points={
        "console_scripts": ["usql = usql.main:cli"],
        "distutils.commands": ["lint = tasks:lint", "test = tasks:test"],
    },
    classifiers=[
        "Intended Audience :: Developers",
        "License :: OSI Approved :: BSD License",
        "Operating System :: Unix",
        "Programming Language :: Python",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: SQL",
        "Topic :: Database",
        "Topic :: Database :: Front-Ends",
        "Topic :: Software Development",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
)
