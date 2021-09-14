#!/bin/bash
rm -rf ./usql.egg-info ./dist ./build
python setup.py bdist_wheel
python -m twine upload dist/*
rm -rf ./usql.egg-info ./dist ./build
