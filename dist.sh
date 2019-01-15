#!/bin/bash
rm -Rf dist/*
pushd docs
make html
popd
python3 setup.py sdist bdist_wheel
python3 -m twine upload dist/*
