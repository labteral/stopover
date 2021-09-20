#!/bin/bash
cd $(dirname $0)/../src
python3 setup.py bdist_wheel
twine upload dist/*.whl
