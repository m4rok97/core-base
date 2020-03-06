#!/bin/bash

apt update
apt -y install python3.8 python3.8-distutils python3.8-dev
rm -f /usr/bin/python3
ln -s $(which python3.8) /usr/bin/python3

python3 ${IGNIS_HOME}/bin/get-pip.py
rm -f ${IGNIS_HOME}/bin/get-pip.py

cd ${IGNIS_HOME}/core/python/
python3 setup.py develop
