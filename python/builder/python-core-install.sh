#!/bin/bash

export DEBIAN_FRONTEND=noninteractive
apt update
apt -y install python3.8 python3.8-distutils openssl

python3 ${IGNIS_HOME}/bin/get-pip.py
rm -f ${IGNIS_HOME}/bin/get-pip.py
python3 -m pip install certifi

cd ${IGNIS_HOME}/core/python-libs/
cd mpi4py
python3 setup.py install
cd ..
cd thrift
python3 setup.py install
rm -fR ${IGNIS_HOME}/core/python-libs/

cd ${IGNIS_HOME}/core/python/
python3 setup.py develop
