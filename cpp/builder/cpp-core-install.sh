#!/bin/bash

export DEBIAN_FRONTEND=noninteractive
apt update
apt -y install g++

cp -R ${IGNIS_HOME}/core/cpp/lib/* ${IGNIS_HOME}/lib
rm -fR ${IGNIS_HOME}/core/cpp/lib
rm -fR ${IGNIS_HOME}/core/cpp/include
ldconfig
