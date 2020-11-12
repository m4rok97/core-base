#!/bin/bash

cp -R ${IGNIS_HOME}/core/cpp/lib/* ${IGNIS_HOME}/lib
rm -fR ${IGNIS_HOME}/core/cpp/lib
rm -fR ${IGNIS_HOME}/core/cpp/include
ldconfig
