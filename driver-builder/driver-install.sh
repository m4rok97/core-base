#!/bin/bash

echo ${IGNIS_HOME}/lib/native > /etc/ld.so.conf.d/ignis-lib.conf
ldconfig
apt update 
apt -y install openjdk-8-jdk openssl
