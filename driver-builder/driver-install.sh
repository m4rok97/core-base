#!/bin/bash

ldconfig
export DEBIAN_FRONTEND=noninteractive
apt update 
apt -y install openjdk-14-jre-headless openssl
