#!/bin/env bash

apt update
apt -y --no-install-recommends install \
  openjdk-${JDK_VERSION}-jre \
  tzdata \
  curl \
  openssl \
  ca-certificates \
  openssh-server \
  gettext \
  socat
rm -rf /var/lib/apt/lists/*
mkdir /run/sshd
