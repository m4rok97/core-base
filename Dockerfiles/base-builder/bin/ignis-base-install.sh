#!/bin/env bash
set -e

apt update
apt -y --no-install-recommends install \
  openjdk-${JDK_VERSION}-jre \
  tzdata \
  curl \
  openssl \
  ca-certificates \
  openssh-server \
  gettext \
  socat \
  libnss-wrapper
rm -rf /var/lib/apt/lists/*
mkdir /run/sshd
