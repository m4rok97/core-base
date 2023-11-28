
ARG REGISTRY=""
ARG NAMESPACE="ignishpc/"
ARG TAG=""
FROM ubuntu:22.04

LABEL ignis.build="optional"

ENV NOMAD_VERSION=1.6.3

RUN apt update && \
    apt install -y --no-install-recommends \
      curl \
      gnupg \
      ca-certificates && \
    curl -fsSL https://apt.releases.hashicorp.com/gpg | apt-key add - && \
    . /etc/lsb-release && \
    echo "deb [arch=amd64] https://apt.releases.hashicorp.com $DISTRIB_CODENAME main" >> /etc/apt/sources.list && \
    apt update && \
    apt install -y nomad=${NOMAD_VERSION} && \
    rm -rf /var/lib/apt/lists/*
