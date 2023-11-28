ARG REGISTRY=""
ARG NAMESPACE="ignishpc/"
ARG TAG=""
FROM ubuntu:20.04
ARG BUILDARCH

LABEL ignis.build="optional"

ENV VERSION=4.0.2
ENV GO_VERSION=1.21.4

RUN export DEBIAN_FRONTEND=noninteractive && \
    apt update && \
    apt install -y \
      build-essential \
      autoconf \
      automake \
      libfuse-dev \
      libglib2.0-dev \
      libseccomp-dev \
      libtool \
      pkg-config \
      wget \
      squashfs-tools \
      tar && \
    wget https://dl.google.com/go/go${GO_VERSION}.linux-${BUILDARCH}.tar.gz && \
    tar -C /tmp -xzvf go${GO_VERSION}.linux-${BUILDARCH}.tar.gz && \
    rm go${GO_VERSION}.linux-${BUILDARCH}.tar.gz && \
    export PATH=/tmp/go/bin:${PATH} && \
    wget https://github.com/sylabs/singularity/releases/download/v${VERSION}/singularity-ce-${VERSION}.tar.gz && \
    tar -xzf singularity-ce-${VERSION}.tar.gz && \
    rm singularity-ce-${VERSION}.tar.gz && \
    cd singularity-ce-${VERSION} && \
    ./mconfig && \
    make -C builddir && \
    make -C builddir install && \
    cd .. && \
    apt purge -y \
      build-essential \
      autoconf \
      automake \
      libfuse-dev \
      libglib2.0-dev \
      libseccomp-dev \
      libtool \
      pkg-config \
      wget && \
    apt autoremove -y && \
    rm -rf \
      /var/lib/apt/lists/* \
      singularity-ce-${VERSION} \
      /tmp/go \
      /root/go \
      /root/.cache/*
