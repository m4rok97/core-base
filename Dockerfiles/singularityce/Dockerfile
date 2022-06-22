
ARG REGISTRY=""
ARG NAMESPACE="ignishpc/"
ARG TAG=""
FROM ubuntu:20.04
ARG RELPATH=""

ENV SIN_VERSION=3.10.0 

RUN export DEBIAN_FRONTEND=noninteractive && \
	apt update && \
    apt install -y \
	   build-essential \
	   libseccomp-dev \
	   libglib2.0-dev \
	   pkg-config \
	   squashfs-tools \
	   cryptsetup \
	   runc \
	   wget

		
ENV GO_VERSION=go1.17.11
RUN cd /tmp && \
	(wget https://go.dev/dl/${GO_VERSION}.linux-$(dpkg --print-architecture).tar.gz -O golang.tar.gz || \ 
	wget https://go.dev/dl/${GO_VERSION}.linux-$(uname -m).tar.gz -O golang.tar.gz) && \
	tar -xvzf  golang.tar.gz && \
	rm -fR golang.tar.gz && \
	export PATH=/tmp/go/bin:$PATH && \
	wget https://github.com/sylabs/singularity/releases/download/v${SIN_VERSION}/singularity-ce-${SIN_VERSION}.tar.gz && \
    tar -xzf singularity-ce-${SIN_VERSION}.tar.gz && \
    cd singularity-ce-${SIN_VERSION} && \
	./mconfig && \
    make -C builddir && \
    make -C builddir install && \
	cd .. && \
	rm -fR singularity&& \
	rm -fR go
