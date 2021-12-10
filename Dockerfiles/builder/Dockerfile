
ARG REGISTRY=""
ARG NAMESPACE="ignishpc"
ARG TAG=""
FROM ${REGISTRY}${NAMESPACE}base${TAG}
ARG RELPATH=""

RUN rm -fR /usr2 && \
	mkdir /usr2 && \
	export DEBIAN_FRONTEND=noninteractive && \
	apt update && \
	apt -y install \
		build-essential \
		gfortran \
		git \
		wget \
		cmake \
		automake \
		unzip \
		pkg-config && \
	rm -rf /var/lib/apt/lists/*

ENV THRIFT_VERSION=0.15.0
RUN git clone --branch ${THRIFT_VERSION} https://github.com/apache/thrift $IGNIS_DEVEL/thrift
