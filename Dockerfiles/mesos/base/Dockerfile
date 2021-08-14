
ARG REGISTRY=""
ARG TAG=""
FROM ubuntu:18.04 as builder
ARG RELPATH=""

RUN export DEBIAN_FRONTEND=noninteractive && \
	apt update && \
	apt install -y \
		build-essential \
		tar wget git zip \
		openjdk-8-jdk maven nodejs \
		autoconf \
		libtool \
		python-dev \
		python-six \
		python3-virtualenv \
		libcurl4-nss-dev \
		libsasl2-dev \
		libsasl2-modules \
		libapr1-dev \
		libsvn-dev \
		zlib1g-dev \
		iputils-ping
		
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
ENV _MESOS_VERSION=1.11.0

RUN mkdir /tmp/mesos && \
	mkdir /tmp/root && \
	cd /tmp/mesos && \
	wget www.apache.org/dist/mesos/${_MESOS_VERSION}/mesos-${_MESOS_VERSION}.tar.gz && \
	tar -zxf mesos-${_MESOS_VERSION}.tar.gz && \
	cd mesos-${_MESOS_VERSION} && \
	./configure && \
	make -j && \
	make install DESTDIR=/tmp/root && \
	rm -fR /tmp/mesos

FROM ubuntu:20.04

RUN export DEBIAN_FRONTEND=noninteractive && \
	apt update && \
	apt install -y --no-install-recommends \
		tar wget git zip \
		openjdk-13-jre-headless \
		python3 \
		libcurl4-nss-dev \
		libevent-dev \
		libsvn1 \
		libltdl7 \
		libsasl2-modules

ENV JAVA_HOME=/usr/lib/jvm/java-13-openjdk-amd64/
ENV MESOS_NATIVE_JAVA_LIBRARY=/usr/local/lib/libmesos.so
ENV MESOS_CGROUPS_ENABLE_CFS=false

COPY --from=builder /tmp/root /

RUN ldconfig && { \
		echo '#!/bin/bash'; \
		echo 'if [ ! -z "$MESOS_QUORUM" ];then'; \
		echo '  mesos master --work_dir=${MESOS_WD}/master --port=${PORT_MASTER} --zk=${ZOOKEEPER}/mesos &'; \
		echo 'fi'; \
		echo 'if [ ! -z "$PORT_AGENT" ];then'; \
		echo 'mesos agent --work_dir=${MESOS_WD}/agent --port=${PORT_AGENT} --no-switch_user \'; \
		echo '   --master=${ZOOKEEPER}/mesos --containerizers=docker,mesos --isolation=cgroups/all'; \
		echo 'fi'; \
	} > /bin/start-mesos.sh  && chmod +x /bin/start-mesos.sh

