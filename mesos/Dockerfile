
FROM ubuntu:18.04

RUN apt update && \
	apt install -y \
		tar \
		wget \
		git  \
		openjdk-8-jdk \
		autoconf \
		libtool \
		build-essential \
		python-dev \
		python-boto \
		libcurl4-nss-dev \ 
		libsasl2-dev \
		libsasl2-modules \ 
		maven \
		libapr1-dev \ 
		libsvn-dev \
		libghc-zlib-dev

ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
ENV _MESOS_VERSION=1.9.0
		
RUN mkdir /tmp/mesos && \
	cd /tmp/mesos && \
	wget www.apache.org/dist/mesos/${_MESOS_VERSION}/mesos-${_MESOS_VERSION}.tar.gz && \
	tar -zxf mesos-${_MESOS_VERSION}.tar.gz && \
	cd mesos-${_MESOS_VERSION} && \
	./configure && \
	make -j 4 && \
	make install && \
	ldconfig && \
	cd /tmp && \
	rm -fR /tmp/mesos

ENV _MARATHON_VERSION=1.5.1
ENV MESOS_NATIVE_JAVA_LIBRARY=/usr/local/lib/libmesos.so

RUN cd /tmp && \
	wget http://downloads.mesosphere.com/marathon/v${_MARATHON_VERSION}/marathon-${_MARATHON_VERSION}.tgz  && \
	tar -zxf marathon-${_MARATHON_VERSION}.tgz  && \
	rm -f marathon-${_MARATHON_VERSION}.tgz && \
	mv $(ls | grep marathon-${_MARATHON_VERSION}*) /opt/marathon

RUN { \
		echo '#!/bin/bash'; \
		echo 'if [ ! -z "$MESOS_QUORUM" ];then'; \
		echo '  mesos master --work_dir=/var/lib/mesos/master --port=${PORT_MASTER} --zk=${ZOOKEEPER}/mesos &'; \
		echo 'fi'; \
		echo 'mesos agent --work_dir=/var/lib/mesos/agent --port=${PORT_AGENT} --no-switch_user \'; \
		echo '   --master=${ZOOKEEPER}/mesos --containerizers=docker,mesos &'; \
		echo '/opt/marathon/bin/marathon --master ${ZOOKEEPER}/mesos --zk ${ZOOKEEPER}/marathon --http_port ${PORT_MARATHON} &'; \
		echo 'wait'; \
	} > /bin/start-mesos.sh  && chmod +x /bin/start-mesos.sh

