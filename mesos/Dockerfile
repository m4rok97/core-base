
FROM ubuntu:18.04

RUN apt update && \
	apt install -y \
		tar \
		wget \
		git  \
		zip \
		build-essential \
		openjdk-8-jdk \
		autoconf \
		libtool \
		python-dev \
		python-boto \
		libcurl4-nss-dev \ 
		libsasl2-dev \
		libsasl2-modules \ 
		libapr1-dev \ 
		libsvn-dev \
		libghc-zlib-dev && \
	apt install -y \
		maven \
		nodejs 


ENV _MESOS_VERSION=1.9.0
RUN mkdir /tmp/mesos && \
	cd /tmp/mesos && \
	wget www.apache.org/dist/mesos/${_MESOS_VERSION}/mesos-${_MESOS_VERSION}.tar.gz && \
	tar -zxf mesos-${_MESOS_VERSION}.tar.gz && \
	cd mesos-${_MESOS_VERSION} && \
	./configure && \
	make -j 4 && \
	make install && \
	rm -fR /tmp/mesos

ENV _MARATHON_VERSION=1.8.222-86475ddac
RUN cd /tmp && \
	wget https://downloads.mesosphere.com/marathon/builds/${_MARATHON_VERSION}/marathon-${_MARATHON_VERSION}.tgz  && \
	tar -zxf marathon-${_MARATHON_VERSION}.tgz  && \
	rm -f marathon-${_MARATHON_VERSION}.tgz && \
	mv $(ls | grep marathon-${_MARATHON_VERSION}*) /opt/marathon

RUN rm -fR ~/.m2 && \
		apt purge -y \
		build-essential \
		openjdk-8-jdk && \
	apt autoremove -y && \
	apt install -y \
		openjdk-8-jre 

ENV JAVA_HOME=/usr/lib/jvm/java-8-openjre-amd64
ENV MESOS_NATIVE_JAVA_LIBRARY=/usr/local/lib/libmesos.so


RUN { \
		echo '#!/bin/bash'; \
		echo 'if [ ! -z "$MESOS_QUORUM" ];then'; \
		echo '  mesos master --work_dir=/var/lib/mesos/master --port=${PORT_MASTER} --zk=${ZOOKEEPER}/mesos &'; \
		echo 'fi'; \
		echo 'mesos agent --work_dir=/var/lib/mesos/agent --port=${PORT_AGENT} --no-switch_user \'; \
		echo '   --master=${ZOOKEEPER}/mesos --containerizers=docker,mesos &'; \
		echo '/opt/marathon/bin/marathon --master ${ZOOKEEPER}/mesos --zk ${ZOOKEEPER}/marathon --http_port ${PORT_MARATHON} &'; \
		echo 'wait'; \
	} > /bin/start-mesos.sh  && chmod +x /bin/start-mesos.sh && \
	ldconfig

