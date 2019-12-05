
FROM ubuntu:18.04 as builder

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
	./configure --prefix /root/mesos && \
	make -j 4 && \
	make install && \
	rm -fR /tmp/mesos

ENV _CHRONOS_VERSION=2.5.0
RUN mkdir /tmp/chronos/ && \
	cd /tmp/chronos/ && \
	wget https://github.com/mesos/chronos/archive/${_CHRONOS_VERSION}.zip  && \
	unzip ${_CHRONOS_VERSION}.zip && \
	cd chronos-${_CHRONOS_VERSION} && \
	mvn package -DskipTests && \
	mkdir /opt/chronos && \
	mv target /root/chronos && \
	rm -fR /tmp/chronos/

FROM ubuntu:18.04

RUN apt update && \
	apt install -y \
		openjdk-8-jre \
		nodejs \
		libcurl4-nss-dev \ 
		libsasl2-dev \
		libsasl2-modules \ 
		libapr1-dev \ 
		libsvn-dev \
		libghc-zlib-dev

ENV JAVA_HOME=/usr/lib/jvm/java-8-openjre-amd64
ENV MESOS_NATIVE_JAVA_LIBRARY=/usr/local/lib/libmesos.so

COPY --from=builder /root/mesos /usr/local
COPY --from=builder /root/chronos /opt/chronos

RUN { \
		echo '#!/bin/bash'; \
		echo 'if [ ! -z "$MESOS_QUORUM" ];then'; \
		echo '  mesos master --work_dir=/var/lib/mesos/master --port=${PORT_MASTER} --zk=${ZOOKEEPER}/mesos &'; \
		echo 'fi'; \
		echo 'mesos agent --work_dir=/var/lib/mesos/agent --port=${PORT_AGENT} --no-switch_user \'; \
		echo '   --master=${ZOOKEEPER}/mesos --containerizers=docker,mesos &'; \
		echo 'java -cp /opt/chronos/chronos*.jar org.apache.mesos.chronos.scheduler.Main --master ${ZOOKEEPER}/mesos \
		--zk_hosts ${ZOOKEEPER} --hostname ${MESOS_ADVERSTISE_IP} --http_port ${PORT_CHRONOS} &'; \
		echo 'wait'; \
	} > /bin/start-mesos.sh  && chmod +x /bin/start-mesos.sh && \
	ldconfig

