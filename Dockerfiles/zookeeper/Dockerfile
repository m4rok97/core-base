
FROM ubuntu:20.04
ARG RELPATH=""

RUN export DEBIAN_FRONTEND=noninteractive && \
	apt update && \
	apt install -y \
		tar \
		wget \
		openjdk-8-jdk 


ENV ZK_VERSION=3.5.5
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
ENV ZOOMAIN=org.apache.zookeeper.server.quorum.QuorumPeerMain
ENV ZOOCFGDIR=/etc/zookeeper/conf
ENV ZOO_LOG_DIR=/var/log/zookeeper
ENV ZOO_LOG4J_PROP=INFO,ROLLINGFILE
ENV JMXLOCALONLY=true

RUN cd /tmp && \
	wget https://archive.apache.org/dist/zookeeper/zookeeper-${ZK_VERSION}/apache-zookeeper-${ZK_VERSION}-bin.tar.gz && \
	tar -zxf apache-zookeeper-${ZK_VERSION}-bin.tar.gz && \
	rm -f apache-zookeeper-${ZK_VERSION}-bin.tar.gz && \
	mv apache-zookeeper-${ZK_VERSION}-bin /opt/zookeeper

RUN { \
		echo '#!/bin/bash'; \
		echo 'read port'; \
		echo 'read auth'; \
		echo 'read cmd'; \
		echo ''; \
		echo '/opt/zookeeper/bin/zkCli.sh -server localhost:${port} <<EOF'; \
		echo '$(echo -e ${auth})'; \
		echo '$(echo -e ${cmd})'; \
		echo 'quit'; \
		echo 'EOF'; \
	} > /opt/zookeeper/bin/auth-cli.sh  && chmod +x /opt/zookeeper/bin/auth-cli.sh
