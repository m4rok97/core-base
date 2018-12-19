
FROM ignisframework/base-skel

RUN apt update && \
	apt -y install \
		build-essential \
		openjdk-8-jdk \
		git \
		wget \
		cmake \
		automake \
		unzip \
		libevent-dev \
		zlib1g-dev  \
		libssl-dev \
		libtool \
		pkg-config

ENV BOOST_VERSION=1.66.0
RUN \
	mkdir -p /tmp/boost && \
	cd /tmp/boost && \
	wget https://dl.bintray.com/boostorg/release/${BOOST_VERSION}/source/boost_$(echo ${BOOST_VERSION} | tr . _).tar.gz -O src.tar.gz && \
	tar xvf src.tar.gz && \
	cd boost_$(echo ${BOOST_VERSION} | tr . _) && \
	./bootstrap.sh && \
	./b2 && \
	mkdir -p /root/boost/build/usr/include/ && \
	mv boost /root/boost/build/usr/include/ && \
	mkdir -p /root/boost/build/usr/lib/ && \
	mv stage/lib /root/boost/build/usr/ && \
	cd / && \
	rm -fr /tmp/boost && \
	{ \
		echo '#!/bin/bash'; \
		echo 'cd "$(dirname "$0")"'; \
		echo 'cp -R build/* /'; \
		echo 'ldconfig'; \
	} > /root/boost/install && \
	chmod +x /root/boost/install && \
	/root/boost/install
	
ENV THRIFT_VERSION=0.11.0
RUN \
	mkdir -p /tmp/thrift && \
	cd /tmp/thrift && \
	git clone --branch ${THRIFT_VERSION} https://github.com/apache/thrift /tmp/thrift && \
	./bootstrap.sh && \
	./configure --enable-tests=no --prefix /usr && \
	cd lib/cpp && \
	make && \
	mkdir -p /root/thrift/build && \
	make install DESTDIR=/root/thrift/build && \
	{ \
		echo '#!/bin/bash'; \
		echo 'cd "$(dirname "$0")"'; \
		echo 'cp -R build/* /'; \
		echo 'ldconfig'; \
	} > /root/thrift/install && \
	chmod +x /root/thrift/install && \
	/root/thrift/install

ENV GRADLE_VERSION=5.0
RUN \
	mkdir -p /tmp/gradle && \ 
	cd /tmp/gradle && \
	wget https://services.gradle.org/distributions/gradle-${GRADLE_VERSION}-bin.zip -O src.zip && \
	unzip src && \
	mv gradle-${GRADLE_VERSION} /opt/gradle && \
	ln -s /opt/gradle/bin/gradle /usr/local/bin/gradle && \
	cd / && \
	rm -fr /tmp/gradle




	

