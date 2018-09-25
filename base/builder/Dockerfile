
FROM ignisframework/base-skel

RUN \
	yum -y update && \
	yum groupinstall -y "Development tools" && \
	yum -y install \
		java-1.8.0-openjdk \
		java-1.8.0-openjdk-devel \
		which \
		wget \
		python-devel.x86_64 \
		libevent-devel \
		zlib-devel \
		openssl-devel


ENV GRADLE_VERSION=4.9
RUN \
	mkdir -p /tmp/gradle && \ 
	cd /tmp/gradle && \
	wget https://services.gradle.org/distributions/gradle-${GRADLE_VERSION}-bin.zip -O gradle.zip && \
	unzip gradle && \
	mkdir -p /root/gradle/build && \
	mv gradle-${GRADLE_VERSION} /root/gradle/build && \
	cd / && \
	rm -fr /tmp/gradle && \
	{ \
		echo '#!/bin/bash'; \
		echo 'cd "$(dirname "$0")"'; \
		echo 'cp -R build/* /opt/gradle'; \
		echo 'ln -s /opt/gradle/bin/gradle /usr/local/bin/gradle'; \
		echo 'rm -f /opt/gradle/install'; \
	} > /root/gradle/install && \
	chmod +x /root/gradle/install && \
	/root/gradle/install



ENV CMAKE_VERSION=3.12
ENV CMAKE_VERSION_REV=0
RUN \
	mkdir -p /tmp/cmake && \
	cd /tmp/cmake && \
	wget https://cmake.org/files/v${CMAKE_VERSION}/cmake-${CMAKE_VERSION}.${CMAKE_VERSION_REV}.tar.gz -O cmake.zip && \
	tar zxvf cmake.zip && \
	cd cmake-${CMAKE_VERSION}.${CMAKE_VERSION_REV} && \
	./bootstrap --prefix=/usr/local && \
	make -j$(nproc) && \
	mkdir -p /root/cmake/build && \
	make install DESTDIR=/root/cmake/build && \
	cd / && \
	rm -fr /tmp/cmake && \
	{ \
		echo '#!/bin/bash'; \
		echo 'cd "$(dirname "$0")"'; \
		echo 'cp -R build/* /'; \
	} > /root/cmake/install && \
	chmod +x /root/cmake/install && \
	/root/cmake/install



ENV BOOST_VERSION_RELEASE=1
ENV BOOST_VERSION_MAJOR=66
ENV BOOST_VERSION_MINOR=0
ENV BOOST_VERSION=${BOOST_VERSION_RELEASE}.${BOOST_VERSION_MAJOR}.${BOOST_VERSION_MINOR}
ENV BOOST_VERSION_=${BOOST_VERSION_RELEASE}_${BOOST_VERSION_MAJOR}_${BOOST_VERSION_MINOR}
RUN \
	mkdir -p /tmp/boost && \
	cd /tmp/boost && \
	wget https://dl.bintray.com/boostorg/release/${BOOST_VERSION}/source/boost_${BOOST_VERSION_}.tar.gz && \
	tar xvf boost_${BOOST_VERSION_}.tar.gz && \
	cd boost_${BOOST_VERSION_} && \
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
	} > /root/boost/install && \
	chmod +x /root/boost/install && \
	/root/boost/install



RUN \
	mkdir -p /tmp/thrift && \
	cd /tmp/thrift && \
	git clone --branch ${THRIFT_VERSION} https://github.com/apache/thrift src && \
	cd src && \
	./bootstrap.sh && \
	./configure --enable-tests=no && \
	cd lib/cpp && \
	make && \
	mkdir -p /root/thrift/build && \
	make install DESTDIR=/root/thrift/build && \
	{ \
		echo '#!/bin/bash'; \
		echo 'cd "$(dirname "$0")"'; \
		echo 'cp -R build/* /'; \
	} > /root/thrift/install && \
	chmod +x /root/thrift/install && \
	/root/thrift/install
	

