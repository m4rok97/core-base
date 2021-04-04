
ARG REGISTRY=""
ARG TAG=""
FROM ${REGISTRY}ignishpc/builder${TAG}

RUN export DEBIAN_FRONTEND=noninteractive && \
	apt update && \
	apt -y install \
		rapidjson-dev \
		libevent-dev \
		zlib1g-dev  \
		libssl-dev \
		libtool 

ENV BOOST_VERSION=1.75.0
RUN mkdir /tmp/boost && \
	cd /tmp/boost && \
	wget https://dl.bintray.com/boostorg/release/${BOOST_VERSION}/source/boost_$(echo ${BOOST_VERSION} | tr . _).tar.gz -O src.tar.gz && \
	tar xvf src.tar.gz && \
	cd boost_$(echo ${BOOST_VERSION} | tr . _) && \
	./bootstrap.sh && \
	./b2 headers && \
	mv boost /usr/include/ && \
	rm -fR /tmp/boost

RUN mv /root/thrift /root/thrift-src && \
	cd /root/thrift-src && \
	./bootstrap.sh && \
	./configure --enable-tests=no --prefix /root/thrift && \
	cd lib/cpp && \
	make -j && \
	make install && \
	rm -fR /root/thrift-src && \
	rm -f /root/thrift/lib/*.a

ENV SPDLOG_VERSION=1.8.5
RUN cd /tmp && \
	git clone https://github.com/gabime/spdlog --branch v${SPDLOG_VERSION} --single-branch spdlog && \
	cd spdlog && \
	cmake -DSPDLOG_BUILD_EXAMPLE=FALSE . && \
	make -j && \
	make install && \
	cd .. && \
	rm -fR spdlog

ENV GHC_FILESYSTEM_VERSION=1.5.4
RUN cd /usr/include && \
	mkdir ghc && \
	cd ghc && \
	wget https://github.com/gulrak/filesystem/releases/download/v${GHC_FILESYSTEM_VERSION}/filesystem.hpp

RUN cd ${IGNIS_HOME} && \
	mkdir -p core/cpp/include && \
	cp -R /root/thrift/include/* core/cpp/include && \
	cp -R /usr/include/rapidjson core/cpp/include/ && \
	cp -R /root/mpi/include/* core/cpp/include/ && \
	cp /usr/include/zlib.h core/cpp/include/ && \
	cp /usr/include/zconf.h core/cpp/include/ && \
	mkdir -p core/cpp/lib && \
	cp -P /usr/lib/x86_64-linux-gnu/libgomp.so* core/cpp/lib && \
	cp -R /root/thrift/lib/* core/cpp/lib && \
	cp -R /root/mpi/lib/* core/cpp/lib

COPY cpp-core-install.sh ${IGNIS_HOME}/bin
RUN chmod +x ${IGNIS_HOME}/bin/cpp-core-install.sh

ARG SOURCE_CACHE=1
RUN mkdir -p /tmp/core-cpp && \ 
	cd /tmp/core-cpp && \
	git clone https://ignis:MbduRCadGPXp3pJMvwK_@gitlab.com/ignis-hpc/core-cpp.git /tmp/core-cpp && \
	export CC=/root/mpi/bin/mpicc && \
	export CXX=/root/mpi/bin/mpic++ && \
	export LIBRARY_PATH=${IGNIS_HOME}/core/cpp/lib && \
	export CPATH=${IGNIS_HOME}/core/cpp/include && \
	cmake -DBUILD_TESTING=OFF -DCMAKE_INSTALL_PREFIX=${IGNIS_HOME}/core/cpp/ . && \
	make -j && \
	make install && \
	rm -fr /tmp/core-cpp && \
	cd ${IGNIS_HOME} && \
	mv core/cpp/bin/ignis-cpp bin/ && \
	chmod +x bin/ignis-cpp && \
	rm -fR core/cpp/bin

