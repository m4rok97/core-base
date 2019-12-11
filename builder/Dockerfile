
FROM ignishpc/base

RUN apt update && \
	apt -y install \
		build-essential \
		git \
		wget \
		cmake \
		automake \
		unzip

ENV BOOST_VERSION=1.71.0
RUN mkdir /tmp/boost && \
	cd /tmp/boost && \
	wget https://dl.bintray.com/boostorg/release/${BOOST_VERSION}/source/boost_$(echo ${BOOST_VERSION} | tr . _).tar.gz -O src.tar.gz && \
	tar xvf src.tar.gz && \
	cd boost_$(echo ${BOOST_VERSION} | tr . _) && \
	./bootstrap.sh && \
	./b2 -j 4&& \
	mkdir -p /root/boost/include/ && \
	mv boost /root/boost/include/ && \
	mv stage/lib /root/boost/ && \
	rm -fR /tmp/boost

COPY mpich_static_port.c /tmp/mpich_static_port.c
ENV MPICH_VERSION=3.3.2
RUN mkdir /tmp/mpi && \
	cd /tmp/mpi && \
	wget https://www.mpich.org/static/downloads/${MPICH_VERSION}/mpich-${MPICH_VERSION}.tar.gz && \
	tar -zxf mpich-${MPICH_VERSION}.tar.gz && \
	cd mpich-${MPICH_VERSION} && \
	sed -e '/.*MPL_listen_anyport.*/ {' -e 'r /tmp/mpich_static_port.c' -e 'd' -e '}' -i $(find . -name mpl_sockaddr.c) && \
	rm -f /tmp/mpich_static_port.c && \
	./configure --disable-fortran --prefix /root/mpi && \
	make -j 4 && \
	make install && \
	rm -fR /tmp/mpi

ENV THRIFT_VERSION=0.13.0
RUN git clone --branch ${THRIFT_VERSION} https://github.com/apache/thrift /root/thrift
