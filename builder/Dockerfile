
ARG REGISTRY=""
ARG TAG=""
FROM ${REGISTRY}ignishpc/base${TAG}

RUN export DEBIAN_FRONTEND=noninteractive && \
	apt update && \
	apt -y install \
		build-essential \
		gfortran \
		git \
		wget \
		cmake \
		automake \
		unzip \
		pkg-config 

COPY mpich_static_port.c /tmp/mpich_static_port.c
ENV MPICH_VERSION=3.3.2
RUN mkdir /tmp/mpi && \
	cd /tmp/mpi && \
	wget https://www.mpich.org/static/downloads/${MPICH_VERSION}/mpich-${MPICH_VERSION}.tar.gz && \
	tar -zxf mpich-${MPICH_VERSION}.tar.gz && \
	cd mpich-${MPICH_VERSION} && \
	sed -e '/.*MPL_listen_anyport.*/ {' -e 'r /tmp/mpich_static_port.c' -e 'd' -e '}' -i $(find . -name mpl_sockaddr.c) && \
	rm -f /tmp/mpich_static_port.c && \
	./configure --prefix /root/mpi && \
	make -j && \
	make install && \
	rm -f /root/mpi/lib/*.a && \
	rm -fR /tmp/mpi

ENV THRIFT_VERSION=0.13.0
RUN git clone --branch ${THRIFT_VERSION} https://github.com/apache/thrift /root/thrift
COPY ignis-run $IGNIS_HOME/bin/ignis-run
RUN chmod +x $IGNIS_HOME/bin/ignis-run
