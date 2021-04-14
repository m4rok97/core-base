
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


COPY mpi_patches /tmp/mpi_patches
ENV MPICH_THREADS=64
ENV MPICH_VERSION=3.4.1
RUN mkdir /tmp/mpi && \
	cd /tmp/mpi && \
	wget https://www.mpich.org/static/downloads/${MPICH_VERSION}/mpich-${MPICH_VERSION}.tar.gz && \
	tar -zxf mpich-${MPICH_VERSION}.tar.gz && \
	cd mpich-${MPICH_VERSION} && \
	bash /tmp/mpi_patches/mpi_path.sh && \
	rm -fR /tmp/mpi_patches && \
	./configure --prefix /root/mpi --with-device=ch4:ofi --with-libfabric=embedded --enable-fast=O3,ndebug --disable-error-checking --without-timing --without-mpit-pvars \
			--enable-thread-cs=per-vci --with-ch4-max-vcis=${MPICH_THREADS} && \
	make -j && \
	make install && \
	rm -f /root/mpi/lib/*.a && \
	rm -f /root/mpi/lib/*.la && \
	rm -fR /tmp/mpi

ENV THRIFT_VERSION=0.14.1
RUN git clone --branch ${THRIFT_VERSION} https://github.com/apache/thrift /root/thrift
COPY ignis-run $IGNIS_HOME/bin/ignis-run
RUN chmod +x $IGNIS_HOME/bin/ignis-run
