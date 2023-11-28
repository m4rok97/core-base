
ARG REGISTRY=""
ARG NAMESPACE="ignishpc/"
ARG TAG=""
FROM ${REGISTRY}${NAMESPACE}template${TAG}
ARG BUILD_CORES="4"

ENV IGNIS_DEVEL="/opt/ignis-devel"
LABEL ignis.build.context="local"

RUN rm -fR /usr2 && \
    mkdir /usr2 && \
    export DEBIAN_FRONTEND=noninteractive && \
    apt update && \
    apt -y install \
      build-essential \
      git \
      wget \
      cmake \
      automake \
      unzip \
      gettext-base \
      pkg-config && \
    rm -rf /var/lib/apt/lists/* && \
    mkdir ${IGNIS_DEVEL}

# upgrade to 0.19 TODO
ENV THRIFT_VERSION=0.15.0
RUN git clone --branch ${THRIFT_VERSION} https://github.com/apache/thrift $IGNIS_DEVEL/thrift

COPY mpi_patches /tmp/mpi_patches
ENV MPICH_THREADS=128
# upgrade to 4.X TODO
ENV MPICH_VERSION=3.4.2 
RUN mkdir /tmp/mpi && \
    cd /tmp/mpi && \
    wget https://www.mpich.org/static/downloads/${MPICH_VERSION}/mpich-${MPICH_VERSION}.tar.gz -O mpich.tar.gz && \
    tar -zxf mpich.tar.gz && \
    cd mpich-${MPICH_VERSION} && \
    bash /tmp/mpi_patches/mpi_path.sh && \
    rm -fR /tmp/mpi_patches && \
    ./configure --prefix /usr2 --with-device=ch4:ofi --with-libfabric=embedded --enable-fast=O3,ndebug --disable-error-checking \
      --without-timing --without-mpit-pvars --enable-thread-cs=per-vci --with-ch4-max-vcis=${MPICH_THREADS} --disable-fortran && \
    make -j${BUILD_CORES} && \
    make install && \
    rm -f /usr2/lib/*.a /usr2/lib/*.la && \
    rm -fR /tmp/mpi* && \
    mkdir -p $IGNIS_HOME/core/base/mpi && \
    mv /usr2/* $IGNIS_HOME/core/base/mpi
