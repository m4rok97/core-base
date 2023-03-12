
ARG REGISTRY=""
ARG NAMESPACE="ignishpc/"
ARG TAG=""
FROM ${REGISTRY}${NAMESPACE}builder${TAG}
ARG RELPATH=""

COPY ${RELPATH}mpi_patches /tmp/mpi_patches
ENV MPICH_THREADS=64
ENV MPICH_VERSION=3.4.2
RUN mkdir /tmp/mpi && \
	cd /tmp/mpi && \
	wget https://www.mpich.org/static/downloads/${MPICH_VERSION}/mpich-${MPICH_VERSION}.tar.gz && \
	tar -zxf mpich-${MPICH_VERSION}.tar.gz && \
	cd mpich-${MPICH_VERSION} && \
	bash /tmp/mpi_patches/mpi_path.sh && \
	rm -fR /tmp/mpi_patches && \
	./configure --prefix /usr2 --with-device=ch4:ofi --with-libfabric=embedded --enable-fast=O3,ndebug --disable-error-checking --without-timing --without-mpit-pvars \
			--enable-thread-cs=per-vci --with-ch4-max-vcis=${MPICH_THREADS} && \
	make -j4 && \
	make install && \
	rm -f /usr2/lib/*.a && \
	rm -f /usr2/lib/*.la && \
	rm -fR /tmp/mpi* && \
	mkdir $IGNIS_DEVEL/mpi && \
	mv /usr2/* $IGNIS_DEVEL/mpi


COPY ${RELPATH}ignis-run $IGNIS_HOME/bin/ignis-run
COPY ${RELPATH}ignis-server $IGNIS_HOME/bin/ignis-server
COPY ${RELPATH}ignis-log $IGNIS_HOME/bin/ignis-log
RUN mkdir $IGNIS_HOME/common && \
	chmod +x $IGNIS_HOME/bin/ignis-run && \
	chmod +x $IGNIS_HOME/bin/ignis-server && \
	chmod +x $IGNIS_HOME/bin/ignis-log
