
ARG REGISTRY=""
ARG TAG=""
FROM ${REGISTRY}ignishpc/base${TAG}

COPY --from=ignishpc/cpp-builder ${IGNIS_HOME} ${IGNIS_HOME}
COPY --from=ignishpc/cpp-builder /root/mpi/bin/* ${IGNIS_HOME}/bin/

RUN export DEBIAN_FRONTEND=noninteractive && \
	apt update && \
	apt install -y \
	gcc \
	g++ \
	make \
	cmake 

RUN cd ${IGNIS_HOME}/core/cpp && \
	for file in */*; do ln -s ${IGNIS_HOME}/core/cpp/$file /usr/local/$file ; done
