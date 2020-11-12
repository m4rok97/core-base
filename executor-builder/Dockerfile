
ARG REGISTRY=""
ARG TAG=""
FROM ${REGISTRY}ignishpc/builder${TAG}

COPY executor-install.sh ${IGNIS_HOME}/bin
RUN chmod +x ${IGNIS_HOME}/bin/executor-install.sh && \
	mv /root/mpi/bin/* ${IGNIS_HOME}/bin && \
	mv /root/mpi/lib/* ${IGNIS_HOME}/lib
