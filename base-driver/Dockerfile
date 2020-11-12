
ARG REGISTRY=""
ARG TAG=""
FROM ${REGISTRY}ignishpc/base${TAG}

COPY --from=ignishpc/driver-builder ${IGNIS_HOME} ${IGNIS_HOME}
RUN ${IGNIS_HOME}/bin/driver-install.sh && \
	rm -f ${IGNIS_HOME}/bin/driver-install.sh