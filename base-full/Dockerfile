
ARG REGISTRY=""
ARG TAG=""
FROM ${REGISTRY}ignishpc/driver-builder${TAG} as driver-builder
FROM ${REGISTRY}ignishpc/executor-builder${TAG} as executor-builder
FROM ${REGISTRY}ignishpc/base${TAG}

COPY --from=driver-builder ${IGNIS_HOME} ${IGNIS_HOME}
RUN ${IGNIS_HOME}/bin/driver-install.sh && \
	rm -f ${IGNIS_HOME}/bin/driver-install.sh

COPY --from=executor-builder ${IGNIS_HOME} ${IGNIS_HOME}
RUN ${IGNIS_HOME}/bin/executor-install.sh && \
	rm -f ${IGNIS_HOME}/bin/executor-install.sh
