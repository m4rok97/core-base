
ARG REGISTRY=""
ARG TAG=""
FROM ${REGISTRY}ignishpc/executor-builder${TAG} as executor-builder
FROM ${REGISTRY}ignishpc/base${TAG}

COPY --from=executor-builder ${IGNIS_HOME} ${IGNIS_HOME}
RUN ${IGNIS_HOME}/bin/executor-install.sh && \
	rm -f ${IGNIS_HOME}/bin/executor-install.sh
