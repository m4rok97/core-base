
ARG REGISTRY=""
ARG TAG=""
FROM ${REGISTRY}ignishpc/python-builder${TAG} as python-builder
FROM ${REGISTRY}ignishpc/base-driver${TAG}

COPY --from=python-builder ${IGNIS_HOME} ${IGNIS_HOME}
RUN ${IGNIS_HOME}/bin/python-core-install.sh && \
	rm -f ${IGNIS_HOME}/bin/python-core-install.sh
