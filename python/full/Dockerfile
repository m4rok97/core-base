
ARG REGISTRY=""
ARG TAG=""
FROM ${REGISTRY}ignishpc/base-full${TAG}

COPY --from=ignishpc/python-builder ${IGNIS_HOME} ${IGNIS_HOME}
RUN ${IGNIS_HOME}/bin/python-core-install.sh && \
	rm -f ${IGNIS_HOME}/bin/python-core-install.sh
