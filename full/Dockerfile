
ARG REGISTRY=""
ARG TAG=""
FROM ${REGISTRY}ignishpc/cpp-builder${TAG} as cpp-builder
FROM ${REGISTRY}ignishpc/python-builder${TAG} as python-builder
FROM ${REGISTRY}ignishpc/base-full${TAG}

COPY --from=cpp-builder ${IGNIS_HOME} ${IGNIS_HOME}
RUN ${IGNIS_HOME}/bin/cpp-core-install.sh && \
	rm -f ${IGNIS_HOME}/bin/cpp-core-install.sh

COPY --from=python-builder ${IGNIS_HOME} ${IGNIS_HOME}
RUN ${IGNIS_HOME}/bin/python-core-install.sh && \
	rm -f ${IGNIS_HOME}/bin/python-core-install.sh
