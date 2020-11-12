
ARG REGISTRY=""
ARG TAG=""
FROM ${REGISTRY}ignishpc/base-full${TAG}

COPY --from=ignishpc/cpp-builder ${IGNIS_HOME} ${IGNIS_HOME}
RUN ${IGNIS_HOME}/bin/cpp-core-install.sh && \
	rm -f ${IGNIS_HOME}/bin/cpp-core-install.sh

COPY --from=ignishpc/python-builder ${IGNIS_HOME} ${IGNIS_HOME}
RUN ${IGNIS_HOME}/bin/python-core-install.sh && \
	rm -f ${IGNIS_HOME}/bin/python-core-install.sh
