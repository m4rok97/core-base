
ARG REGISTRY=""
ARG TAG=""
FROM ${REGISTRY}ignishpc/base-driver${TAG}

COPY --from=${REGISTRY}ignishpc/cpp-builder${TAG} ${IGNIS_HOME} ${IGNIS_HOME}
RUN ${IGNIS_HOME}/bin/cpp-core-install.sh && \
	rm -f ${IGNIS_HOME}/bin/cpp-core-install.sh
