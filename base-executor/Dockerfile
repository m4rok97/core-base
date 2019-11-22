
FROM ignishpc/base

COPY --from=ignishpc/executor-builder ${IGNIS_HOME} ${IGNIS_HOME}
RUN ${IGNIS_HOME}/bin/executor-install.sh && \
	rm -f ${IGNIS_HOME}/bin/executor-install.sh
