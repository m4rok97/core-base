
FROM ignishpc/base

COPY --from=ignishpc/driver-builder ${IGNIS_HOME} ${IGNIS_HOME}
RUN ${IGNIS_HOME}/bin/driver-install.sh && \
	rm -f ${IGNIS_HOME}/bin/driver-install.sh

COPY --from=ignishpc/executor-builder ${IGNIS_HOME} ${IGNIS_HOME}
RUN ${IGNIS_HOME}/bin/executor-install.sh && \
	rm -f ${IGNIS_HOME}/bin/executor-install.sh
