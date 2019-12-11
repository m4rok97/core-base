
FROM ignishpc/base-full

COPY --from=ignishpc/cpp-builder ${IGNIS_HOME} ${IGNIS_HOME}
RUN ${IGNIS_HOME}/bin/cpp-core-install.sh && \
	rm -f ${IGNIS_HOME}/bin/cpp-core-install.sh
