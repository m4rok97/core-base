
FROM ignishpc/base

COPY --from=ignishpc/cpp-builder ${IGNIS_HOME} ${IGNIS_HOME}
RUN apt update && \
	apt install -y \
	gcc \
	g++ \
	make \
	cmake && \
	mv ${IGNIS_HOME}/core/cpp/include /usr/local/ && \
	${IGNIS_HOME}/bin/cpp-core-install.sh && \
	rm -f ${IGNIS_HOME}/bin/cpp-core-install.sh
