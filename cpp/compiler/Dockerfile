
FROM ignishpc/base

COPY --from=ignishpc/cpp-builder ${IGNIS_HOME} ${IGNIS_HOME}
COPY --from=ignishpc/cpp-builder /root/mpi/bin/* ${IGNIS_HOME}/bin/
RUN apt update && \
	apt install -y \
	gcc \
	g++ \
	make \
	cmake && \
	mv ${IGNIS_HOME}/lib/native/* /usr/local/lib/ && \
	echo ${IGNIS_HOME}/lib/native > /etc/ld.so.conf.d/ignis-lib.conf && \
	mv ${IGNIS_HOME}/core/cpp/include /usr/local/ && \
	${IGNIS_HOME}/bin/cpp-core-install.sh && \
	rm -f ${IGNIS_HOME}/bin/cpp-core-install.sh
