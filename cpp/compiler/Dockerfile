
FROM ignishpc/base

COPY --from=ignishpc/cpp-builder ${IGNIS_HOME} ${IGNIS_HOME}
COPY --from=ignishpc/cpp-builder /root/mpi/ /root/mpi/
RUN apt update && \
	apt install -y \
	gcc \
	g++ \
	make \
	cmake && \
	cp -R /root/mpi/bin/* ${IGNIS_HOME}/bin && \
	cp -R /root/mpi/lib/* ${IGNIS_HOME}/lib/native && \
	rm -fR /root/mpi && \
	echo ${IGNIS_HOME}/lib/native > /etc/ld.so.conf.d/ignis-lib.conf && \
	ln /opt/ignis/lib/native/libigniscore.so /usr/local/lib/libigniscore.so && \
	mv ${IGNIS_HOME}/core/cpp/include /usr/local/ && \
	${IGNIS_HOME}/bin/cpp-core-install.sh && \
	rm -f ${IGNIS_HOME}/bin/cpp-core-install.sh
