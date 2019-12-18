
FROM ignishpc/builder

COPY executor-install.sh ${IGNIS_HOME}/bin
RUN chmod +x ${IGNIS_HOME}/bin/executor-install.sh && \
	mv /root/mpi/bin/* ${IGNIS_HOME}/bin && \
	mkdir ${IGNIS_HOME}/lib/native && \
	mv /root/mpi/lib/* ${IGNIS_HOME}/lib/native
