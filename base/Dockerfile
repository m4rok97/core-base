
FROM ubuntu:20.04

LABEL ignis="1.0"
ENV IGNIS_HOME="/opt/ignis"
ENV PATH="${IGNIS_HOME}/bin:${PATH}"

RUN mkdir -p \
	${IGNIS_HOME}/bin \
	${IGNIS_HOME}/core \
	${IGNIS_HOME}/lib \
	${IGNIS_HOME}/etc && \
	echo ${IGNIS_HOME}/lib > /etc/ld.so.conf.d/ignis-lib.conf
