
ARG REGISTRY=""
ARG TAG=""
FROM ubuntu:20.04
ARG RELPATH=""

LABEL ignis="0.0"
ENV IGNIS_HOME="/opt/ignis"
ENV IGNIS_DEVEL="/opt/ignis-devel"
ENV PATH="${IGNIS_HOME}/bin:${PATH}"

RUN mkdir -p \
	${IGNIS_HOME}/bin \
	${IGNIS_HOME}/core \
	${IGNIS_HOME}/lib \
	${IGNIS_HOME}/etc && \
	echo ${IGNIS_HOME}/lib > /etc/ld.so.conf.d/ignis-lib.conf && \
	mkdir $IGNIS_DEVEL && \
	ln -s /usr /usr2
