
FROM centos:7

LABEL ignis=true
ENV IGNIS_HOME="/opt/ignis"
ENV PATH="${IGNIS_HOME}/bin:${PATH}"
ENV THRIFT_VERSION=0.11.0

RUN mkdir -p \
	${IGNIS_HOME}/bin \
	${IGNIS_HOME}/core \
	${IGNIS_HOME}/backend \
	${IGNIS_HOME}/manager \
	${IGNIS_HOME}/submitter \
	${IGNIS_HOME}/etc
