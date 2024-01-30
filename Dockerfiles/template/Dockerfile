
ARG REGISTRY=""
ARG NAMESPACE="ignishpc/"
ARG TAG=""

FROM ubuntu:24.04
ARG DOCK_DIR=""
ARG VERSION="dev"

ENV IGNIS_VERSION=${VERSION}
ENV IGNIS_HOME="/opt/ignis"
ENV PATH="${IGNIS_HOME}/bin:${PATH}"

RUN mkdir -p \
    ${IGNIS_HOME}/bin \
    ${IGNIS_HOME}/core \
    ${IGNIS_HOME}/lib \
    ${IGNIS_HOME}/include \
    ${IGNIS_HOME}/etc \
    ${IGNIS_HOME}/env.d && \
    echo ${IGNIS_HOME}/lib > /etc/ld.so.conf.d/ignis-lib.conf && \
    ln -s /usr /usr2
