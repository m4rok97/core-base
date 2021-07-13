
ARG REGISTRY=""
ARG TAG=""
FROM ${REGISTRY}ignishpc/common-builder${TAG}
ARG RELPATH=""

COPY ${RELPATH}executor-install.sh ${IGNIS_HOME}/common
RUN chmod +x ${IGNIS_HOME}/common/executor-install.sh 
