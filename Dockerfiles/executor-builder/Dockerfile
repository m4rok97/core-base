
ARG REGISTRY=""
ARG NAMESPACE="ignishpc/"
ARG TAG=""
FROM ${REGISTRY}${NAMESPACE}common-builder${TAG}
ARG RELPATH=""

COPY ${RELPATH}executor-install.sh ${IGNIS_HOME}/common
RUN chmod +x ${IGNIS_HOME}/common/executor-install.sh 
