
ARG REGISTRY=""
ARG NAMESPACE="ignishpc/"
ARG TAG=""
FROM ${REGISTRY}${NAMESPACE}common${TAG}
ARG RELPATH=""

COPY ${RELPATH}ignis-slurm ${IGNIS_HOME}/bin
RUN chmod +x ${IGNIS_HOME}/bin/ignis-slurm
