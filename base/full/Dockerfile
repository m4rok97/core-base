
FROM ignisframework/base-executor

RUN yum -y install java-1.8.0-openjdk
COPY --from=ignisframework/base-driver ${IGNIS_HOME} ${IGNIS_HOME}