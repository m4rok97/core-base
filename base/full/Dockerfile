
FROM ignisframework/base-executor

RUN apt update && apt -y install openjdk-8-jdk
COPY --from=ignisframework/base-driver ${IGNIS_HOME} ${IGNIS_HOME}