
ARG REGISTRY=""
ARG NAMESPACE="ignishpc/"
ARG TAG=""
FROM ${REGISTRY}${NAMESPACE}mesos-base${TAG}
ARG DOCK_DIR=""

LABEL ignis.build="optional"

ENV _MARATHON_VERSION=1.8.222-86475ddac
RUN cd /tmp && \
    wget https://downloads.mesosphere.com/marathon/builds/${_MARATHON_VERSION}/marathon-${_MARATHON_VERSION}.tgz  && \
    tar -zxf marathon-${_MARATHON_VERSION}.tgz  && \
    rm -f marathon-${_MARATHON_VERSION}.tgz && \
    mv $(ls | grep marathon-${_MARATHON_VERSION}*) /opt/marathon

COPY ${DOCK_DIR}auto_clear.py /opt/marathon/bin/
RUN { \
        echo '#!/bin/bash'; \
        echo 'start-mesos.sh &'; \
        echo 'export JAVA_HOME=$(echo /usr/lib/jvm/java-8-openjdk-* | cut -d "" -f1)'; \
        echo '/opt/marathon/bin/marathon --master ${ZOOKEEPER}/mesos --zk ${ZOOKEEPER}/marathon --http_port ${PORT_SERVICE} &'; \
        echo 'python3 /opt/marathon/bin/auto_clear.py &'; \
        echo 'wait'; \
    } > /bin/start-marathon.sh  && chmod +x /bin/start-marathon.sh

