
ARG REGISTRY=""
ARG TAG=""
FROM ${REGISTRY}ignishpc/mesos-base${TAG}
ARG RELPATH=""

ENV _SINGULARITY_VERSION=1.3.0
RUN mkdir /opt/singularity && \
	cd /opt/singularity && \
	wget https://repo1.maven.org/maven2/com/hubspot/SingularityService/${_SINGULARITY_VERSION}/SingularityService-${_SINGULARITY_VERSION}-shaded.jar && \
	wget https://repo1.maven.org/maven2/com/sun/activation/javax.activation/1.2.0/javax.activation-1.2.0.jar && \
	wget https://repo1.maven.org/maven2/javax/xml/bind/jaxb-api/2.3.1/jaxb-api-2.3.1.jar  && \
	wget https://repo1.maven.org/maven2/com/sun/xml/bind/jaxb-core/3.0.0/jaxb-core-3.0.0.jar && \
	wget https://repo1.maven.org/maven2/com/sun/xml/bind/jaxb-impl/3.0.0/jaxb-impl-3.0.0.jar

RUN { \
		echo '#!/bin/bash'; \
		echo 'start-mesos.sh &'; \
		echo 'java -cp "/opt/singularity/*" com.hubspot.singularity.SingularityService server /etc/singularity/config.yaml &'; \
		echo 'wait'; \
	} > /bin/start-singularity.sh  && chmod +x /bin/start-singularity.sh

