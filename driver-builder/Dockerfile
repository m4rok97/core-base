
ARG REGISTRY=""
ARG TAG=""
FROM ${REGISTRY}ignishpc/builder${TAG}

COPY driver-install.sh ${IGNIS_HOME}/bin
RUN chmod +x ${IGNIS_HOME}/bin/driver-install.sh && \
	mv /root/mpi/bin/* ${IGNIS_HOME}/bin && \
	mv /root/mpi/lib/* ${IGNIS_HOME}/lib

RUN export DEBIAN_FRONTEND=noninteractive && \
	apt update && \
	apt -y install openjdk-14-jdk

ENV GRADLE_VERSION=6.7
RUN \
	mkdir -p /tmp/gradle && \ 
	cd /tmp/gradle && \
	wget https://services.gradle.org/distributions/gradle-${GRADLE_VERSION}-bin.zip -O src.zip && \
	unzip src && \
	mv gradle-${GRADLE_VERSION} /opt/gradle && \
	ln -s /opt/gradle/bin/gradle /usr/local/bin/gradle && \
	cd / && \
	rm -fr /tmp/gradle


ARG SOURCE_CACHE=1
RUN mkdir ${IGNIS_HOME}/lib/java && \
	mkdir /tmp/backend && \ 
	git clone https://ignis:aTCXjiyroiGyrh5XNbhS@gitlab.com/ignis-hpc/backend.git /tmp/backend && \
	cd /tmp/backend && \
	gradle jarAndLibs && \
	rm -rf ~/.gradle && \
	mv build/libs/* ${IGNIS_HOME}/lib/java && \
	cd ${IGNIS_HOME} && \
	mv /tmp/backend/etc/* ./etc/ && \
	rm -fr /tmp/backend && \
	{ \
		echo '#!/bin/bash'; \
		echo 'exec java -cp "${IGNIS_HOME}/lib/java/*" org.ignis.backend.Main "$@"'; \
	} > bin/ignis-backend  && chmod +x bin/ignis-backend
