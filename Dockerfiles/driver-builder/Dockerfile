
ARG REGISTRY=""
ARG TAG=""
FROM ${REGISTRY}ignishpc/common-builder${TAG}
ARG RELPATH=""


RUN export DEBIAN_FRONTEND=noninteractive && \
	apt update && \
	apt -y --no-install-recommends  install openjdk-14-jdk && \
	rm -rf /var/lib/apt/lists/*


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

COPY / /tmp/backend/
RUN mkdir ${IGNIS_HOME}/lib/java && \
	cd /tmp/backend && \
	gradle jarAndLibs && \
	rm -rf ~/.gradle && \
	mv libs/* ${IGNIS_HOME}/lib/java && \
	cd ${IGNIS_HOME} && \
	mv /tmp/backend/etc/* ./etc/ && \
	rm -fr /tmp/backend

COPY ${RELPATH}driver-install.sh ${IGNIS_HOME}/common
RUN chmod +x ${IGNIS_HOME}/common/driver-install.sh 
