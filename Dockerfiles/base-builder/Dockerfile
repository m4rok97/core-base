
ARG REGISTRY=""
ARG NAMESPACE="ignishpc/"
ARG TAG=""
FROM ${REGISTRY}${NAMESPACE}builder${TAG}
ARG DOCK_DIR=""

ENV JDK_VERSION=21

RUN export DEBIAN_FRONTEND=noninteractive && \
    apt update && \
    apt -y --no-install-recommends install openjdk-${JDK_VERSION}-jdk && \
    rm -rf /var/lib/apt/lists/*

# remove rc-1 TODO
ENV GRADLE_VERSION=8.5-rc-1 
RUN cd /opt && \
    wget https://services.gradle.org/distributions/gradle-${GRADLE_VERSION}-bin.zip -O gradle.zip && \
    unzip gradle.zip && \
    rm -f gradle.zip && \
    ln -s /opt/gradle-${GRADLE_VERSION}/bin/gradle /usr/local/bin/gradle

COPY / /tmp/backend/
RUN mkdir ${IGNIS_HOME}/lib/java && \
    cd /tmp/backend && \
    gradle jarlibs && \
    rm -rf ~/.gradle && \
    mv build/jarlibs/* ${IGNIS_HOME}/lib/java && \
    mv etc/* ${IGNIS_HOME}/etc/ && \
    rm -fr /tmp/backend

COPY ${DOCK_DIR}bin ${IGNIS_HOME}/bin/
RUN chmod +x ${IGNIS_HOME}/bin/* && \
    envsubst <${IGNIS_HOME}/bin/ignis-base-install.sh >${IGNIS_HOME}/bin/ignis-base-install.sh
