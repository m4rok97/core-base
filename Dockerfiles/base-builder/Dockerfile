
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

ENV GRADLE_VERSION=8.5
RUN cd /opt && \
    wget https://services.gradle.org/distributions/gradle-${GRADLE_VERSION}-bin.zip -O gradle.zip && \
    unzip gradle.zip && \
    rm -f gradle.zip && \
    ln -s /opt/gradle-${GRADLE_VERSION}/bin/gradle /usr/local/bin/gradle

COPY / /tmp/core-base/
RUN mkdir ${IGNIS_HOME}/lib/java && \
    cd /tmp/core-base && \
    gradle jarlibs && \
    rm -rf ~/.gradle && \
    mv build/jarlibs/* ${IGNIS_HOME}/lib/java && \
    mv etc/* ${IGNIS_HOME}/etc/ && \
    rm -fr /tmp/core-base

COPY ${DOCK_DIR}bin ${IGNIS_HOME}/bin/
RUN cd ${IGNIS_HOME}/bin && \
    chmod +x * && \
    envsubst <ignis-base-install.sh | sponge ignis-base-install.sh
