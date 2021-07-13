
ARG REGISTRY=""
ARG TAG=""
FROM ${REGISTRY}ignishpc/common-builder${TAG} as common-builder
FROM ${REGISTRY}ignishpc/driver-builder${TAG} as driver-builder
FROM ${REGISTRY}ignishpc/executor-builder${TAG} as executor-builder
FROM ${REGISTRY}ignishpc/base${TAG}
ARG RELPATH=""

COPY --from=common-builder ${IGNIS_HOME} ${IGNIS_HOME}
COPY --from=common-builder ${IGNIS_DEVEL}/mpi/bin/* ${IGNIS_HOME}/bin/
COPY --from=common-builder ${IGNIS_DEVEL}/mpi/lib/* ${IGNIS_HOME}/lib/

COPY --from=driver-builder ${IGNIS_HOME} ${IGNIS_HOME}
COPY --from=executor-builder ${IGNIS_HOME} ${IGNIS_HOME}

RUN export DEBIAN_FRONTEND=noninteractive && \
	apt update && \
	apt -y --no-install-recommends install \
		tzdata \
		curl \
		openssl \
		ca-certificates \
		openssh-server && \
	rm -rf /var/lib/apt/lists/* && \
	ldconfig && \
	mkdir /var/run/sshd && \
	sed -i 's/#PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config
