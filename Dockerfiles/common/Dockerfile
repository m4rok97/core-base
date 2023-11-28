
ARG REGISTRY=""
ARG NAMESPACE="ignishpc/"
ARG TAG=""
FROM ${REGISTRY}${NAMESPACE}common-builder${TAG} as common-builder
FROM ${REGISTRY}${NAMESPACE}driver-builder${TAG} as driver-builder
FROM ${REGISTRY}${NAMESPACE}executor-builder${TAG} as executor-builder
FROM ${REGISTRY}${NAMESPACE}base${TAG}
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
	sed -i 's/#PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config && \
	sed -i 's/UsePAM yes/UsePAM no/g' /etc/ssh/sshd_config && \
	sed -i "s|#AuthorizedKeysFile.*|AuthorizedKeysFile /ssh/.ssh/authorized_keys|" /etc/ssh/sshd_config
