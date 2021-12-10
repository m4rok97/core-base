
ARG REGISTRY=""
ARG NAMESPACE="ignishpc"
ARG TAG=""
FROM ${REGISTRY}${NAMESPACE}common${TAG}
ARG RELPATH=""

RUN ${IGNIS_HOME}/common/driver-install.sh && \
	rm -fR ${IGNIS_HOME}/common && \
	rm -f ${IGNIS_HOME}/bin/* && \
	export DEBIAN_FRONTEND=noninteractive && \
	apt update && \
	apt install -y --no-install-recommends \
		git \
		wget \
		unzip \ 
		python3 \
		python3-distutils && \
	rm -rf /var/lib/apt/lists/* && \
	wget https://bootstrap.pypa.io/get-pip.py -O /tmp/get-pip.py && \
	python3 /tmp/get-pip.py && \
	rm -f /tmp/get-pip.py

COPY ${RELPATH}ignis-sshd ${IGNIS_HOME}/bin
COPY ${RELPATH}ignis-submit ${IGNIS_HOME}/bin

RUN chmod +x ${IGNIS_HOME}/bin/ignis-sshd && \
	chmod +x ${IGNIS_HOME}/bin/ignis-submit && \
	rm -f ${IGNIS_HOME}/bin/ignis-backend

