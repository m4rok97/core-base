
ARG REGISTRY=""
ARG TAG=""
FROM ${REGISTRY}ignishpc/driver-builder${TAG} as driver-builder
FROM ${REGISTRY}ignishpc/base${TAG}

COPY --from=driver-builder ${IGNIS_HOME} ${IGNIS_HOME}
RUN ${IGNIS_HOME}/bin/driver-install.sh && \
	rm -f ${IGNIS_HOME}/bin/driver-install.sh && \
	rm -f ${IGNIS_HOME}/bin/ignis-backend && \
	export DEBIAN_FRONTEND=noninteractive && \
	apt update && \
	apt install -y \
		git \
		wget \
		unzip \
		openssh-server \
		python3 \
		python3-distutils && \
	wget https://bootstrap.pypa.io/get-pip.py -O /tmp/get-pip.py && \
	python3 /tmp/get-pip.py && \
	rm -f /tmp/get-pip.py && \
	mkdir /var/run/sshd && \
	sed -i 's/#PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config && \
	cd ${IGNIS_HOME}/bin && \
	{ \
		echo '#!/bin/bash'; \
		echo 'export -p >> /etc/profile'; \
		echo 'export -p | sed "s/^declare -x //" > /etc/environment'; \
		echo 'mkdir -p ~/.ssh'; \
		echo '/usr/sbin/sshd -D'; \
	} > ignis-sshd  && \
	chmod +x ignis-sshd && \
	{ \
		echo '#!/bin/bash'; \
		echo 'exec java -cp "${IGNIS_HOME}/lib/java/*" org.ignis.backend.Submit "$@"'; \
	} > ignis-submit  && \
	chmod +x ignis-submit
