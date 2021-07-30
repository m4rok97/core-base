
ARG REGISTRY=""
ARG TAG=""
FROM ubuntu:20.04
ARG RELPATH=""

RUN apt update && \
	apt install -y --no-install-recommends \
		curl \
		gnupg \
		ca-certificates \
		&& \
	curl -fsSL https://apt.releases.hashicorp.com/gpg | apt-key add - && \
	. /etc/lsb-release && \
	echo "deb [arch=amd64] https://apt.releases.hashicorp.com $DISTRIB_CODENAME main" >> /etc/apt/sources.list && \
	apt update && \
	apt install -y nomad && \
	rm -rf /var/lib/apt/lists/*