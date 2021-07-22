#!/bin/bash

ldconfig
export DEBIAN_FRONTEND=noninteractive
apt update 
apt -y --no-install-recommends install openjdk-14-jre-headless openssl
rm -rf /var/lib/apt/lists/*

{ \
	echo '#!/bin/bash'; \
	echo 'exec java -cp "${IGNIS_HOME}/lib/java/*" org.ignis.backend.Main "$@"'; \
} > ${IGNIS_HOME}/bin/ignis-backend
chmod +x bin/ignis-backend
