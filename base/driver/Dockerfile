
FROM ignisframework/base-builder as builder

ARG SOURCE_CACHE=1
RUN \
	mkdir -p /tmp/backend && \ 
	cd /tmp/backend && \
	git clone https://ignis-ro:ghqvbdWcAfpDTLwLHgSf@gitlab.com/ignis-framework/ignis/backend.git src && \
	gradle jarAndLibs && \
	mv build/libs/* ${IGNIS_HOME}/backend && \
	cd ${IGNIS_HOME} && \
	rm -fr /tmp/backend && \
	{ \
		echo '#!/bin/bash'; \
		echo 'java -cp "${IGNIS_HOME}/backend/*" org.ignis.backend.Main "$@"'; \
	} > bin/ignis-backend  && chmod +x bin/ignisbackend


FROM ignisframework/base-skel

RUN yum -y install java-1.8.0-openjdk
COPY --from=builder ${IGNIS_HOME} ${IGNIS_HOME}

