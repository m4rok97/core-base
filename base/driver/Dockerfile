
FROM ignisframework/base-builder as builder

ARG SOURCE_CACHE=1
RUN \
	mkdir -p /tmp/backend && \ 
	git clone https://ignis-ro:ghqvbdWcAfpDTLwLHgSf@gitlab.com/ignis-framework/ignis/backend.git /tmp/backend && \
	cd /tmp/backend && \
	gradle jarAndLibs && \
	mv build/libs/* ${IGNIS_HOME}/backend && \
	cd ${IGNIS_HOME} && \
	rm -fr /tmp/backend && \
	{ \
		echo '#!/bin/bash'; \
		echo 'exec java -cp "${IGNIS_HOME}/backend/*" org.ignis.backend.Main "$@"'; \
	} > bin/ignis-backend  && chmod +x bin/ignis-backend

RUN \
	mkdir -p /tmp/utils && \ 
	git clone https://ignis-ro:ghqvbdWcAfpDTLwLHgSf@gitlab.com/ignis-framework/ignis/utils.git /tmp/utils && \
	mv /tmp/utils/etc/* ${IGNIS_HOME}/etc && \
	cd /tmp/utils/line-indexing && \
	cmake . && \
	make && \
	mv /tmp/utils/line-indexing/ii ${IGNIS_HOME}/bin/ && \
	mv /tmp/utils/line-indexing/icp ${IGNIS_HOME}/bin/ && \
	mv /tmp/utils/line-indexing/libignisindex.so ${IGNIS_HOME}/bin/ && \
	cd /tmp/utils/submit-remote && \
	gradle jarAndLibs && \
	mv build/libs/* ${IGNIS_HOME}/backend && \
	cd ${IGNIS_HOME} && \
	rm -fr /tmp/utils && \
	{ \
		echo '#!/bin/bash'; \
		echo 'exec java -cp "${IGNIS_HOME}/backend/*" org.ignis.submit.remote.Main "$@"'; \
	} > bin/isr  && chmod +x bin/isr 


FROM ignisframework/base-skel

RUN apt update && apt -y install openjdk-8-jdk gcc g++ openssl
COPY --from=builder ${IGNIS_HOME} ${IGNIS_HOME}
