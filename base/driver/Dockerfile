
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
		echo 'java -cp "${IGNIS_HOME}/backend/*" org.ignis.backend.Main "$@"'; \
	} > bin/ignis-backend  && chmod +x bin/ignis-backend

RUN \
	mkdir -p /tmp/utils && \ 
	git clone https://ignis-ro:ghqvbdWcAfpDTLwLHgSf@gitlab.com/ignis-framework/ignis/utils.git /tmp/utils && \
	cd /tmp/utils/line-indexing && \
	cmake . && \
	make && \
	mv /tmp/utils/line-indexing/ii ${IGNIS_HOME}/bin/ && \
	mv /tmp/utils/line-indexing/icp ${IGNIS_HOME}/bin/ && \
	mv /tmp/utils/line-indexing/libignisindex.so ${IGNIS_HOME}/bin/ && \
	cd /tmp/utils/remote-client && \
	cmake . && \
	make && \
	mv /tmp/utils/remote-client/irc ${IGNIS_HOME}/bin/ && \
	cd / && \
	rm -fr /tmp/utils

FROM ignisframework/base-skel

RUN yum -y install java-1.8.0-openjdk openssl
COPY --from=builder ${IGNIS_HOME} ${IGNIS_HOME}

COPY --from=builder /root/boost /root/boost
COPY --from=builder /root/thrift /root/thrift

RUN \
	cd /root/boost && \
	./install && \
	cd /root/thrift && \
	./install && \
	cd / && \
	rm -fr /root/boost /root/thrift
