
FROM ignisframework/base-builder as builder

ARG SOURCE_CACHE=1
RUN \
	mkdir -p /tmp/manager && \ 
	cd /tmp/manager && \
	git clone https://ignis-ro:ghqvbdWcAfpDTLwLHgSf@gitlab.com/ignis-framework/ignis/manager.git src && \
	cd src && \
	cmake . && \
	make && \
	chmod +x ignismanager && \ 
	mv ignismanager ${IGNIS_HOME}/bin/ignis-manager && \ 
	cd / && \
	rm -fr /tmp/manager


FROM ignisframework/base-skel

RUN apt update && apt -y install gcc g++
COPY --from=builder ${IGNIS_HOME} ${IGNIS_HOME}

COPY --from=builder /root/boost /root/boost
COPY --from=builder /root/thrift /root/thrift

RUN /root/boost/install && \
	/root/thrift/install && \
	rm -fr /root/boost /root/thrift