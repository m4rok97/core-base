
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
	mv ignismanager ${IGNIS_HOME}/bin && \ 
	cd / && \
	rm -fr /tmp/manager


FROM ignisframework/base-skel

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