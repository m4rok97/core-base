
FROM ignisframework/cpp-builder as builder

ARG SOURCE_CACHE=1
RUN \
	mkdir -p /tmp/core-cpp && \ 
	cd /tmp/core-cpp && \
	git clone https://ignis-ro:ghqvbdWcAfpDTLwLHgSf@gitlab.com/ignis-framework/ignis/core-cpp.git src && \
	cd src && \
	cmake . && \
	make && \
	chmod +x igniscpp && \
	mv igniscpp ${IGNIS_HOME}/bin && \
	mkdir -p ${IGNIS_HOME}/core/cpp && \
	make install DESTDIR=${IGNIS_HOME}/core/cpp


FROM ignisframework/base-full

COPY --from=builder ${IGNIS_HOME} ${IGNIS_HOME}

COPY --from=builder /root/jsoncpp /root/jsoncpp

RUN \
	yum -y install libgomp && \
	cp -R ${IGNIS_HOME}/core/cpp/* / && \
	cd /root/jsoncpp && \
	./install && \
	ldconfig && \
	rm -fr /root/jsoncpp