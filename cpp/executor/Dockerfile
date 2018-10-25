
FROM ignisframework/cpp-builder as builder

ARG SOURCE_CACHE=1
RUN \
	mkdir -p /tmp/core-cpp && \ 
	cd /tmp/core-cpp && \
	git clone https://ignis-ro:ghqvbdWcAfpDTLwLHgSf@gitlab.com/ignis-framework/ignis/core-cpp.git /tmp/core-cpp && \
	cmake -DBUILD_TESTING=OFF . && \
	make && \
	chmod +x ignis-cpp && \
	mv igniscpp ${IGNIS_HOME}/bin && \
	mkdir -p ${IGNIS_HOME}/core/cpp && \
	make install DESTDIR=${IGNIS_HOME}/core/cpp && \
	mv ignis-cpp ${IGNIS_HOME}/core/cpp/ignis-cpp && \
	cd / && \
	rm -fR /tmp/core-cpp


FROM ignisframework/base-executor

COPY --from=builder ${IGNIS_HOME} ${IGNIS_HOME}

COPY --from=builder /root/jsoncpp /root/jsoncpp

RUN \
	yum -y install libgomp && \
	cp -R ${IGNIS_HOME}/core/cpp/* / && \
	/root/jsoncpp/install && \
	rm -fr /root/jsoncpp && \
	ldconfig && \
	cp ${IGNIS_HOME}/core/cpp/ignis-cpp ${IGNIS_HOME}/bin