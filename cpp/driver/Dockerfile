
FROM ignisframework/base-builder as builder

ARG SOURCE_CACHE=1
RUN \
	apt -y install rapidjson-dev && \
	mkdir -p /tmp/core-cpp && \ 
	cd /tmp/core-cpp && \
	git clone https://ignis-ro:ghqvbdWcAfpDTLwLHgSf@gitlab.com/ignis-framework/ignis/core-cpp.git /tmp/core-cpp && \
	cmake -DBUILD_TESTING=OFF -DCMAKE_INSTALL_PREFIX=/usr . && \
	make && \
	cd ignis && \
	chmod +x ignis-cpp && \
	mv ignis-cpp ${IGNIS_HOME}/bin && \
	mkdir -p ${IGNIS_HOME}/core/cpp && \
	make install DESTDIR=${IGNIS_HOME}/core/cpp && \
	cd / && \
	rm -fR /tmp/core-cpp


FROM ignisframework/base-driver
COPY --from=builder /root/boost /root/boost
COPY --from=builder /root/thrift /root/thrift

RUN /root/boost/install && \
	/root/thrift/install && \
	rm -fr /root/boost /root/thrift

COPY --from=builder ${IGNIS_HOME} ${IGNIS_HOME}
RUN cp -R ${IGNIS_HOME}/core/cpp/* /