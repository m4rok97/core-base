
FROM ignisframework/base-full

COPY --from=ignisframework/cpp-executor ${IGNIS_HOME} ${IGNIS_HOME}

COPY --from=ignisframework/cpp-builder /root/jsoncpp /root/jsoncpp

RUN \
	yum -y install libgomp && \
	cp -R ${IGNIS_HOME}/core/cpp/* / && \
	/root/jsoncpp/install && \
	rm -fr /root/jsoncpp && \
	cp ${IGNIS_HOME}/core/cpp/ignis-cpp ${IGNIS_HOME}/bin