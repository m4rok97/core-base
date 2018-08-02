
FROM ignisframework/base-builder

ENV JSON_VERSION=1.8.4
RUN \
	mkdir -p /tmp/jsoncpp && \
	cd /tmp/jsoncpp && \
	git clone --branch ${JSON_VERSION} https://github.com/open-source-parsers/jsoncpp src && \
	cd src && \
	cmake . && \
	make && \
	mkdir -p /root/jsoncpp/build/usr/include/jsoncpp && \
	mkdir -p /root/jsoncpp/build/usr/lib && \
	mv include/json /root/jsoncpp/build/usr/include/jsoncpp && \
	mv src/lib_json/libjsoncpp.a /root/jsoncpp/build/usr/lib && \
	cd / && \
	rm -fr /tmp/jsoncpp && \
	{ \
		echo '#!/bin/bash'; \
		echo 'cd "$(dirname "$0")"'; \
		echo 'cp -R build/* /'; \
	} > /root/jsoncpp/install && \
	chmod +x /root/jsoncpp/install && \
	/root/jsoncpp/install
