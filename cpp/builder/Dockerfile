
FROM ignisframework/base-builder

ENV JSON_VERSION=1.8.4
RUN \
	mkdir -p /tmp/jsoncpp && \
	cd /tmp/jsoncpp && \
	git clone --branch ${JSON_VERSION} https://github.com/open-source-parsers/jsoncpp src && \
	cd src && \
	cmake -DBUILD_STATIC_LIBS=OFF -DBUILD_SHARED_LIBS=ON -DJSONCPP_WITH_TESTS=OFF . && \
	make && \
	mkdir -p /root/jsoncpp/build/usr/include/jsoncpp && \
	mkdir -p /root/jsoncpp/build/usr/lib && \
	mv include/json /root/jsoncpp/build/usr/include/jsoncpp && \
	mv src/lib_json/libjsoncpp* /root/jsoncpp/build/usr/lib && \
	cd / && \
	rm -fr /tmp/jsoncpp && \
	{ \
		echo '#!/bin/bash'; \
		echo 'cd "$(dirname "$0")"'; \
		echo 'cp -R build/* /'; \
		echo 'ldconfig'; \
	} > /root/jsoncpp/install && \
	chmod +x /root/jsoncpp/install && \
	/root/jsoncpp/install
