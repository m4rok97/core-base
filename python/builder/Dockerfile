
FROM ignisframework/base-builder

PYTHON JSON_VERSION=3.7.0
RUN \
	mkdir -p /tmp/python3 && \
	cd /tmp/python3 && \
	wget https://www.python.org/ftp/python/${PY_VERSION}/Python-${PY_VERSION}.tgz && \
	tar xvf Python-${PY_VERSION}.tgz && \
	cd Python-${PY_VERSION} && \
	sed -i 's/$(MAKE) run_profile_task//' Makefile.pre.in &&\
	./configure --enable-optimizations && \
	make && \
	make install DESTDIR=/root/python3/build && \
	cd / && \
	rm -fr /tmp/python3 && \
	{ \
		echo '#!/bin/bash'; \
		echo 'cd "$(dirname "$0")"'; \
		echo 'cp -R build/* /'; \
	} > /root/python3/install && \
	chmod +x /root/python3/install && \
	/root/python3/install
