
FROM ignisframework/base-builder

ENV PY_VERSION=3.7.0
RUN \
	yum install -y \ 
		centos-release-scl \
		devtoolset-7-gcc-c++ \
		bzip2-devel \
		libffi-devel \
		openssl-devel \
		ncurses-devel \
		gdbm-devel \
		liblzma-dev \
		readline-devel && \
	source /opt/rh/devtoolset-7/enable && \
	mkdir -p /tmp/python3 && \
	cd /tmp/python3 && \
	wget https://www.python.org/ftp/python/${PY_VERSION}/Python-${PY_VERSION}.tgz && \
	tar xvf Python-${PY_VERSION}.tgz && \
	cd Python-${PY_VERSION} && \
	./configure --enable-optimizations --prefix=/opt/python3 && \
	make && \
	make altinstall DESTDIR=/root/python3/build && \
	cd / && \
	rm -fr /tmp/python3 && \
	{ \
		echo '#!/bin/bash'; \
		echo 'cd "$(dirname "$0")"'; \
		echo 'yum install -y libffi'; \
		echo 'cp -R build/* /'; \
		echo 'find /opt/python3/bin -name "*3*" -print0 | \
			xargs -0 -I {} echo '"'"'ln -s {} /usr/bin/$(basename {} | \
			sed -r "s/(\.[0-9]+)+//g")'"'"' | \ 
			xargs -0 -I {} bash -c {}'; \
	} > /root/python3/install && \
	chmod +x /root/python3/install && \
	/root/python3/install

RUN \
	cp -R /tmp/thrift/lib/py /root/pythrift && \
	cd /root/pythrift && \
	python3 setup.py build && \
	{ \
		echo '#!/bin/bash'; \
		echo 'cd "$(dirname "$0")"'; \
		echo 'python3 setup.py install'; \
	} > /root/pythrift/install && \
	chmod +x /root/pythrift/install && \
	/root/pythrift/install
