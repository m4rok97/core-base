
FROM ignisframework/base-driver

COPY --from=ignisframework/python-builder/root/python3 /tmp/python3
COPY --from=ignisframework/python-builder/root/pythrift /tmp/pythrift


RUN cd /tmp/python3 && \
	./install && \
	cd /tmp/pythrift && \
	./install && \
	cd /tmp && \
	rm -fR /tmp/python3 && \
	rm -fR /tmp/pythrift && \
	curl https://bootstrap.pypa.io/get-pip.py | python3 && \
	pip3 install setuptools --upgrade

RUN cd ${IGNIS_HOME}/core && \
	git clone https://ignis-ro:ghqvbdWcAfpDTLwLHgSf@gitlab.com/ignis-framework/ignis/core-python.git python && \
	cd python && \
	python3 setup.py develop