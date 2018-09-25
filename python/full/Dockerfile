
FROM ignisframework/base-full

COPY --from=ignisframework/python-executor ${IGNIS_HOME} ${IGNIS_HOME}

COPY --from=ignisframework/python-builder /root/python3 /tmp/python3
COPY --from=ignisframework/python-builder /root/pythrift /tmp/pythrift

RUN /tmp/python3/install && \
	/tmp/pythrift/install && \
	rm -fR /tmp/python3 && \
	rm -fR /tmp/pythrift && \
	curl https://bootstrap.pypa.io/get-pip.py | python3 && \
	pip3 install setuptools --upgrade

RUN cd ${IGNIS_HOME}/core/python && \
	python3 setup.py develop

