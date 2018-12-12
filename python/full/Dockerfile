
FROM ignisframework/base-full

COPY --from=ignisframework/python-executor ${IGNIS_HOME} ${IGNIS_HOME}

COPY --from=ignisframework/python-builder /root/python3 /tmp/python3
COPY --from=ignisframework/python-builder /root/pythrift /tmp/pythrift

RUN /tmp/python3/install && \
	/tmp/pythrift/install && \
	rm -fR /tmp/python3 && \
	rm -fR /tmp/pythrift && \
	pip3 install --upgrade pip && \
	pip3 install setuptools --upgrade && \
	pip3 install cloudpickle && \
	python3 ${IGNIS_HOME}/core/python/setup.py develop

