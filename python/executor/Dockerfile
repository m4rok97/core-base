
FROM ignisframework/python-builder as builder

ARG SOURCE_CACHE=1
RUN git clone https://ignis-ro:ghqvbdWcAfpDTLwLHgSf@gitlab.com/ignis-framework/ignis/core-python.git ${IGNIS_HOME}/core/python


FROM ignisframework/base-executor 

COPY --from=builder ${IGNIS_HOME} ${IGNIS_HOME}
COPY --from=builder /root/python3 /tmp/python3
COPY --from=builder /root/pythrift /tmp/pythrift


RUN /tmp/python3/install && \
	/tmp/pythrift/install && \
	rm -fR /tmp/python3 && \
	rm -fR /tmp/pythrift && \
	pip3 install --upgrade pip && \
	pip3 install setuptools --upgrade && \
	pip3 install cloudpickle && \
	cd ${IGNIS_HOME}/core/python/ && \
	python3 setup.py develop

RUN { \
		echo '#!/bin/bash'; \
		echo 'exec python3 "${IGNIS_HOME}/core/python/ignis/Main.py" "$@"'; \
	} > ${IGNIS_HOME}/bin/ignis-py  && chmod +x ${IGNIS_HOME}/bin/ignis-py