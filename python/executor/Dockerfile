
FROM ignisframework/base-builder as builder

ARG SOURCE_CACHE=1
RUN git clone https://ignis-ro:ghqvbdWcAfpDTLwLHgSf@gitlab.com/ignis-framework/ignis/core-python.git ${IGNIS_HOME}/core/python && \
	wget https://bootstrap.pypa.io/get-pip.py -O /tmp/get-pip.py


FROM ignisframework/base-executor 

COPY --from=builder /tmp/get-pip.py /tmp/get-pip.py
RUN apt -y install python3  python3-distutils python3.6-dev ca-certificates && \
	python3 /tmp/get-pip.py && \
	rm -f /tmp/get-pip.py && \
	pip3 install numpy 

COPY --from=builder ${IGNIS_HOME} ${IGNIS_HOME}
RUN cd ${IGNIS_HOME}/core/python/ && \
	python3 setup.py develop && \
	{ \
		echo '#!/bin/bash'; \
		echo 'exec python3 "${IGNIS_HOME}/core/python/ignis/Main.py" "$@"'; \
	} > ${IGNIS_HOME}/bin/ignis-py  && chmod +x ${IGNIS_HOME}/bin/ignis-py