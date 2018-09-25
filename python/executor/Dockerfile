
FROM ignisframework/python-builder as builder

ARG SOURCE_CACHE=1
RUN git clone https://ignis-ro:ghqvbdWcAfpDTLwLHgSf@gitlab.com/ignis-framework/ignis/core-python.git ${IGNIS_HOME}/core/python


FROM ignisframework/base-driver 

COPY --from=builder ${IGNIS_HOME} ${IGNIS_HOME}
COPY --from=builder /root/python3 /tmp/python3
COPY --from=builder /root/pythrift /tmp/pythrift


RUN cd /tmp/python3 && \
	./install && \
	cd /tmp/pythrift && \
	./install && \
	cd /tmp && \
	rm -fR /tmp/python3 && \
	rm -fR /tmp/pythrift && \
	curl https://bootstrap.pypa.io/get-pip.py | python3 && \
	pip3 install setuptools --upgrade

RUN { \
		echo '#!/bin/bash'; \
		echo 'python3 "${IGNIS_HOME}/core/python/ignis/main.py" "$@"'; \
	} > bin/ignis-py  && chmod +x bin/ignis-py