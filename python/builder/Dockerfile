
FROM ignishpc/builder

COPY python-core-install.sh ${IGNIS_HOME}/bin
RUN chmod +x ${IGNIS_HOME}/bin/python-core-install.sh && \
	wget https://bootstrap.pypa.io/get-pip.py -O ${IGNIS_HOME}/bin/get-pip.py && \
	{ \
		echo '#!/bin/bash'; \
		echo 'exec python3 "${IGNIS_HOME}/core/python/ignis/Main.py" "$@"'; \
	} > ${IGNIS_HOME}/bin/ignis-python  && chmod +x ${IGNIS_HOME}/bin/ignis-python

ARG SOURCE_CACHE=1
RUN git clone https://ignis:1G3xNTVoEnxm6GsJDFWe@gitlab.com/ignis-hpc/core-python.git ${IGNIS_HOME}/core/python
