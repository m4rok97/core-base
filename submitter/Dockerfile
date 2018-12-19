
FROM ignisframework/base-builder as builder

ARG SOURCE_CACHE=1
RUN git clone https://ignis-ro:ghqvbdWcAfpDTLwLHgSf@gitlab.com/ignis-framework/ignis/utils.git /tmp/utils && \
	mv /tmp/utils/submit/* ${IGNIS_HOME}/submitter && \
	wget https://bootstrap.pypa.io/get-pip.py -O /tmp/get-pip.py

FROM ignisframework/base-skel

RUN apt update -y && \
	apt install -y \
		nano \
		openssh-server \
		python3 \
		python3-distutils  \
		python3.6-dev  \
		ca-certificates

COPY --from=builder /tmp/get-pip.py /tmp/get-pip.py
RUN python3 /tmp/get-pip.py && \
	rm -f /tmp/get-pip.py 

RUN ssh-keygen -A && \ 
	echo "root:root" | chpasswd
	#chage -d 0 root

RUN echo "source /etc/environment" >> /etc/profile && \  
	echo "source /etc/environment" >> /etc/bashrc && \  
	{ \
		echo '#!/bin/bash'; \
		echo 'EXCLUDE="HOSTNAME|TERM|LS_COLORS|PWD|SHLVL|HOME|_|OLDPWD"'; \
		echo 'rm -f /etc/environment'; \
		echo 'for key in `env | cut -f 1 -d =`; do'; \
		echo 'if ! [[ $key =~ ^($EXCLUDE)$ ]]; then'; \
		echo '    echo "export $key=\"$(printenv $key)\"";'; \ 
		echo '  fi';\
		echo 'done > /etc/environment'; \
		echo '/usr/sbin/sshd -D'; \
	} > ${IGNIS_HOME}/bin/submitter && chmod +x ${IGNIS_HOME}/bin/submitter
	

COPY --from=builder ${IGNIS_HOME} ${IGNIS_HOME}

RUN pip3 install requests && \
	{ \
		echo '#!/bin/bash'; \
		echo 'python3 ${IGNIS_HOME}/submitter/ignis/submit/Submit.py "$@"'; \
	} > ${IGNIS_HOME}/bin/ignis-submit  && chmod +x ${IGNIS_HOME}/bin/ignis-submit
