
FROM ignisframework/base-builder as builder

ARG SOURCE_CACHE=1
RUN git clone https://ignis-ro:ghqvbdWcAfpDTLwLHgSf@gitlab.com/ignis-framework/ignis/utils.git /tmp/utils && \
	mv /tmp/utils/submit/* ${IGNIS_HOME}/submitter

FROM ignisframework/base-skel

RUN yum update -y && \
	yum install -y nano openssh-clients openssh-server

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
	

COPY --from=ignisframework/python-builder /root/python3 /tmp/python3
COPY --from=ignisframework/python-builder /root/pythrift /tmp/pythrift
COPY --from=builder ${IGNIS_HOME} ${IGNIS_HOME}

RUN /tmp/python3/install && \
	rm -fr /tmp/python3 && \
	curl https://bootstrap.pypa.io/get-pip.py | python3 && \
	pip3 install setuptools --upgrade && \
	pip3 install requests && \
	/tmp/pythrift/install && \
	rm -fr /tmp/pythrift

RUN { \
		echo '#!/bin/bash'; \
		echo 'python3 ${IGNIS_HOME}/submitter/ignis/submit/Submit.py "$@"'; \
	} > bin/ignis-submit  && chmod +x bin/ignis-submit
