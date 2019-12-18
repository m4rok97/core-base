#!/bin/bash

echo ${IGNIS_HOME}/lib/native > /etc/ld.so.conf.d/ignis-lib.conf
ldconfig
apt update 
apt install -y openssh-server curl
mkdir /var/run/sshd
sed -i 's/#PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config

cat > ${IGNIS_HOME}/bin/ignis-executor << 'EOL'
#!/bin/bash
export -p >> /etc/profile
mkdir -p ~/.ssh
echo ${IGNIS_DRIVER_PUBLIC_KEY} >> ~/.ssh/authorized_keys
/usr/sbin/sshd

interval=${IGNIS_DRIVER_HEALTHCHECK_INTERVAL}
timeout=${IGNIS_DRIVER_HEALTHCHECK_TIMEOUT}
retries=${IGNIS_DRIVER_HEALTHCHECK_RETRIES}
url=${IGNIS_DRIVER_HEALTHCHECK_URL}
attempt=0
while true; do
    if [ -f "${IGNIS_HOME}/exit" ]; then
        exit 0
    fi
    if $(curl --output /dev/null --silent --head --fail --connect-timeout ${timeout} ${url}); then
        attempt=0
    else
        if [ ${attempt} -eq ${retries} ];then
            echo "Driver lost, exiting"
            exit 1
        fi
        attempt=$(($attempt+1))
    fi
    sleep ${interval}
done

EOL
chmod +x ${IGNIS_HOME}/bin/ignis-executor
