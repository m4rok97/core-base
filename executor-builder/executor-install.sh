#!/bin/bash

ldconfig
export DEBIAN_FRONTEND=noninteractive
apt update 
apt install -y --no-install-recommends openssh-server curl tzdata 
mkdir /var/run/sshd
sed -i 's/#PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config

cat > ${IGNIS_HOME}/bin/ignis-server << 'EOL'
#!/bin/bash
export -p >> /etc/profile
export -p | sed 's/^declare -x //' > /etc/environment
mkdir -p ~/.ssh
echo ${IGNIS_DRIVER_PUBLIC_KEY} >> ~/.ssh/authorized_keys
/usr/sbin/sshd -p $1

interval=${IGNIS_DRIVER_HEALTHCHECK_INTERVAL}
timeout=${IGNIS_DRIVER_HEALTHCHECK_TIMEOUT}
retries=${IGNIS_DRIVER_HEALTHCHECK_RETRIES}
url=${IGNIS_DRIVER_HEALTHCHECK_URL}
attempt=0
trap "exit 0" TERM 
while true; do
    if $(curl --output /dev/null --silent --head --fail --connect-timeout ${timeout} ${url}); then
        attempt=0
    else
        if [ ${attempt} -eq ${retries} ];then
            echo "Driver lost, exiting" 1>&2
            exit 1
        fi
        attempt=$(($attempt+1))
    fi
    sleep ${interval} &
    wait $!
done

EOL
chmod +x ${IGNIS_HOME}/bin/ignis-server
