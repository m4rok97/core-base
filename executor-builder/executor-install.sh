#!/bin/bash

echo ${IGNIS_HOME}/lib/native > /etc/ld.so.conf.d/ignis-lib.conf
ldconfig
apt update 
apt install -y openssh-server
mkdir /var/run/sshd
sed -i 's/#PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config

cd ${IGNIS_HOME}/bin
{ 
	echo '#!/bin/bash'; 
	echo 'export -p >> /etc/profile'; 
	echo 'mkdir -p ~/.ssh'; 
	echo 'echo ${DRIVER_PUBLIC_KEY} >> ~/.ssh/authorized_keys'; 
	echo '/usr/sbin/sshd -D'; 
} > ignis-sshd  
chmod +x ignis-sshd
