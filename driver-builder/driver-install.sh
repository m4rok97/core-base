#!/bin/bash

apt update 
apt -y install openjdk-8-jdk openssl nginx
echo ${IGNIS_HOME}/lib/native > /etc/ld.so.conf.d/ignis-lib.conf
ldconfig
#Health check
cat > /etc/nginx/sites-enabled/default << 'EOL'
server {
    listen 8080 default_server;

    location / {
        access_log off;
        return 200 "Ok\n";
    }
}
EOL
