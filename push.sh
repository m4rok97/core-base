#!/bin/bash

tag=":latest"
if [ $# -ge 1 ]; then
    tag=":$1"
fi

docker push ignishpc/base
docker push ignishpc/builder
docker push ignishpc/driver-builder
docker push ignishpc/executor-builder
docker push ignishpc/base-driver
docker push ignishpc/base-executor
docker push ignishpc/base-full
docker push ignishpc/cpp-builder
docker push ignishpc/cpp-driver
docker push ignishpc/cpp-executor
docker push ignishpc/cpp-full
docker push ignishpc/full
docker push ignishpc/submitter
docker push ignishpc/mesos
docker push ignishpc/zookeeper
