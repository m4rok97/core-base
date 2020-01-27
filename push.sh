#!/bin/bash

tag=":latest"
if [ $# -ge 1 ]; then
    tag=":$1"
fi

docker push ignishpc/base$tag
docker push ignishpc/builder$tag
docker push ignishpc/driver-builder$tag
docker push ignishpc/executor-builder$tag
docker push ignishpc/base-driver$tag
docker push ignishpc/base-executor$tag
docker push ignishpc/base-full$tag
docker push ignishpc/cpp-builder$tag
docker push ignishpc/cpp-compiler$tag
docker push ignishpc/cpp-driver$tag
docker push ignishpc/cpp-executor$tag
docker push ignishpc/cpp-full$tag
docker push ignishpc/full$tag
docker push ignishpc/submitter$tag
docker push ignishpc/mesos$tag
docker push ignishpc/zookeeper$tag
