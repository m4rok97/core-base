#!/bin/bash

tag=":latest"
rty=""
if [ $# -ge 1 ]; then
    tag=":$1"
fi

if [ $# -ge 2 ]; then
	rty="$2/"
fi

docker push ${rty}ignishpc/base${tag}
docker push ${rty}ignishpc/builder${tag}
docker push ${rty}ignishpc/driver-builder${tag}
docker push ${rty}ignishpc/executor-builder${tag}
docker push ${rty}ignishpc/base-driver${tag}
docker push ${rty}ignishpc/base-executor${tag}
docker push ${rty}ignishpc/base-full${tag}
docker push ${rty}ignishpc/cpp-builder${tag}
docker push ${rty}ignishpc/cpp-compiler${tag}
docker push ${rty}ignishpc/cpp-driver${tag}
docker push ${rty}ignishpc/cpp-executor${tag}
docker push ${rty}ignishpc/cpp-full${tag}
docker push ${rty}ignishpc/python-builder${tag}
docker push ${rty}ignishpc/python-driver${tag}
docker push ${rty}ignishpc/python-executor${tag}
docker push ${rty}ignishpc/python-full${tag}
docker push ${rty}ignishpc/full${tag}
docker push ${rty}ignishpc/submitter${tag}
docker push ${rty}ignishpc/mesos-base${tag}
docker push ${rty}ignishpc/mesos-marathon${tag}
docker push ${rty}ignishpc/mesos-singularity${tag}
docker push ${rty}ignishpc/zookeeper${tag}
