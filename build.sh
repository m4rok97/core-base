#!/bin/bash
cd "$(dirname "$0")"

tag=":latest"
rty=""
if [ $# -ge 1 ]; then
    rty=":$1"
fi

if [ $# -ge 2 ]; then
	rty="$2/"
fi

cd base
	docker build -t ${rty}ignishpc/base${tag} --build-arg REGISTRY=${rty} --build-arg TAG=${tag} .
cd ..

cd builder
	docker build -t ${rty}ignishpc/builder${tag} --build-arg REGISTRY=${rty} --build-arg TAG=${tag} .
cd ..

cd driver-builder
	docker build -t ${rty}ignishpc/driver-builder${tag} --build-arg REGISTRY=${rty} --build-arg TAG=${tag} .
cd ..

cd executor-builder
	docker build -t ${rty}ignishpc/executor-builder${tag} --build-arg REGISTRY=${rty} --build-arg TAG=${tag} .
cd ..

cd base-driver
	docker build -t ${rty}ignishpc/base-driver${tag} --build-arg REGISTRY=${rty} --build-arg TAG=${tag} .
cd ..

cd base-executor
	docker build -t ${rty}ignishpc/base-executor${tag} --build-arg REGISTRY=${rty} --build-arg TAG=${tag} .
cd ..

cd base-full
	docker build -t ${rty}ignishpc/base-full${tag} --build-arg REGISTRY=${rty} --build-arg TAG=${tag} .
cd ..

cd cpp
	cd builder
		docker build -t ${rty}ignishpc/cpp-builder${tag} --build-arg REGISTRY=${rty} --build-arg TAG=${tag} .
	cd ..
	cd compiler
		docker build -t ${rty}ignishpc/cpp-compiler${tag} --build-arg REGISTRY=${rty} --build-arg TAG=${tag} .
	cd ..
	cd driver
		docker build -t ${rty}ignishpc/cpp-driver${tag} --build-arg REGISTRY=${rty} --build-arg TAG=${tag} .
	cd ..
	cd executor
		docker build -t ${rty}ignishpc/cpp-executor${tag} --build-arg REGISTRY=${rty} --build-arg TAG=${tag} .
	cd ..
	cd full
		docker build -t ${rty}ignishpc/cpp-full${tag} --build-arg REGISTRY=${rty} --build-arg TAG=${tag} .
	cd ..
cd ..

cd python
	cd builder
		docker build -t ${rty}ignishpc/python-builder${tag} --build-arg REGISTRY=${rty} --build-arg TAG=${tag} .
	cd ..
	cd driver
		docker build -t ${rty}ignishpc/python-driver${tag} --build-arg REGISTRY=${rty} --build-arg TAG=${tag} .
	cd ..
	cd executor
		docker build -t ${rty}ignishpc/python-executor${tag} --build-arg REGISTRY=${rty} --build-arg TAG=${tag} .
	cd ..
	cd full
		docker build -t ${rty}ignishpc/python-full${tag} --build-arg REGISTRY=${rty} --build-arg TAG=${tag} .
	cd ..
cd ..

cd full
	docker build -t ${rty}ignishpc/full${tag} --build-arg REGISTRY=${rty} --build-arg TAG=${tag} .
cd ..

cd submitter
	docker build -t ${rty}ignishpc/submitter${tag} --build-arg REGISTRY=${rty} --build-arg TAG=${tag} .
cd ..

cd mesos
	docker build -t ${rty}ignishpc/mesos${tag} .
cd ..

cd zookeeper
	docker build -t ${rty}ignishpc/zookeeper${tag} .
cd ..


