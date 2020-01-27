#!/bin/bash
cd "$(dirname "$0")"

cd base
	docker build -t ignishpc/base .
cd ..

cd builder
	docker build -t ignishpc/builder .
cd ..

cd driver-builder
	docker build -t ignishpc/driver-builder .
cd ..

cd executor-builder
	docker build -t ignishpc/executor-builder .
cd ..

cd base-driver
	docker build -t ignishpc/base-driver .
cd ..

cd base-executor
	docker build -t ignishpc/base-executor .
cd ..

cd base-full
	docker build -t ignishpc/base-full .
cd ..

cd cpp
	cd builder
		docker build -t ignishpc/cpp-builder .
	cd ..
	cd compiler
		docker build -t ignishpc/cpp-compiler .
	cd ..
	cd driver
		docker build -t ignishpc/cpp-driver .
	cd ..
	cd executor
		docker build -t ignishpc/cpp-executor .
	cd ..
	cd full
		docker build -t ignishpc/cpp-full .
	cd ..
cd ..

cd full
	docker build -t ignishpc/full .
cd ..

cd submitter
	docker build -t ignishpc/submitter .
cd ..

cd mesos
	docker build -t ignishpc/mesos .
cd ..

cd zookeeper
	docker build -t ignishpc/zookeeper .
cd ..


