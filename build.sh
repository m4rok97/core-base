#!/bin/bash
cd "$(dirname "$0")"

cd base
	cd skel
		docker build -t ignisframework/base-skel .
	cd ..
	cd builder
		docker build -t ignisframework/base-builder .
	cd ..
	cd driver
		docker build -t ignisframework/base-driver .
	cd ..
	cd executor
		docker build -t ignisframework/base-executor .
	cd ..
	cd full
		docker build -t ignisframework/base-full .
	cd ..
cd ..

cd cpp
	cd driver
		docker build -t ignisframework/cpp-driver .
	cd ..
	cd executor
		docker build -t ignisframework/cpp-executor .
	cd ..
	cd full
		docker build -t ignisframework/cpp-full .
	cd ..
cd ..

cd python
	cd driver
		docker build -t ignisframework/python-driver .
	cd ..
	cd executor
		docker build -t ignisframework/python-executor .
	cd ..
	cd full
		docker build -t ignisframework/python-full .
	cd ..
cd ..

cd submitter
	docker build -t ignisframework/submitter .

