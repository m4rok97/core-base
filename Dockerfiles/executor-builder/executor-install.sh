#!/bin/bash

export DEBIAN_FRONTEND=noninteractive && \
	apt update && \
	apt -y --no-install-recommends install \
		tar \
		unzip \
		bzip2 \
		gzip \
		xz-utils && \
	rm -rf /var/lib/apt/lists/*
