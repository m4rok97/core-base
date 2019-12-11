
FROM ignishpc/builder

COPY cpp-core-install.sh ${IGNIS_HOME}/bin
RUN chmod +x ${IGNIS_HOME}/bin/cpp-core-install.sh

RUN apt update && \
	apt -y install \
		rapidjson-dev \
		automake  \
		libevent-dev \
		zlib1g-dev  \
		libssl-dev \
		libtool \
		pkg-config 

RUN mv /root/thrift /root/thrift-src && \
	cd /root/thrift-src && \
	./bootstrap.sh && \
	./configure --with-boost=/root/boost --enable-tests=no --prefix /root/thrift && \
	cd lib/cpp && \
	make -j 4 && \
	make install && \
	rm -fR /root/thrift-src

RUN cd ${IGNIS_HOME} && \
	mkdir -p ${IGNIS_HOME}/core/cpp/include && \
	mkdir -p ${IGNIS_HOME}/lib/native && \
	cp -R /root/boost/include/* /usr/local/include && \
	cp -R /root/boost/lib/* lib/native/ && \
	cp -R /root/thrift/include/* core/cpp/include/ && \
	cp -R /root/thrift/lib/* lib/native/ && \
	cp -R /root/mpi/include/* core/cpp/include/ && \
	cp -R /root/mpi/lib/* lib/native/ && \
	cp -R /usr/include/rapidjson core/cpp/include/ && \
	rm -f lib/native/*.a 

ARG SOURCE_CACHE=1
RUN mkdir -p /tmp/core-cpp && \ 
	cd /tmp/core-cpp && \
	git clone https://ignis:MbduRCadGPXp3pJMvwK_@gitlab.com/ignis-hpc/core-cpp.git /tmp/core-cpp && \
	export CC=/root/mpi/bin/mpicc && \
	export CXX=/root/mpi/bin/mpic++ && \
	export LIBRARY_PATH=${IGNIS_HOME}/lib/native && \
	export CPATH=${IGNIS_HOME}/core/cpp/include && \
	cmake -DBUILD_TESTING=OFF -DCMAKE_INSTALL_PREFIX=${IGNIS_HOME}/core/cpp/ . && \
	make -j 4 && \
	make install && \
	rm -fr /tmp/core-cpp && \
	cd ${IGNIS_HOME} && \
	mv core/cpp/bin/ignis-cpp bin/ && \
	chmod +x bin/ignis-cpp && \
	mv core/cpp/lib lib/native && \
	rm -fR core/cpp/bin



