#!/bin/bash

dir=$(dirname "$0")
comm_headers=$(cat << EOF
	#include <ctype.h>
EOF
)

######## MPICH VCI ########
cat "$dir/ignis_ch4_vci.h" > "src/mpid/ch4/src/ch4_vci.h"

######## MPICH SERVICE PATH ########
file_path="src/mpid/ch4/netmod/ofi/ofi_init.c"
position='NULL, NULL, 0ULL, hints, \&prov_list'
source='getenv("MPICH_SERVICE"), NULL, getenv("MPICH_SERVICE") == NULL ? 0ULL : FI_SOURCE, hints, \&prov_list'

sed  "s/$position/$source/g" -i $file_path


######## MPICH LIST PORT PATH ########
file_path=$(find . -name mpl_sockaddr.c)
position=".*MPL_listen_anyport.*"
source="mpich_list_port.c"

sed "1 s/^/$comm_headers\n/" -i $file_path
sed -e "/$position/ {" -e "r $dir/$source" -e "d" -e "}" -i $file_path


######## LIBFABRIC TCP PORTS ########
file_path=$(find . -name tcpx_ep.c)
position=".*ofi_addr_get_port.*"
source="libfabric_tcp_list_port.c"

sed "1 s/^/$comm_headers\n/" -i $file_path
sed -e "/$position/ {" -e "r $dir/$source" -e "d" -e "}" -i $file_path


######## LIBFABRIC SOCKETS PORTS ########
file_path=$(find . -name sock_conn.c)
position=".*ret = bind.*"
source="libfabric_sockets_list_port.c"

sed "1 s/^/$comm_headers\n/" -i $file_path
sed -e "/$position/ {" -e "r $dir/$source" -e "d" -e "}" -i $file_path

position="ofi_getsockname"
source="mpich_service != NULL ? 0 : ofi_getsockname"
sed  "s/$position/$source/g" -i $file_path


