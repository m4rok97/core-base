static int tcpx_bind_to_port_range_aux(SOCKET sock, void* src_addr, size_t addrlen);
int isspace(int argument);
int isdigit(int argument);

static int tcpx_bind_to_port_range(SOCKET sock, void* src_addr, size_t addrlen){
	char* static_ports;
	if(fi_param_get_str(&tcpx_prov, "static_ports", &static_ports) != FI_SUCCESS){
		return tcpx_bind_to_port_range_aux(sock, src_addr, addrlen);
	}
	int i;
    int ret;
    char *p = (char*)static_ports;

	while(*p) {
		i = 0;
		while (*p && isspace(*p)){
			p++;
		}
		while (*p && isdigit(*p)){
			i = 10 * i + (*p++ - '0');
		}
		if(i==0){
			FI_WARN(&tcpx_prov, FI_LOG_EP_CTRL, "Invalid character %c in %s\n", *p, "FI_TCP_STATIC_PORTS");
			return -FI_EADDRNOTAVAIL;
		}
		
		ofi_addr_set_port(src_addr, i);
		ret = bind(sock, src_addr, (socklen_t) addrlen);
		if (ret) {
			if (ofi_sockerr() == EADDRINUSE){
				continue;
			}

			FI_WARN(&tcpx_prov, FI_LOG_EP_CTRL,
				"failed to bind listener: %s\n",
				strerror(ofi_sockerr()));
			return -ofi_sockerr();
		}
	}
	return FI_SUCCESS;
}

static int tcpx_bind_to_port_range_aux(SOCKET sock, void* src_addr, size_t addrlen)