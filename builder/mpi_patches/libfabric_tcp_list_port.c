
    char* mpich_service = getenv("MPICH_SERVICE");
    SOCKET sock = pep->sock;
    void* src_addr = pep->info->src_addr;
    size_t addrlen = pep->info->src_addrlen;
    struct sockaddr_in public_addr; 
    if(pep->info->addr_format == FI_SOCKADDR_IN && mpich_service != NULL){
        public_addr = *((struct sockaddr_in *)src_addr);
        public_addr.sin_addr.s_addr = INADDR_ANY;
        src_addr = &public_addr;
        addrlen = sizeof(public_addr);
    }


    char* port_list = getenv("MPICH_LIST_PORTS");
    if (ofi_addr_get_port(pep->info->src_addr) == 0 && port_list != NULL){
        int i;
        char *p = port_list;

        while(*p) {
            i = 0;
            while (*p && isspace(*p)){
                p++;
            }
            while (*p && isdigit(*p)){
                i = 10 * i + (*p++ - '0');
            }
            if(i == 0){
                FI_WARN(&tcpx_prov, FI_LOG_EP_CTRL, "Invalid character %c in %s\n", *p, "MPICH_LIST_PORTS");
                ret = -FI_EADDRNOTAVAIL;
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
                ret = -ofi_sockerr();
            }
            break;
        }
    }else if (ofi_addr_get_port(pep->info->src_addr) != 0 || port_range.high == 0)
