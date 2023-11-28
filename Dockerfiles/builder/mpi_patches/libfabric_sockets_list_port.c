
        char* mpich_service = getenv("MPICH_SERVICE");
        SOCKET sock = listen_fd;
        void* src_addr = &addr.sa;
        size_t addrlen = ofi_sizeofaddr(&addr.sa);
        struct sockaddr_in public_addr; 
        if(mpich_service != NULL){
            public_addr = *((struct sockaddr_in *)src_addr);
            public_addr.sin_addr.s_addr = INADDR_ANY;
            src_addr = &public_addr;
            addrlen = sizeof(public_addr);
        }

        ret = 0;
        char* port_list = getenv("MPICH_LIST_PORTS");
        if (port_list != NULL){
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
                    FI_WARN(&sock_prov, FI_LOG_EP_CTRL, "Invalid character %c in %s\n", *p, "MPICH_LIST_PORTS");
                    ret = -FI_EADDRNOTAVAIL;
                }
                
                ofi_addr_set_port(src_addr, i);
                ofi_addr_set_port(&addr.sa, i);
                ret = bind(sock, src_addr, (socklen_t) addrlen);
                if (ret) {
                    if (ofi_sockerr() == EADDRINUSE){
                        continue;
                    }

                    FI_WARN(&sock_prov, FI_LOG_EP_CTRL,
                        "failed to bind listener: %s\n",
                        strerror(ofi_sockerr()));
                    ret = -ofi_sockerr();
                }
                break;
            }
        }else {
            ret = bind(listen_fd, &addr.sa, ofi_sizeofaddr(&addr.sa));
        }
