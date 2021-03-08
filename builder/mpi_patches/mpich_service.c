    char buffer[] = {0, 0, 0, 0, 0, 0, 0};
    char *port = NULL;
    const char *static_ports;
    int flag = 0;
    if (MPL_env2str("MPICH_STATIC_PORTS", &static_ports) && getenv("MPICH_HOST") != NULL) {
        MPL_sockaddr_t addr;
        int sock_fd = MPL_socket();
        int i;
        int ret;
        char *p = (char *) static_ports;

        while (*p) {
            i = 0;
            while (*p && isspace(*p)) {
                p++;
            }
            while (*p && isdigit(*p)) {
                i = 10 * i + (*p++ - '0');
            }
            if (i == 0) {
                fprintf(stderr, "Invalid character %c in %s\n", *p, "MPICH_STATIC_PORTS");
                break;
            }
            ret = MPL_listen(sock_fd, i);
            if (ret == 0) {
                snprintf(buffer, 7, "%u", i);
                port = buffer;
                flag = FI_SOURCE;
                break;
            } else if (errno == EADDRINUSE) {
                continue;
            } else {
                break;
            }
        }
        close(sock_fd);
        if (port == NULL) {
            fprintf(stderr, "not enough ports in %s\n", "MPICH_STATIC_PORTS");
        }
    }    
    MPIDI_OFI_CALL(fi_getinfo(get_ofi_version(), getenv("MPICH_HOST"), port, 0ULL, hints, &prov_list), getinfo);