
int MPL_env2str(const char *envName, const char **val);
int MPL_listen_anyport_aux(int sock_fd, unsigned short *p_port);

int MPL_listen_anyport(int sock_fd, unsigned short *p_port)
{
    const char* list_ports;
    if (MPL_env2str("MPICH_LIST_PORTS", &list_ports)){ 
        MPL_sockaddr_t addr;
        int i;
        int ret;
        char *p = (char*)list_ports;

        if (_use_loopback) {
            MPL_get_sockaddr_direct(MPL_SOCKADDR_LOOPBACK, &addr);
        } else {
            MPL_get_sockaddr_direct(MPL_SOCKADDR_ANY, &addr);
        }
        while(*p) {
            i = 0;
            while (*p && isspace(*p)){
                p++;
            }
            while (*p && isdigit(*p)){
                i = 10 * i + (*p++ - '0');
            }
            if(i==0){
                fprintf(stderr,"Invalid character %c in %s\n", *p, "MPICH_LIST_PORTS");
                return -1;
            }

            if (af_type == AF_INET) {
                ((struct sockaddr_in *) &addr)->sin_port = htons(i);
                ret = bind(sock_fd, (const struct sockaddr *) &addr, sizeof(struct sockaddr_in));
            } else if (af_type == AF_INET6) {
                ((struct sockaddr_in6 *) &addr)->sin6_port = htons(i);
                ret = bind(sock_fd, (const struct sockaddr *) &addr, sizeof(struct sockaddr_in6));
            }

            if (ret == 0) {
                *p_port = i;
                return listen(sock_fd, _max_conn);
            } else if (errno == EADDRINUSE) {
                continue;
            } else {
                fprintf(stderr,"failed to bind, code %d: %s\n", errno, strerror(errno));
                return -1;
            }
        }
        fprintf(stderr,"not enough ports in %s\n", "MPICH_LIST_PORTS");
        return -2;
    }
    return MPL_listen_anyport_aux(sock_fd, p_port);
}

int MPL_listen_anyport_aux(int sock_fd, unsigned short *p_port)