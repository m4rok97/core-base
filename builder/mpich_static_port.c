int isspace(int argument);
int isdigit(int argument);
int MPL_env2str(const char *envName, const char **val);
int MPL_listen_anyport_aux(int sock_fd, unsigned short *p_port);
#include<stdio.h> 
int MPL_listen_anyport(int socket, unsigned short *p_port)
{
    const char* static_ports;
    if (MPL_env2str("MPICH_STATIC_PORTS", &static_ports)){ 
        MPL_sockaddr_t addr;
        int i;
        int ret;
        char *p = (char*)static_ports;

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
                fprintf(stderr,"Invalid character %c in %s\n", *p, "MPICH_STATIC_PORTS");
            }
            ret = MPL_listen(socket, i);
            if (ret == 0) {
                *p_port = i;
                return listen(socket, _max_conn);
            } else if (errno == EADDRINUSE) {
                continue;
            } else {
                return -1;
            }
        }
        fprintf(stderr,"not enough ports in %s\n", "MPICH_STATIC_PORTS");
        return -2;
    }
    return MPL_listen_anyport_aux(socket, p_port);
}

int MPL_listen_anyport_aux(int socket, unsigned short *p_port)