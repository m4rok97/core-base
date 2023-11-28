/*
 * Copyright (C) by Argonne National Laboratory
 *     See COPYRIGHT in top-level directory
 */

#ifndef CH4_VCI_H_INCLUDED
#define CH4_VCI_H_INCLUDED

#include "ch4_impl.h"

/* vci is embedded in the request's pool index */

#define MPIDI_Request_get_vci(req) MPIR_REQUEST_POOL(req)

/* VCI hashing function (fast path)
 * NOTE: The returned vci should always MOD NUMVCIS, where NUMVCIS is
 *       the number of VCIs determined at init time
 *       Potentially, we'd like to make it config constants of power of 2
 * TODO: move the MOD here.
 */

/* For consistent hashing, we may need differentiate between src and dst vci and whether
 * it is being called from sender side or receiver side (consdier intercomm). We use an
 * integer flag to encode the information.
 *
 * The flag constants are designed as bit fields, so different hashing algorithm can easily
 * pick the needed information.
 */
#define SRC_VCI_FROM_SENDER 0   /* Bit 1 marks SRC/DST */
#define DST_VCI_FROM_SENDER 1
#define SRC_VCI_FROM_RECVER 2   /* Bit 2 marks SENDER/RECVER */
#define DST_VCI_FROM_RECVER 3

/*Ignis vci implementation*/
#define IGNIS_MASK "ignis_thread_"

MPL_STATIC_INLINE_PREFIX int MPIDI_get_vci(int flag, MPIR_Comm * comm_ptr, int src_rank, int dst_rank, int tag){
    char* name = comm_ptr->name;
    int mask_len = strlen(IGNIS_MASK);
    if(memcmp(name, IGNIS_MASK, mask_len) == 0){
        name += mask_len;
        char* ptr;
        long id = strtol(name, &ptr, 10);
        if(ptr = '\0'){
            return (int)id;
        }
    }
    return 0;
}


#endif /* CH4_VCI_H_INCLUDED */
