int MPIDIU_get_node_id(MPIR_Comm * comm, int rank, int *id_p)
{
    int mpi_errno = MPI_SUCCESS;
    int avtid = 0, lpid = 0;

    MPIR_FUNC_VERBOSE_STATE_DECL(MPID_STATE_MPIDIU_GET_NODE_ID);
    MPIR_FUNC_VERBOSE_ENTER(MPID_STATE_MPIDIU_GET_NODE_ID);

    MPIDIU_comm_rank_to_pid(comm, rank, &lpid, &avtid);
    if (avtid != 0) {
        *id_p = -1;
    } else {
        *id_p = MPIDI_global.node_map[avtid][lpid];
    }

    MPIR_FUNC_VERBOSE_EXIT(MPID_STATE_MPIDIU_GET_NODE_ID);
    return mpi_errno;
}

int MPIDIU_get_node_id_bugged(MPIR_Comm * comm, int rank, int *id_p)