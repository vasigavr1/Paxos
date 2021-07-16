#include <od_rdma_gen_util.h>
#include "od_trace_util.h"
#include "cp_util.h"
#include "od_generic_inline_util.h"

atomic_uint_fast64_t committed_glob_sess_rmw_id[GLOBAL_SESSION_NUM];
FILE* client_log[CLIENTS_PER_MACHINE];


void cp_static_assert_compile_parameters()
{
  static_assert(ENABLE_RMWS, "this flag must be set in od_top.h");
  static_assert(RMW_RATIO == 1000, "turn the RMW ratio in od_top.h to 100%");
  static_assert(RMW_ACK_BASE_TS_STALE > RMW_ACK, "assumption used to check if replies are acks");
  static_assert(PAXOS_TS > ALL_ABOARD_TS, "Paxos TS must be bigger than ALL Aboard TS");
  static_assert(!(COMMIT_LOGS && (PRINT_LOGS || VERIFY_PAXOS)), " ");
  static_assert(sizeof(struct key) == KEY_SIZE, " ");
  static_assert(sizeof(struct network_ts_tuple) == TS_TUPLE_SIZE, "");
//  static_assert(sizeof(struct cache_key) ==  KEY_SIZE, "");
//  static_assert(sizeof(cache_meta) == 8, "");

 static_assert(INVALID_RMW == 0, "the initial state of a mica_op must be invalid");
  static_assert(MACHINE_NUM < 16, "the bit_vec vector is 16 bits-- can be extended");

  static_assert(VALUE_SIZE >= 2, "first round of release can overload the first 2 bytes of value");
  //static_assert(VALUE_SIZE > RMW_BYTE_OFFSET, "");
  static_assert(VALUE_SIZE == (RMW_VALUE_SIZE), "RMW requires the value to be at least this many bytes");
  static_assert(MACHINE_NUM <= 255, ""); // for deprecated reasons

  // ACCEPTS


  static_assert(PROP_MES_SIZE <= MTU, "");
  static_assert(PROP_REP_MES_SIZE <= MTU, "");

  // COALESCING
  static_assert(PROP_COALESCE < 256, "");
  static_assert(PROP_COALESCE > 0, "");
  static_assert(ACC_COALESCE > 0, "");
  static_assert(COM_COALESCE > 0, "");

  // NETWORK STRUCTURES

  static_assert(sizeof(cp_prop_t) == PROP_SIZE, "");
  static_assert(sizeof(cp_prop_mes_t) == PROP_MES_SIZE, "");
  static_assert(sizeof(cp_acc_mes_t) == ACC_MES_SIZE, "");
  static_assert(sizeof(cp_acc_t) == ACC_SIZE, "");
  static_assert(PROP_REP_ACCEPTED_SIZE == PROP_REP_LOG_TOO_LOW_SIZE + 1, "");
  static_assert(sizeof(cp_rmw_rep_t) == PROP_REP_ACCEPTED_SIZE, "");
  //static_assert(sizeof(cp_rmw_rep_mes_t) == PROP_REP_MES_SIZE, "");
  static_assert(sizeof(cp_com_t) == COM_SIZE, "");
  // UD- REQS
  static_assert(sizeof(cp_prop_mes_ud_t) == PROP_RECV_SIZE, "");
  static_assert(sizeof(cp_acc_mes_ud_t) == ACC_RECV_SIZE, "");
  static_assert(sizeof(ctx_ack_mes_ud_t) == CTX_ACK_RECV_SIZE, "");


  // we want to have more write slots than credits such that we always know that if a machine fails
  // the pressure will appear in the credits and not the write slots
  //static_assert(PENDING_WRITES > (W_CREDITS * MAX_MES_IN_WRITE), " ");
  // RMWs
  static_assert(!ENABLE_RMWS || LOCAL_PROP_NUM >= SESSIONS_PER_THREAD, "");
  static_assert(GLOBAL_SESSION_NUM < K_64, "global session ids are stored in uint16_t");

  static_assert(!(VERIFY_PAXOS && PRINT_LOGS), "only one of those can be set");
#if VERIFY_PAXOS == 1
  static_assert(EXIT_ON_PRINT == 1, "");
#endif
  static_assert(TRACE_ONLY_CAS + TRACE_ONLY_FA + TRACE_MIXED_RMWS == 1, "");

}

void cp_print_parameters_in_the_start()
{
  emphatic_print(green, ENABLE_ALL_ABOARD ? "ALL-ABOARD PAXOS" : "CLASSIC PAXOS");
  if (ENABLE_ASSERTIONS) {

    printf("MICA OP capacity %ld/%d added padding %d  \n",
           sizeof(mica_op_t), MICA_OP_SIZE, MICA_OP_PADDING_SIZE);

    printf("quorum-num %d \n", QUORUM_NUM);
  }
}

void cp_init_globals()
{
  memset(committed_glob_sess_rmw_id, 0, GLOBAL_SESSION_NUM * sizeof(uint64_t));
}


void dump_stats_2_file(struct stats* st){
    uint8_t typeNo = 0;
    assert(typeNo >=0 && typeNo <=3);
    int i = 0;
    char filename[128];
    FILE *fp;
    double total_MIOPS;
    char* path = "../../results/scattered-results";

    sprintf(filename, "%s/%s_s_%d_v_%d_m_%d_w_%d_r_%d-%d.csv", path,
            "CP",
            SESSIONS_PER_THREAD,
            USE_BIG_OBJECTS == 1 ? ((EXTRA_CACHE_LINES * 64) + BASE_VALUE_SIZE): BASE_VALUE_SIZE,
            MACHINE_NUM, WORKERS_PER_MACHINE,
            WRITE_RATIO,
            machine_id);
    printf("%s\n", filename);
    fp = fopen(filename, "w"); // "w" means that we are going to write on this file
    fprintf(fp, "machine_id: %d\n", machine_id);

    fprintf(fp, "comment: thread ID, total MIOPS,"
            "reads sent, read-replies sent, acks sent, "
            "received read-replies, received reads, received acks\n");
    //for(i = 0; i < WORKERS_PER_MACHINE; ++i){
    //    total_MIOPS = st->total_reqs[i];
    //    fprintf(fp, "client: %d, %.2f, %.2f, %.2f, %.2f, %.2f, %.2f, %.2f, %.2f\n",
    //            i, total_MIOPS, st->total_reqs[i], st->reads_sent[i],
    //            st->r_reps_sent[i], st->acks_sent[i],
    //            st->received_r_reps[i],st->received_reads[i],
    //            st->received_acks[i]);
    //}

    fclose(fp);
}

// If reading CAS rmws out of the trace, CASes that compare against 0 succeed the rest fail
void randomize_op_values(trace_op_t *ops, uint16_t t_id)
{
  if (!ENABLE_CLIENTS) {
    for (uint16_t i = 0; i < MAX_OP_BATCH; i++) {
      if (TRACE_ONLY_FA)
        ops[i].value[0] = 1;
      else if (rand() % 1000 < RMW_CAS_CANCEL_RATIO)
        memset(ops[i].value, 1, VALUE_SIZE);
      else memset(ops[i].value, 0, VALUE_SIZE);
    }
  }
}


/* ---------------------------------------------------------------------------
------------------------------CP WORKER --------------------------------------
---------------------------------------------------------------------------*/
void cp_init_send_fifos(context_t *ctx)
{
  fifo_t *send_fifo = ctx->qp_meta[PROP_QP_ID].send_fifo;
  cp_prop_mes_t *props = (cp_prop_mes_t *) send_fifo->fifo;

  for (uint32_t i = 0; i < PROP_FIFO_SIZE; i++) {
    props[i].m_id = ctx->m_id;
  }

  send_fifo = ctx->qp_meta[ACC_QP_ID].send_fifo;
  cp_acc_mes_t *accs = (cp_acc_mes_t *) send_fifo->fifo;

  for (uint32_t i = 0; i < ACC_FIFO_SIZE; i++) {
    accs[i].m_id = ctx->m_id;
  }

  send_fifo = ctx->qp_meta[COM_QP_ID].send_fifo;
  for (uint32_t i = 0; i < COM_FIFO_SIZE; i++) {
    cp_com_mes_t *com_mes = (cp_com_mes_t *) get_fifo_slot_mod(send_fifo, i);
    com_mes->m_id = ctx->m_id;
    com_mes->opcode = COMMIT_OP;
  }

  ctx_ack_mes_t *ack_send_buf = (ctx_ack_mes_t *) ctx->qp_meta[ACK_QP_ID].send_fifo->fifo;
  assert(ctx->qp_meta[ACK_QP_ID].send_fifo->max_byte_size == CTX_ACK_SIZE * MACHINE_NUM);
  memset(ack_send_buf, 0, ctx->qp_meta[ACK_QP_ID].send_fifo->max_byte_size);
  for (int i = 0; i < MACHINE_NUM; i++) {
    ack_send_buf[i].m_id = ctx->m_id;
    ack_send_buf[i].opcode = OP_ACK;
  }
}


void cp_qp_meta_mfs(context_t *ctx)
{
  mf_t *mfs = calloc(QP_NUM, sizeof(mf_t));

  mfs[PROP_QP_ID].insert_helper = cp_insert_prop_help;
  mfs[PROP_QP_ID].send_helper = send_props_helper;
  mfs[PROP_QP_ID].recv_handler = prop_recv_handler;
  mfs[PROP_QP_ID].recv_kvs = cp_KVS_batch_op_props;


  mfs[RMW_REP_QP_ID].insert_helper = cp_insert_rmw_rep_helper;
  mfs[RMW_REP_QP_ID].send_helper = rmw_prop_rep_helper;
  mfs[RMW_REP_QP_ID].recv_handler = cp_rmw_rep_recv_handler;

  mfs[ACC_QP_ID].insert_helper = cp_insert_acc_help;
  mfs[ACC_QP_ID].send_helper = send_accs_helper;
  mfs[ACC_QP_ID].recv_handler = acc_recv_handler;
  mfs[ACC_QP_ID].recv_kvs = cp_KVS_batch_op_accs;

  //mfs[ACC_REP_QP_ID].insert_helper = cp_insert_acc_rep_helper;
  //mfs[ACC_REP_QP_ID].send_helper = cp_acc_rep_helper;
  //mfs[ACC_REP_QP_ID].recv_handler = cp_acc_rep_recv_handler;

  mfs[COM_QP_ID].insert_helper = cp_insert_com_help;
  mfs[COM_QP_ID].send_helper = cp_send_coms_helper;
  mfs[COM_QP_ID].recv_handler = cp_com_recv_handler;
  mfs[COM_QP_ID].recv_kvs = cp_KVS_batch_op_coms;

  mfs[ACK_QP_ID].send_helper = cp_send_ack_helper;
  mfs[ACK_QP_ID].recv_handler = cp_ack_recv_handler;
  mfs[ACK_QP_ID].send_debug  = cp_send_ack_debug;

  ctx_set_qp_meta_mfs(ctx, mfs);
  free(mfs);
}

void cp_init_qp_meta(context_t *ctx)
{
  per_qp_meta_t *qp_meta = ctx->qp_meta;
  create_per_qp_meta(&qp_meta[PROP_QP_ID], MAX_PROP_WRS,
                     MAX_RECV_PROP_WRS, SEND_BCAST_RECV_BCAST, RECV_REQ,
                     RMW_REP_QP_ID,
                     REM_MACH_NUM, REM_MACH_NUM, PROP_BUF_SLOTS,
                     PROP_RECV_SIZE, PROP_MES_SIZE, ENABLE_MULTICAST, ENABLE_MULTICAST,
                     PROP_SEND_MCAST_QP, 0, PROP_FIFO_SIZE,
                     PROP_CREDITS, PROP_MES_HEADER,
                     "send props", "recv props");

  create_per_qp_meta(&qp_meta[RMW_REP_QP_ID], MAX_RMW_REP_WRS,
                     MAX_RECV_RMW_REP_WRS, SEND_UNI_REP_RECV_UNI_REP, RECV_REPLY,
                     PROP_QP_ID,
                     REM_MACH_NUM, REM_MACH_NUM, RMW_REP_BUF_SLOTS,
                     RMW_REP_RECV_SIZE, RMW_REP_MES_SIZE, false, false,
                     0, 0, RMW_REP_FIFO_SIZE,
                     0, RMW_REP_MES_HEADER,
                     "send rmw_reps", "recv rmw_reps");

  ///
  create_per_qp_meta(&qp_meta[ACC_QP_ID], MAX_ACC_WRS,
                     MAX_RECV_ACC_WRS, SEND_BCAST_RECV_BCAST, RECV_REQ,
                     RMW_REP_QP_ID,
                     REM_MACH_NUM, REM_MACH_NUM, ACC_BUF_SLOTS,
                     ACC_RECV_SIZE, ACC_MES_SIZE, ENABLE_MULTICAST, ENABLE_MULTICAST,
                     ACC_SEND_MCAST_QP, 0, ACC_FIFO_SIZE,
                     ACC_CREDITS, ACC_MES_HEADER,
                     "send accepts", "recv accepts");

  //create_per_qp_meta(&qp_meta[ACC_REP_QP_ID], MAX_ACC_REP_WRS,
  //                   MAX_RECV_ACC_REP_WRS, SEND_UNI_REP_RECV_UNI_REP, RECV_REPLY,
  //                   ACC_QP_ID,
  //                   REM_MACH_NUM, REM_MACH_NUM, ACC_REP_BUF_SLOTS,
  //                   ACC_REP_RECV_SIZE, ACC_REP_MES_SIZE, false, false,
  //                   0, 0, ACC_REP_FIFO_SIZE,
  //                   0, RMW_REP_MES_HEADER,
  //                   "send accepts reps", "recv accepts reps");

  create_per_qp_meta(&qp_meta[COM_QP_ID], MAX_COM_WRS,
                     MAX_RECV_COM_WRS, SEND_BCAST_RECV_BCAST, RECV_REQ,
                     ACK_QP_ID,
                     REM_MACH_NUM, REM_MACH_NUM, COM_BUF_SLOTS,
                     COM_RECV_SIZE, COM_MES_SIZE, ENABLE_MULTICAST, ENABLE_MULTICAST,
                     COM_SEND_MCAST_QP, 0, COM_FIFO_SIZE,
                     COM_CREDITS, COM_MES_HEADER,
                     "send commits", "recv commits");
  ///

  ///
  create_ack_qp_meta(&qp_meta[ACK_QP_ID],
                     COM_QP_ID, REM_MACH_NUM,
                     REM_MACH_NUM, COM_CREDITS);

  cp_qp_meta_mfs(ctx);
  cp_init_send_fifos(ctx);
}

cp_debug_t *init_debug_loop_struct()
{
  cp_debug_t *loop_dbg = calloc(1, sizeof(cp_debug_t));
  if (DEBUG_SESSIONS) {
    loop_dbg->ses_dbg = (struct session_dbg *) malloc(sizeof(struct session_dbg));
    memset(loop_dbg->ses_dbg, 0, sizeof(struct session_dbg));
  }
  return loop_dbg;
}






// Initialize the pending ops struct
cp_ctx_t* cp_set_up_pending_ops(context_t *ctx)
{
  cp_ctx_t *cp_ctx = (cp_ctx_t *) calloc(1, sizeof(cp_ctx_t));
  cp_ctx->com_rob = fifo_constructor(COM_ROB_SIZE, sizeof(cp_com_rob_t),
                                    false, 0, 1);

  cp_ctx->ptrs_to_ops = calloc(1, sizeof(cp_ptrs_to_ops_t));
  uint32_t max_incoming_ops = MAX(MAX_INCOMING_PROP, MAX_INCOMING_ACC);
  cp_ctx->ptrs_to_ops->ptr_to_ops = calloc(max_incoming_ops, sizeof(void*));
  cp_ctx->ptrs_to_ops->ptr_to_mes = calloc(max_incoming_ops, sizeof(void*));
  cp_ctx->ptrs_to_ops->break_message = calloc(max_incoming_ops, sizeof(bool));


  cp_ctx->stall_info.stalled = (bool *) calloc(SESSIONS_PER_THREAD, sizeof(bool));
  for (uint32_t i = 0; i < COM_ROB_SIZE; i++) {
    cp_com_rob_t *com_rob = get_fifo_slot(cp_ctx->com_rob, i);
    com_rob->state = INVALID;
  }
  cp_ctx->ops = (trace_op_t *) calloc(MAX_OP_BATCH, sizeof(trace_op_t));
  randomize_op_values(cp_ctx->ops, ctx->t_id);
  if (!ENABLE_CLIENTS)
    cp_ctx->trace_info.trace = trace_init(ctx->t_id);

  cp_ctx->debug_loop = init_debug_loop_struct();

  cp_ctx->cp_core_ctx = cp_init_cp_core_ctx((void*) cp_ctx,
                                            &cp_ctx->stall_info,
                                            (void*)ctx,
                                            ctx->t_id);
  return cp_ctx;
}





