//
// Created by vasilis on 11/05/20.
//

#ifndef CP_DEBUG_UTIL_H
#define CP_DEBUG_UTIL_H

#include "cp_netw_generic_util.h"
#include "od_debug_util.h"
#include "od_network_context.h"


static inline void cp_checks_at_loop_start(context_t *ctx)
{
  cp_ctx_t *cp_ctx = (cp_ctx_t *) ctx->appl_ctx;
  //if (ENABLE_ASSERTIONS && CHECK_DBG_COUNTERS)
  //  check_debug_cntrs(credit_debug_cnt, waiting_dbg_counter, cp_ctx,
  //                    (void *) cb->dgram_buf, r_buf_pull_ptr,
  //                    w_buf_pull_ptr, ack_buf_pull_ptr, r_rep_buf_pull_ptr, t_id);

  if (PUT_A_MACHINE_TO_SLEEP && (machine_id == MACHINE_THAT_SLEEPS) &&
      (t_stats[WORKERS_PER_MACHINE -1].total_reqs > 4000000) && (!cp_ctx->debug_loop->slept)) {
    uint seconds = 15;
    if (ctx->t_id == 0) my_printf(yellow, "Workers are going to sleep for %u secs\n", seconds);
    sleep(seconds); cp_ctx->debug_loop->slept = true;
    if (ctx->t_id == 0) my_printf(green, "Worker %u is back\n", ctx->t_id);
  }
  if (ENABLE_ASSERTIONS) {
    if (ENABLE_ASSERTIONS && ctx->t_id == 0)  time_approx++;
    cp_ctx->debug_loop->loop_counter++;
    if (cp_ctx->debug_loop->loop_counter == M_16) {
      //if (t_id == 0) print_all_stalled_sessions(cp_ctx, t_id);

      //printf("Wrkr %u is working rectified keys %lu \n",
      //       t_id, t_stats[t_id].rectified_keys);

      //        if (t_id == 0) {
      //          printf("Wrkr %u sleeping machine bit %u, q-reads %lu, "
      //                   "epoch_id %u, reqs %lld failed writes %lu, writes done %lu/%lu \n", t_id,
      //                 conf_bit_vec[MACHINE_THAT_SLEEPS].bit,
      //                 t_stats[t_id].quorum_reads, (uint16_t) epoch_id,
      //                 t_stats[t_id].total_reqs, t_stats[t_id].failed_rem_writes,
      //                 t_stats[t_id].writes_sent, t_stats[t_id].writes_asked_by_clients);
      //        }
      cp_ctx->debug_loop->loop_counter = 0;
    }
  }
}





static inline void check_received_rmw_in_KVS(void **ops,
                                             uint16_t op_i,
                                             bool is_accept)
{
  if (ENABLE_ASSERTIONS) {
    if (is_accept) {
      cp_acc_t **accs = (cp_acc_t **) ops;
      assert(accs[op_i]->opcode == ACCEPT_OP);
    }
    else {
      cp_prop_t **props = (cp_prop_t **) ops;
      assert(props[op_i]->opcode == PROPOSE_OP);
    }
  }
}



// When pulling a new req from the trace, check the req and the working session
static inline void check_trace_req(cp_ctx_t *cp_ctx, trace_t *trace, trace_op_t *op,
                                   int working_session, uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    assert(trace->opcode != NOP);
    check_state_with_allowed_flags(8, trace->opcode, OP_RELEASE, KVS_OP_PUT,
                                   OP_ACQUIRE, KVS_OP_GET, FETCH_AND_ADD, COMPARE_AND_SWAP_WEAK,
                                   COMPARE_AND_SWAP_STRONG);
    assert(op->opcode == trace->opcode);
    assert(!cp_ctx->stall_info.stalled[working_session]);
  }
}

static inline void sending_stats(context_t *ctx,
                                 uint16_t qp_id,
                                 uint8_t coalesce_num)
{
  od_sending_stats(&t_stats[ctx->t_id].qp_stats[qp_id], coalesce_num);
}

static inline void receiving_stats(context_t *ctx,
                                   uint16_t qp_id,
                                   uint8_t coalesce_num)
{
  od_receiving_stats(&t_stats[ctx->t_id].qp_stats[qp_id], coalesce_num);
}


static inline void send_prop_checks(context_t *ctx)
{
  cp_ctx_t *cp_ctx = (cp_ctx_t *) ctx->appl_ctx;
  per_qp_meta_t *qp_meta = &ctx->qp_meta[PROP_QP_ID];
  fifo_t *send_fifo = qp_meta->send_fifo;

  // Create the broadcast messages
  cp_prop_mes_t *prop_buf = (cp_prop_mes_t *) qp_meta->send_fifo->fifo;
  cp_prop_mes_t *prop_mes = &prop_buf[send_fifo->pull_ptr];

  slot_meta_t *slot_meta = get_fifo_slot_meta_pull(send_fifo);
  uint8_t coalesce_num = (uint8_t) slot_meta->coalesce_num;
  if (ENABLE_ASSERTIONS) {
    assert(send_fifo->net_capacity >= coalesce_num);
    qp_meta->outstanding_messages += coalesce_num;
    assert(prop_mes->coalesce_num == (uint8_t) slot_meta->coalesce_num);
    assert(prop_mes->coalesce_num > 0);
    assert(prop_mes->m_id == (uint8_t) ctx->m_id);

    if (DEBUG_RMW) {
      struct propose *prop = &prop_mes->prop[0];
      my_printf(green, "Wrkr %u : I BROADCAST a propose message %u with %u props with mes_size %u, with credits: %d, lid: %u, "
                       "rmw_id %u, glob_sess id %u, log_no %u, version %u \n",
                ctx->t_id, prop->opcode, coalesce_num, slot_meta->byte_size,
                qp_meta->credits[(machine_id + 1) % MACHINE_NUM], prop_mes->l_id,
                prop->t_rmw_id, prop->t_rmw_id % GLOBAL_SESSION_NUM,
                prop->log_no, prop->ts.version);
    }

  }
  sending_stats(ctx, PROP_QP_ID, coalesce_num);
}


static inline void send_acc_checks(context_t *ctx)
{
  cp_ctx_t *cp_ctx = (cp_ctx_t *) ctx->appl_ctx;
  per_qp_meta_t *qp_meta = &ctx->qp_meta[ACC_QP_ID];
  fifo_t *send_fifo = qp_meta->send_fifo;

  // Create the broadcast messages
  cp_acc_mes_t *acc_buf = (cp_acc_mes_t *) qp_meta->send_fifo->fifo;
  cp_acc_mes_t *acc_mes = &acc_buf[send_fifo->pull_ptr];

  slot_meta_t *slot_meta = get_fifo_slot_meta_pull(send_fifo);
  uint8_t coalesce_num = (uint8_t) slot_meta->coalesce_num;
  if (ENABLE_ASSERTIONS) {
    assert(send_fifo->net_capacity >= coalesce_num);
    qp_meta->outstanding_messages += coalesce_num;
    assert(acc_mes->coalesce_num == (uint8_t) slot_meta->coalesce_num);
    assert(acc_mes->coalesce_num > 0);
    assert(acc_mes->m_id == (uint8_t) ctx->m_id);

    if (DEBUG_RMW) {
      cp_acc_t *acc = &acc_mes->acc[0];
      my_printf(green, "Wrkr %u : I BROADCAST an accept message %u with %u accs with mes_size %u, with credits: %d, lid: %u, "
                       "rmw_id %u, glob_sess id %u, log_no %u, version %u \n",
                ctx->t_id, acc->opcode, coalesce_num, slot_meta->byte_size,
                qp_meta->credits[(machine_id + 1) % MACHINE_NUM], acc_mes->l_id,
                acc->t_rmw_id, acc->t_rmw_id % GLOBAL_SESSION_NUM,
                acc->log_no, acc->ts.version);
    }
  }
  sending_stats(ctx, ACC_QP_ID, coalesce_num);
}


static inline void send_com_checks(context_t *ctx)
{
  cp_ctx_t *cp_ctx = (cp_ctx_t *) ctx->appl_ctx;
  per_qp_meta_t *qp_meta = &ctx->qp_meta[COM_QP_ID];
  fifo_t *send_fifo = qp_meta->send_fifo;

  // Create the broadcast messages
  cp_com_mes_t *com_mes = (cp_com_mes_t *) get_fifo_pull_slot(qp_meta->send_fifo);

  slot_meta_t *slot_meta = get_fifo_slot_meta_pull(send_fifo);
  uint8_t coalesce_num = (uint8_t) slot_meta->coalesce_num;
  if (ENABLE_ASSERTIONS) {

    if (DEBUG_COMMITS) {
      cp_com_t *com = &com_mes->com[0];
      my_printf(green, "Wrkr %u : I BROADCAST a commit message %u "
                       "with %u (%u) coms with mes_size %u, with credits: %d, lid: %u, "
                       "rmw_id %u, glob_sess id %u, log_no %u, base version %u \n",
                ctx->t_id, com->opcode, com_mes->coalesce_num,
                slot_meta->coalesce_num,
                slot_meta->byte_size,
                qp_meta->credits[(machine_id + 1) % MACHINE_NUM], com_mes->l_id,
                com->t_rmw_id, com->t_rmw_id % GLOBAL_SESSION_NUM,
                com->log_no, com->base_ts.version);
    }

    assert(send_fifo->net_capacity >= coalesce_num);
    qp_meta->outstanding_messages += coalesce_num;
    assert(com_mes->coalesce_num == (uint8_t) slot_meta->coalesce_num);
    assert(com_mes->coalesce_num > 0);
    assert(com_mes->m_id == (uint8_t) ctx->m_id);

    for (uint8_t i = 0; i < coalesce_num; i++)
    {
      uint16_t backward_ptr = (slot_meta->backward_ptr + i) % COM_FIFO_SIZE;
      cp_com_rob_t *com_rob = (cp_com_rob_t *) get_fifo_slot(cp_ctx->com_rob, backward_ptr);
      assert(com_rob->state == VALID);
      assert(com_rob->acks_seen == 0);
      assert(com_rob->l_id == com_mes->l_id + i);
      com_rob->state = SENT_COMMIT;

    }


  }
  sending_stats(ctx, COM_QP_ID, coalesce_num);
}


static inline void send_rmw_rep_checks(context_t *ctx)
{
  cp_ctx_t *cp_ctx = (cp_ctx_t *) ctx->appl_ctx;
  per_qp_meta_t *qp_meta = &ctx->qp_meta[RMW_REP_QP_ID];
  fifo_t *send_fifo = qp_meta->send_fifo;

  cp_rmw_rep_mes_t *rep_buf = (cp_rmw_rep_mes_t *) qp_meta->send_fifo->fifo;
  cp_rmw_rep_mes_t *rep_mes = &rep_buf[send_fifo->pull_ptr];
  bool is_accept = rep_mes->opcode == ACCEPT_REPLY;
  slot_meta_t *slot_meta = get_fifo_slot_meta_pull(send_fifo);
  if (DEBUG_RMW)
    my_printf(yellow, "Worker %u sending %s rep_mes l_id %lu, coalesce %u, to m_id %u, opcode %u\n",
              ctx->t_id,
              is_accept ? "accept" : "propose",
              rep_mes->l_id,
              rep_mes->coalesce_num,
              rep_mes->m_id,
              rep_mes->opcode);

  sending_stats(ctx, RMW_REP_QP_ID, slot_meta->coalesce_num);
}


// When polling acks: more precisely when inspecting each l_id acked
static inline void  cp_check_ack_and_print(context_t *ctx,
                                           cp_com_rob_t *com_rob,
                                           ctx_ack_mes_t *ack,
                                           uint16_t ack_i,
                                           uint32_t ack_ptr,
                                           uint16_t  ack_num)
{
  if (ENABLE_ASSERTIONS) {
    cp_ctx_t* cp_ctx = (cp_ctx_t*) ctx->appl_ctx;
    per_qp_meta_t *qp_meta = &ctx->qp_meta[ACK_QP_ID];
    uint64_t pull_lid = cp_ctx->l_ids.applied_com_id;
    if (ENABLE_ASSERTIONS && (ack_ptr == cp_ctx->com_rob->push_ptr)) {
      uint32_t origin_ack_ptr = (uint32_t) (ack_ptr - ack_i + COM_ROB_SIZE) % COM_ROB_SIZE;
      my_printf(red, "Origin ack_ptr %u/%u, acks %u/%u, pull_ptr %u, push_ptr % u, capacity %u \n",
                origin_ack_ptr, (cp_ctx->com_rob->pull_ptr + (ack->l_id - pull_lid)) % COM_ROB_SIZE,
                ack_i, ack_num, cp_ctx->com_rob->pull_ptr, cp_ctx->com_rob->push_ptr, cp_ctx->com_rob->capacity);
    }

    assert((com_rob->l_id % cp_ctx->com_rob->max_size) == ack_ptr);
    if (com_rob->acks_seen == REMOTE_QUORUM) {
      qp_meta->outstanding_messages--;
      assert(com_rob->state == SENT_COMMIT);
      if (DEBUG_ACKS)
        printf("Worker %d, sess %u: valid ack %u/%u com at ptr %d is ready \n",
               ctx->t_id, com_rob->sess_id, ack_i, ack_num,  ack_ptr);
    }
  }
}





static inline void check_when_polling_for_props(context_t* ctx,
                                                cp_prop_mes_t *prop_mes)
{
  uint8_t coalesce_num = prop_mes->coalesce_num;
  per_qp_meta_t *qp_meta = &ctx->qp_meta[PROP_QP_ID];
  fifo_t *recv_fifo = qp_meta->recv_fifo;
  if (ENABLE_ASSERTIONS) {
    //struct prop_message *p_mes = (struct prop_message *)r_mes;
    cp_prop_t *prop = &prop_mes->prop[0];
    assert(coalesce_num > 0);
   if (DEBUG_RMW) {
      my_printf(cyan, "Worker %u sees a Propose "
                      "from m_id %u: opcode %d at offset %d, rmw_id %lu, "
                  "log_no %u, coalesce_num %u version %u \n",
                ctx->t_id, prop_mes->m_id,
                prop->opcode,
                recv_fifo->pull_ptr,
                prop->t_rmw_id,
                prop->log_no,
                coalesce_num,
                prop->ts.version);
      assert(prop_mes->m_id != machine_id);

    }
    if (qp_meta->polled_messages + coalesce_num > MAX_INCOMING_PROP) assert(false);
  }
  receiving_stats(ctx, PROP_QP_ID, coalesce_num);
}

static inline void check_when_polling_for_accs(context_t* ctx,
                                                cp_acc_mes_t *acc_mes)
{
  uint8_t coalesce_num = acc_mes->coalesce_num;
  per_qp_meta_t *qp_meta = &ctx->qp_meta[ACC_QP_ID];
  fifo_t *recv_fifo = qp_meta->recv_fifo;
  if (ENABLE_ASSERTIONS) {
    //struct acc_message *p_mes = (struct acc_message *)r_mes;
    cp_acc_t *acc = &acc_mes->acc[0];
    assert(coalesce_num > 0);
    if (DEBUG_RMW) {
      my_printf(cyan, "Worker %u sees an Accept "
                      "from m_id %u: opcode %d at offset %d, rmw_id %lu, "
                      "log_no %u, coalesce_num %u version %u \n",
                ctx->t_id, acc_mes->m_id,
                acc->opcode,
                recv_fifo->pull_ptr,
                acc->t_rmw_id,
                acc->log_no,
                coalesce_num,
                acc->ts.version);
      assert(acc_mes->m_id != machine_id);

    }
    if (qp_meta->polled_messages + coalesce_num > MAX_INCOMING_ACC) assert(false);
  }
  receiving_stats(ctx, ACC_QP_ID, coalesce_num);
}


static inline void check_when_polling_for_coms(context_t* ctx,
                                               cp_com_mes_t *com_mes)
{
  uint8_t coalesce_num = com_mes->coalesce_num;
  per_qp_meta_t *qp_meta = &ctx->qp_meta[COM_QP_ID];
  fifo_t *recv_fifo = qp_meta->recv_fifo;
  if (ENABLE_ASSERTIONS) {
    cp_com_t *com = &com_mes->com[0];
    assert(coalesce_num > 0);
    if (DEBUG_RMW) {
      my_printf(cyan, "Worker %u sees a Commit "
                      "from m_id %u: opcode %d at offset %d, rmw_id %lu, "
                      "log_no %u, coalesce_num %u version %u \n",
                ctx->t_id, com_mes->m_id,
                com->opcode,
                recv_fifo->pull_ptr,
                com->t_rmw_id,
                com->log_no,
                coalesce_num,
                com->base_ts.version);
      assert(com_mes->m_id != machine_id);

    }
    if (qp_meta->polled_messages + coalesce_num > MAX_INCOMING_COM) assert(false);
  }
  receiving_stats(ctx, COM_QP_ID, coalesce_num);
}

//-------------------------SENDING ACKS
// This is to be called from within odyssey
static inline void checks_stats_prints_when_sending_acks(context_t *ctx,
                                                         ctx_ack_mes_t *acks,
                                                         uint8_t m_i)
{
  sending_stats(ctx, ACK_QP_ID, acks[m_i].ack_num);
  if (DEBUG_ACKS)
    my_printf(yellow, "Wrkr %d is sending an ack  to machine %u for lid %lu, credits %u and ack num %d and m id %d \n",
              ctx->t_id, m_i, acks[m_i].l_id, acks[m_i].credits, acks[m_i].ack_num, acks[m_i].m_id);

  if (ENABLE_ASSERTIONS) {
    assert(acks[m_i].credits <= acks[m_i].ack_num);
    if (acks[m_i].ack_num > MAX_COM_COALESCE) assert(acks[m_i].credits > 1);
    if (!ENABLE_MULTICAST) assert(acks[m_i].credits <= COM_CREDITS);
    assert(acks[m_i].ack_num > 0);
  }
}

static inline void check_when_filling_op(cp_ctx_t *cp_ctx,
                                         trace_op_t *op,
                                         int working_session,
                                         bool is_rmw)
{
  if (ENABLE_ASSERTIONS) assert(is_rmw);
  if (ENABLE_ASSERTIONS && !ENABLE_CLIENTS && op->opcode == FETCH_AND_ADD) {
    assert(is_rmw);
    assert(op->value_to_write == op->value);
    assert(*(uint64_t *) op->value_to_write == 1);
    assert(!cp_ctx->stall_info.stalled[working_session]);
  }
}

#endif //CP_DEBUG_UTIL_H
