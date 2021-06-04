#ifndef CP_INLINE_UTIL_H
#define CP_INLINE_UTIL_H

//#include "kvs.h"
#include "od_hrd.h"

#include "od_inline_util.h"
#include "cp_generic_util.h"
#include "cp_kvs_util.h"
#include "cp_debug_util.h"
#include "cp_config_util.h"
#include "cp_paxos_util.h"
#include "cp_reserve_stations_util.h"
#include "cp_communication_utility.h"

#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <assert.h>
#include <cp_config.h>
#include <infiniband/verbs.h>


/* ---------------------------------------------------------------------------
//------------------------------ PULL NEW REQUESTS ----------------------------------
//---------------------------------------------------------------------------*/

static inline void batch_requests_to_KVS(context_t *ctx)
{

  p_ops_t* p_ops = (p_ops_t*) ctx->appl_ctx;
  trace_op_t *ops = p_ops->ops;
  trace_t *trace = p_ops->trace;

  uint16_t writes_num = 0, reads_num = 0, op_i = 0;
  int working_session = -1;
  // if there are clients the "all_sessions_stalled" flag is not used,
  // so we need not bother checking it
  if (!ENABLE_CLIENTS && p_ops->all_sessions_stalled) {
    debug_all_sessions(p_ops, ctx->t_id);
    return;
  }
  for (uint16_t i = 0; i < SESSIONS_PER_THREAD; i++) {
    uint16_t sess_i = (uint16_t)((p_ops->last_session + i) % SESSIONS_PER_THREAD);
    if (od_pull_request_from_this_session(p_ops->stalled[sess_i], sess_i, ctx->t_id)) {
      working_session = sess_i;
      break;
    }
    else debug_sessions(p_ops, sess_i, ctx->t_id);
  }
  //printf("working session = %d\n", working_session);
  if (ENABLE_CLIENTS) {
    if (working_session == -1) return;
  }
  else if (ENABLE_ASSERTIONS ) assert(working_session != -1);

  bool passed_over_all_sessions = false;
  while (op_i < MAX_OP_BATCH && !passed_over_all_sessions) {
    fill_trace_op(ctx, p_ops, &ops[op_i], &trace[p_ops->trace_iter],
                  working_session, ctx->t_id);

    // Find out next session to work on
    passed_over_all_sessions =
        od_find_next_working_session(ctx, &working_session,
                                     p_ops->stalled,
                                     p_ops->last_session,
                                     &p_ops->all_sessions_stalled);
    if (!ENABLE_CLIENTS) {
      p_ops->trace_iter++;
      if (trace[p_ops->trace_iter].opcode == NOP) p_ops->trace_iter = 0;
    }
    op_i++;
  }

  p_ops->last_session = (uint16_t) working_session;
  t_stats[ctx->t_id].cache_hits_per_thread += op_i;
  cp_KVS_batch_op_trace(op_i, ops, p_ops, ctx->t_id);
  for (uint16_t i = 0; i < op_i; i++) {
    insert_rmw(p_ops, &ops[i], ctx->t_id);
  }
}

/* ---------------------------------------------------------------------------
//------------------------------ RMW FSM ----------------------------------
//---------------------------------------------------------------------------*/

// Worker inspects its local RMW entries
static inline void inspect_rmws(p_ops_t *p_ops, uint16_t t_id)
{
  for (uint16_t sess_i = 0; sess_i < SESSIONS_PER_THREAD; sess_i++) {
    loc_entry_t* loc_entry = &p_ops->prop_info->entry[sess_i];
    uint8_t state = loc_entry->state;
    if (state == INVALID_RMW) continue;
    if (ENABLE_ASSERTIONS) {
      assert(loc_entry->sess_id == sess_i);
      assert(p_ops->stalled[sess_i]);
    }

    /* =============== ACCEPTED ======================== */
    if (state == ACCEPTED) {
      check_sum_of_reps(loc_entry);
      //printf("reps %u \n", loc_entry->rmw_reps.tot_replies);
      if (loc_entry->rmw_reps.ready_to_inspect) {
        loc_entry->rmw_reps.inspected = true;
        inspect_accepts(p_ops, loc_entry, t_id);
        check_state_with_allowed_flags(7, (int) loc_entry->state, ACCEPTED, INVALID_RMW, RETRY_WITH_BIGGER_TS,
                                       NEEDS_KV_PTR, MUST_BCAST_COMMITS, MUST_BCAST_COMMITS_FROM_HELP);
        if (ENABLE_ASSERTIONS && loc_entry->rmw_reps.ready_to_inspect)
            assert(loc_entry->state == ACCEPTED && loc_entry->all_aboard);
      }
    }

    /* =============== PROPOSED ======================== */
    if (state == PROPOSED) {
      //if (cannot_accept_if_unsatisfied_release(loc_entry, &p_ops->sess_info[sess_i])) {
      //  continue;
      //}

      if (loc_entry->rmw_reps.ready_to_inspect) {
        loc_entry->stalled_reason = NO_REASON;
        // further responses for that broadcast of Propose must be disregarded;
        // in addition we do this before inspecting, so that if we broadcast accepts, they have a fresh l_id
        loc_entry->rmw_reps.inspected = true;
        advance_loc_entry_l_id(loc_entry, t_id);
        inspect_proposes(p_ops, loc_entry, t_id);
        check_state_with_allowed_flags(7, (int) loc_entry->state, INVALID_RMW, RETRY_WITH_BIGGER_TS,
                                       NEEDS_KV_PTR, ACCEPTED, MUST_BCAST_COMMITS, MUST_BCAST_COMMITS_FROM_HELP);
        if (ENABLE_ASSERTIONS) assert(!loc_entry->rmw_reps.ready_to_inspect);
        if (loc_entry->state != ACCEPTED) assert(loc_entry->rmw_reps.tot_replies == 0);
        else assert(loc_entry->rmw_reps.tot_replies == 1);
      }
      else {
        assert(loc_entry->rmw_reps.tot_replies < QUORUM_NUM);
        loc_entry->stalled_reason = STALLED_BECAUSE_NOT_ENOUGH_REPS;
      }
    }

    /* =============== BROADCAST COMMITS ======================== */
    if (state == MUST_BCAST_COMMITS || state == MUST_BCAST_COMMITS_FROM_HELP) {
      loc_entry_t *entry_to_commit =
        state == MUST_BCAST_COMMITS ? loc_entry : loc_entry->help_loc_entry;
      //bool is_commit_helping = loc_entry->helping_flag != NOT_HELPING;
      if (p_ops->virt_w_size < MAX_ALLOWED_W_SIZE) {
        if (state == MUST_BCAST_COMMITS_FROM_HELP && loc_entry->helping_flag == PROPOSE_NOT_LOCALLY_ACKED) {
          my_printf(green, "Wrkr %u sess %u will bcast commits for the latest committed RMW,"
                      " after learning its proposed RMW has already been committed \n",
                    t_id, loc_entry->sess_id);
        }
        insert_write(p_ops, (trace_op_t*) entry_to_commit, FROM_COMMIT, state, t_id);
        loc_entry->state = COMMITTED;
        continue;
      }
    }

    /* =============== RETRY ======================== */
    if (state == RETRY_WITH_BIGGER_TS) {
      take_kv_ptr_with_higher_TS(p_ops, loc_entry, false, t_id);
      check_state_with_allowed_flags(5, (int) loc_entry->state, INVALID_RMW, PROPOSED,
                                     NEEDS_KV_PTR, MUST_BCAST_COMMITS);
      if (loc_entry->state == PROPOSED) {
        insert_prop_to_read_fifo(p_ops, loc_entry, t_id);
      }
    }

    /* =============== NEEDS_KV_PTR ======================== */
    if (state == NEEDS_KV_PTR) {
      handle_needs_kv_ptr_state(p_ops, loc_entry, sess_i, t_id);
      check_state_with_allowed_flags(6, (int) loc_entry->state, INVALID_RMW, PROPOSED, NEEDS_KV_PTR,
                                     ACCEPTED, MUST_BCAST_COMMITS);
    }

  }
}


/* ---------------------------------------------------------------------------
//------------------------------ BROADCASTS ----------------------------------
//---------------------------------------------------------------------------*/
static inline void send_props_helper(context_t *ctx)
{
  send_prop_checks(ctx);
}

// Broadcast Writes
static inline void broadcast_writes(context_t *ctx)
{
  //printf("Worker %d bcasting writes \n", t_id);
  per_qp_meta_t *qp_meta = &ctx->qp_meta[W_QP_ID];
  per_qp_meta_t *ack_qp_meta = &ctx->qp_meta[ACK_QP_ID];
  per_qp_meta_t *r_rep_qp_meta = &ctx->qp_meta[PROP_REP_QP_ID];

  p_ops_t *p_ops = (p_ops_t *) ctx->appl_ctx;
  uint16_t br_i = 0, mes_sent = 0, available_credits = 0;
  uint32_t bcast_pull_ptr = p_ops->w_fifo->bcast_pull_ptr;
  if (p_ops->w_fifo->bcast_size == 0) return;
  if (release_not_ready(p_ops, &p_ops->w_fifo->info[bcast_pull_ptr], (w_mes_t *)
    &p_ops->w_fifo->w_message[bcast_pull_ptr], ctx->t_id))
    return;

  if (!check_bcast_credits(qp_meta->credits, ctx->q_info,
                           &qp_meta->time_out_cnt,
                           &available_credits, 1,
                           ctx->t_id)) return;
  if (ENABLE_ASSERTIONS) assert(available_credits <= W_CREDITS);

  while (p_ops->w_fifo->bcast_size > 0 && mes_sent < available_credits) {
    if (mes_sent >  0 &&
      release_not_ready(p_ops, &p_ops->w_fifo->info[bcast_pull_ptr], (struct w_message *)
        &p_ops->w_fifo->w_message[bcast_pull_ptr], ctx->t_id)) {
      break;
    }
    if (DEBUG_WRITES)
      printf("Wrkr %d has %u write bcasts to send credits %d\n", ctx->t_id, p_ops->w_fifo->bcast_size, available_credits);
    // Create the broadcast messages
    forge_w_wr(bcast_pull_ptr, ctx, br_i);
    br_i++;
    struct w_message *w_mes = (struct w_message *) &p_ops->w_fifo->w_message[bcast_pull_ptr];
      uint8_t coalesce_num = w_mes->coalesce_num;
    debug_and_count_stats_when_broadcasting_writes(p_ops, bcast_pull_ptr, coalesce_num,
                                                   ctx->t_id, br_i,
                                                   &qp_meta->outstanding_messages);
    p_ops->w_fifo->bcast_size -= coalesce_num;
    // This message has been sent, do not add other writes to it!
    if (p_ops->w_fifo->bcast_size == 0) reset_write_message(p_ops);
    mes_sent++;
    MOD_INCR(bcast_pull_ptr, W_FIFO_SIZE);
    if (br_i == MAX_BCAST_BATCH) {
      post_receives_for_r_reps_for_accepts(r_rep_qp_meta->recv_info, ctx->t_id);
      post_quorum_broadasts_and_recvs(ack_qp_meta->recv_info,
                                      ack_qp_meta->recv_wr_num - ack_qp_meta->recv_info->posted_recvs,
                                      ctx->q_info, br_i, qp_meta->sent_tx, qp_meta->send_wr,
                                      qp_meta->send_qp, qp_meta->enable_inlining);
      br_i = 0;
    }
  }
  if (br_i > 0) {
    if (ENABLE_ASSERTIONS) assert(MAX_BCAST_BATCH > 1);
    post_receives_for_r_reps_for_accepts(r_rep_qp_meta->recv_info, ctx->t_id);
    post_quorum_broadasts_and_recvs(ack_qp_meta->recv_info,
                                    ack_qp_meta->recv_wr_num - ack_qp_meta->recv_info->posted_recvs,
                                    ctx->q_info, br_i, qp_meta->sent_tx, qp_meta->send_wr,
                                    qp_meta->send_qp, qp_meta->enable_inlining);
  }

  p_ops->w_fifo->bcast_pull_ptr = bcast_pull_ptr;
  if (ENABLE_ASSERTIONS) assert(mes_sent <= available_credits && mes_sent <= W_CREDITS);
  if (mes_sent > 0) decrease_credits(qp_meta->credits, ctx->q_info, mes_sent);
}





/* ---------------------------------------------------------------------------
//------------------------------ UNICASTS-------------------------------------
//---------------------------------------------------------------------------*/

// Send Read Replies
static inline void send_r_reps(context_t *ctx)
{
  per_qp_meta_t *qp_meta = &ctx->qp_meta[PROP_REP_QP_ID];
  p_ops_t *p_ops = (p_ops_t *) ctx->appl_ctx;
  uint16_t mes_i = 0, accept_recvs_to_post = 0, read_recvs_to_post = 0;
  uint32_t pull_ptr = p_ops->r_rep_fifo->pull_ptr;
  struct ibv_send_wr *bad_send_wr;

  struct r_rep_fifo *r_rep_fifo = p_ops->r_rep_fifo;
  while (r_rep_fifo->total_size > 0) {
    struct r_rep_message *r_rep_mes = (struct r_rep_message *) &r_rep_fifo->r_rep_message[pull_ptr];
    // Create the r_rep messages
    forge_r_rep_wr(pull_ptr, mes_i, ctx);
    uint8_t coalesce_num = r_rep_mes->coalesce_num;
    print_check_count_stats_when_sending_r_rep(r_rep_fifo, coalesce_num, mes_i, ctx->t_id);
    r_rep_fifo->total_size -= coalesce_num;
    r_rep_fifo->mes_size--;
    //r_reps_sent += coalesce_num;
    if (r_rep_mes->opcode == ACCEPT_REPLY)
      accept_recvs_to_post++;
    else if (r_rep_mes->opcode != ACCEPT_REPLY_NO_CREDITS)
      read_recvs_to_post++;
    MOD_INCR(pull_ptr, R_REP_FIFO_SIZE);
    mes_i++;
  }
  if (mes_i > 0) {
    if (read_recvs_to_post > 0) {
      if (DEBUG_READ_REPS) printf("Wrkr %d posting %d read recvs\n", ctx->t_id,  read_recvs_to_post);
      post_recvs_with_recv_info(ctx->qp_meta[PROP_QP_ID].recv_info, read_recvs_to_post);
    }
    if (accept_recvs_to_post > 0) {
      if (DEBUG_RMW) printf("Wrkr %d posting %d accept recvs\n", ctx->t_id,  accept_recvs_to_post);
      post_recvs_with_recv_info(ctx->qp_meta[W_QP_ID].recv_info, accept_recvs_to_post);
    }
    qp_meta->send_wr[mes_i - 1].next = NULL;
    int ret = ibv_post_send(qp_meta->send_qp, qp_meta->send_wr, &bad_send_wr);
    if (ENABLE_ASSERTIONS) CPE(ret, "R_REP ibv_post_send error", ret);
  }
  r_rep_fifo->pull_ptr = pull_ptr;

}


/* ---------------------------------------------------------------------------
//------------------------------ POLLING-------------------------------------
//---------------------------------------------------------------------------*/
static inline bool prop_recv_handler(context_t* ctx)
{
  p_ops_t *p_ops = (p_ops_t *) ctx->appl_ctx;
  per_qp_meta_t *qp_meta = &ctx->qp_meta[PROP_QP_ID];
  fifo_t *recv_fifo = qp_meta->recv_fifo;
  volatile cp_prop_mes_ud_t *incoming_props = (volatile cp_prop_mes_ud_t *) recv_fifo->fifo;
  cp_prop_mes_t *prop_mes = (cp_prop_mes_t *) &incoming_props[recv_fifo->pull_ptr].prop_mes;

  check_when_polling_for_props(ctx, prop_mes);

  uint8_t coalesce_num = prop_mes->coalesce_num;

  ptrs_to_prop_t *ptrs_to_prop = p_ops->ptrs_to_prop;
  if (qp_meta->polled_messages == 0) ptrs_to_prop->polled_props = 0;

  for (uint16_t i = 0; i < coalesce_num; i++) {
    cp_prop_t *prop = &prop_mes->prop[i];
    check_state_with_allowed_flags(2, prop->opcode, PROPOSE_OP);
    ptrs_to_prop->ptr_to_ops[ptrs_to_prop->polled_props] = (void *) prop;
    ptrs_to_prop->ptr_to_mes[ptrs_to_prop->polled_props] = prop_mes;
    ptrs_to_prop->break_message[ptrs_to_prop->polled_props] = i == 0;
    ptrs_to_prop->polled_props++;
  }

  return true;
}

// Poll for the write broadcasts
static inline void poll_for_writes(context_t *ctx,
                                   uint16_t qp_id)
{
  per_qp_meta_t *qp_meta = &ctx->qp_meta[qp_id];
  fifo_t *recv_fifo = qp_meta->recv_fifo;
  p_ops_t *p_ops = (p_ops_t *) ctx->appl_ctx;
  ctx_ack_mes_t *acks = (ctx_ack_mes_t *) ctx->qp_meta[ACK_QP_ID].send_fifo->fifo;
  uint32_t writes_for_kvs = 0;
  int completed_messages =
    find_how_many_messages_can_be_polled(qp_meta->recv_cq, qp_meta->recv_wc,
                                         &qp_meta->completed_but_not_polled,
                                         qp_meta->recv_buf_slot_num, ctx->t_id);
  if (completed_messages <= 0) return;

  qp_meta->polled_messages = 0;
  // Start polling
  while (qp_meta->polled_messages < completed_messages) {
    volatile w_mes_ud_t *incoming_ws = (volatile w_mes_ud_t *) qp_meta->recv_fifo->fifo;
    w_mes_t *w_mes = (w_mes_t *) &incoming_ws[recv_fifo->pull_ptr].w_mes;
    check_the_polled_write_message(w_mes, recv_fifo->pull_ptr, writes_for_kvs, ctx->t_id);
    print_polled_write_message_info(w_mes, recv_fifo->pull_ptr, ctx->t_id);
    uint8_t w_num = w_mes->coalesce_num;
    check_state_with_allowed_flags(4, w_mes->opcode, ONLY_WRITES, ONLY_ACCEPTS, WRITES_AND_ACCEPTS);
    bool is_only_accepts = w_mes->opcode == ONLY_ACCEPTS;


    uint8_t writes_to_be_acked = 0, accepts = 0;
    uint32_t running_writes_for_kvs = writes_for_kvs;
    uint16_t byte_ptr = W_MES_HEADER;
    for (uint16_t i = 0; i < w_num; i++) {
      write_t *write = (write_t *)(((void *)w_mes) + byte_ptr);
      byte_ptr += get_write_size_from_opcode(write->opcode);
      check_a_polled_write(write, i, w_num, w_mes->opcode, ctx->t_id);
      handle_configuration_on_receiving_rel(write, ctx->t_id);
      if (ENABLE_ASSERTIONS) assert(write->opcode != ACCEPT_OP_BIT_VECTOR);

      if (write->opcode != NO_OP_RELEASE) {
        p_ops->ptrs_to_mes_ops[running_writes_for_kvs] = (void *) write; //(((void *) write) - 3); // align with trace_op
        if (write->opcode == ACCEPT_OP) {
          p_ops->ptrs_to_mes_headers[running_writes_for_kvs] = (struct r_message *) w_mes;
          p_ops->coalesce_r_rep[running_writes_for_kvs] = accepts > 0;
          raise_conf_bit_if_accept_signals_it((struct accept *) write, w_mes->m_id, ctx->t_id);
        }
        if (PRINT_LOGS && write->opcode == COMMIT_OP) {
          p_ops->ptrs_to_mes_headers[running_writes_for_kvs] = (struct r_message *) w_mes;
        }
        running_writes_for_kvs++;
      }
      if (write->opcode != ACCEPT_OP) writes_to_be_acked++;
      else accepts++;
    }

    if (ENABLE_ASSERTIONS) assert(accepts + writes_to_be_acked == w_num);
    // Make sure the writes of the message can be processed
    if (!is_only_accepts) {
      if (ENABLE_ASSERTIONS) assert(writes_to_be_acked > 0);
      if (!ctx_ack_insert(ctx, ACK_QP_ID, writes_to_be_acked, w_mes->l_id, w_mes->m_id)) {

        //if (DEBUG_QUORUM)
         // my_printf(yellow, "Wrkr %u leaves %u messages for the next polling round \n",
         //               t_id, *completed_but_not_polled);
        break;
      }
    }
    else if (ENABLE_ASSERTIONS) assert(w_mes->l_id == 0);

    writes_for_kvs = running_writes_for_kvs;
    count_stats_on_receiving_w_mes_reset_w_num(w_mes, w_num, ctx->t_id);
    fifo_incr_pull_ptr(recv_fifo);
    qp_meta->polled_messages++;
  }
  qp_meta->completed_but_not_polled = completed_messages - qp_meta->polled_messages;
  qp_meta->recv_info->posted_recvs -= qp_meta->polled_messages;

  if (writes_for_kvs > 0) {
    if (DEBUG_WRITES) my_printf(yellow, "Worker %u is going with %u writes to the kvs \n", ctx->t_id, writes_for_kvs);
    KVS_batch_op_updates((uint16_t) writes_for_kvs, ctx->t_id, (write_t **) p_ops->ptrs_to_mes_ops,
                         p_ops, 0, (uint32_t) MAX_INCOMING_W);
    if (DEBUG_WRITES) my_printf(yellow, "Worker %u propagated %u writes to the kvs \n", ctx->t_id, writes_for_kvs);
  }
}


// Poll for the r_rep broadcasts
static inline void poll_for_reads(context_t *ctx)
{
  per_qp_meta_t *qp_meta = &ctx->qp_meta[PROP_QP_ID];
  fifo_t *recv_fifo = qp_meta->recv_fifo;
  p_ops_t *p_ops = (p_ops_t *) ctx->appl_ctx;
  if (p_ops->r_rep_fifo->mes_size == R_REP_FIFO_SIZE) return;

  int completed_messages =
    find_how_many_messages_can_be_polled(qp_meta->recv_cq, qp_meta->recv_wc,
                                         &qp_meta->completed_but_not_polled,
                                         qp_meta->recv_buf_slot_num, ctx->t_id);
  if (completed_messages <= 0) {
    qp_meta->wait_for_reps_ctr++;
    return;
  }
  qp_meta->polled_messages = 0;
  uint32_t polled_reads = 0;
  // Start polling
  while (qp_meta->polled_messages < completed_messages) {
    volatile r_mes_ud_t *incoming_rs = (volatile r_mes_ud_t *) qp_meta->recv_fifo->fifo;
    r_mes_t *r_mes = (r_mes_t *) &incoming_rs[recv_fifo->pull_ptr].r_mes;
    uint8_t r_num = r_mes->coalesce_num;
    uint16_t byte_ptr = PROP_MES_HEADER;
    for (uint16_t i = 0; i < r_num; i++) {
      struct read *read = (struct read*)(((void *) r_mes) + byte_ptr);
      //printf("Receiving read opcode %u \n", read->opcode);
      bool is_propose = read->opcode == PROPOSE_OP;
      if (is_propose) {
        struct propose *prop = (struct propose *) read;
        check_state_with_allowed_flags(2, prop->opcode, PROPOSE_OP);
        p_ops->ptrs_to_mes_ops[polled_reads] = (void *) prop;
      }
      else {
        check_read_opcode_when_polling_for_reads(read, i, r_num, ctx->t_id);
        if (read->opcode == OP_ACQUIRE) {
          read->opcode =
            take_ownership_of_a_conf_bit(r_mes->l_id + i, (uint16_t) r_mes->m_id, false, ctx->t_id) ?
            (uint8_t) OP_ACQUIRE_FP : (uint8_t) OP_ACQUIRE;
        }
        if (read->opcode == OP_ACQUIRE_FLIP_BIT)
          raise_conf_bit_iff_owned(*(uint64_t *) &read->key, (uint16_t) r_mes->m_id, false, ctx->t_id);

        p_ops->ptrs_to_mes_ops[polled_reads] = (void *) read;

      }
      p_ops->ptrs_to_mes_headers[polled_reads] = r_mes;
      p_ops->coalesce_r_rep[polled_reads] = i > 0;
      polled_reads++;
      byte_ptr += get_read_size_from_opcode(read->opcode);
    }
    if (ENABLE_ASSERTIONS) r_mes->coalesce_num = 0;
    fifo_incr_pull_ptr(recv_fifo);
    qp_meta->polled_messages++;
    if (ENABLE_ASSERTIONS)
      assert(qp_meta->polled_messages + p_ops->r_rep_fifo->mes_size < R_REP_FIFO_SIZE);
  }
  // Poll for the completion of the receives
  if (qp_meta->polled_messages > 0) {
    KVS_batch_op_reads(polled_reads, ctx->t_id, p_ops, 0, MAX_INCOMING_PROP);
    if (ENABLE_ASSERTIONS) qp_meta->wait_for_reps_ctr = 0;
  }

}



// Apply the acks that refer to stored writes
static inline void apply_acks(uint32_t ack_num, uint32_t ack_ptr,
                              uint8_t ack_m_id,
                              uint64_t l_id, uint64_t pull_lid,
                              context_t *ctx)
{
  //per_qp_meta_t *qp_meta = &ctx->qp_meta[ACK_QP_ID];
  p_ops_t *p_ops = (p_ops_t *) ctx->appl_ctx;
  for (uint16_t ack_i = 0; ack_i < ack_num; ack_i++) {
    //printf("Checking my acks \n");
    check_ack_and_print(p_ops, ack_i, ack_ptr, ack_num, l_id, pull_lid, ctx->t_id);
    per_write_meta_t *w_meta = &p_ops->w_meta[ack_ptr];
    w_meta->acks_seen++;
    bool ack_m_id_found = false;
    if (ENABLE_ASSERTIONS) assert(w_meta->acks_expected >= REMOTE_QUORUM);

    for (uint8_t i = 0; i < w_meta->acks_expected; i++) {
      if (ack_m_id == w_meta->expected_ids[i]) {
        ack_m_id_found = true;
        w_meta->seen_expected[i] = true;
        break;
      }
    }
    if (w_meta->w_state == SENT_PUT || w_meta->w_state == SENT_COMMIT ||
        w_meta->w_state == SENT_RELEASE) {
      if (!ack_m_id_found) {
        my_printf(red, "Wrkr %u, ack_ptr %u/%u received ack from m_i %u, state %u, received/expected %u/%u "
                    "active-machines/acks-seen: \n",
                  ctx->t_id, ack_ptr, PENDING_WRITES, ack_m_id,
                  w_meta->w_state, w_meta->acks_seen, w_meta->acks_expected);
        for (uint8_t i = 0; i < w_meta->acks_expected; i++) {
          my_printf(red, "%u/%u \n", w_meta->expected_ids[i], w_meta->seen_expected[i]);
        }
        if (!ENABLE_MULTICAST) assert(ack_m_id_found);
        else {
          MOD_INCR(ack_ptr, PENDING_WRITES);
          continue;
        }
      }
    }


    uint8_t w_state = w_meta->w_state;
//    printf("Wrkr %d valid ack %u/%u, from %u write at ptr %d is %u/%u \n",
//           t_id, ack_i, ack_num, ack_m_id, ack_ptr,
//           w_meta->acks_seen, w_meta->acks_expected);

    // If it's a quorum, the request has been completed -- but releases/writes/commits will
    // still hold a slot in the write FIFO until they see expected acks (or timeout)
    if (w_meta->acks_seen == REMOTE_QUORUM) {
      if (ENABLE_ASSERTIONS) ctx->qp_meta[W_QP_ID].outstanding_messages--;
//      printf("Wrkr %d valid ack %u/%u, write at ptr %d is ready \n",
//         t_id, ack_i, ack_num,  ack_ptr);
      switch(w_state) {
        case SENT_PUT : break;
        case SENT_RELEASE:
          clear_after_release_quorum(p_ops, ack_ptr, ctx->t_id);
          break;
        case SENT_COMMIT:
          act_on_quorum_of_commit_acks(p_ops, ack_ptr, ctx->t_id);
          break;
          // THE FOLLOWING ARE WAITING FOR A QUORUM
        case SENT_ACQUIRE:
        case SENT_RMW_ACQ_COMMIT:
        case SENT_BIT_VECTOR:
        case SENT_NO_OP_RELEASE:
          p_ops->w_meta[ack_ptr].w_state += W_STATE_OFFSET;
          break;
        default:
          if (w_state >= READY_PUT && w_state <= READY_NO_OP_RELEASE)
            break;
          my_printf(red, "Wrkr %u state %u, ptr %u \n", ctx->t_id, w_state, ack_ptr);
          assert(false);
      }
    }

    // Free writes/releases/commits
    if (w_meta->acks_seen == w_meta->acks_expected) {
      //assert(w_meta->acks_seen == REM_MACH_NUM);
      if (complete_requests_that_wait_all_acks(&w_meta->w_state, ack_ptr, ctx->t_id))
        update_sess_info_with_fully_acked_write(p_ops, ack_ptr, ctx->t_id);
    }
    MOD_INCR(ack_ptr, PENDING_WRITES);
  }
}

// Worker polls for acks
static inline void poll_acks(context_t *ctx)
{
  per_qp_meta_t *qp_meta = &ctx->qp_meta[ACK_QP_ID];
  fifo_t *recv_fifo = qp_meta->recv_fifo;
  p_ops_t *p_ops = (p_ops_t *) ctx->appl_ctx;
  int completed_messages =
    find_how_many_messages_can_be_polled(qp_meta->recv_cq, qp_meta->recv_wc,
                                         &qp_meta->completed_but_not_polled,
                                         qp_meta->recv_buf_slot_num, ctx->t_id);
  if (completed_messages <= 0) return;

  qp_meta->polled_messages = 0;
  while (qp_meta->polled_messages < completed_messages) {
    volatile ctx_ack_mes_ud_t *incoming_acks = (volatile ctx_ack_mes_ud_t *) qp_meta->recv_fifo->fifo;
    ctx_ack_mes_t *ack = (ctx_ack_mes_t *) &incoming_acks[recv_fifo->pull_ptr].ack;
    uint32_t ack_num = ack->ack_num;
    check_ack_message_count_stats(p_ops, ack, recv_fifo->pull_ptr, ack_num, ctx->t_id);

    fifo_incr_pull_ptr(recv_fifo);
    qp_meta->polled_messages++;

    uint64_t l_id = ack->l_id;
    uint64_t pull_lid = p_ops->local_w_id; // l_id at the pull pointer
    uint32_t ack_ptr; // a pointer in the FIFO, from where ack should be added
    //my_printf(green, "Receiving %u write credits, total %u,  from %u \n ",
    //          ack->credits, ctx->qp_meta[W_QP_ID].credits[ack->m_id], ack->m_id);
    ctx_increase_credits_on_polling_ack(ctx, ACK_QP_ID, ack);
    // if the pending write FIFO is empty it means the acks are for committed messages.
    if (p_ops->w_size == 0 ) {
      ack->opcode = INVALID_OPCODE;
      ack->ack_num = 0; continue;
    }
    if (pull_lid >= l_id) {
      if ((pull_lid - l_id) >= ack_num) {ack->opcode = 5;
        ack->ack_num = 0; continue;}
      ack_num -= (pull_lid - l_id);
      ack_ptr = p_ops->w_pull_ptr;
    }
    else { // l_id > pull_lid
      ack_ptr = (uint32_t) (p_ops->w_pull_ptr + (l_id - pull_lid)) % PENDING_WRITES;
    }
    // Apply the acks that refer to stored writes
    apply_acks(ack_num, ack_ptr, ack->m_id, l_id,
               pull_lid, ctx);
    if (ENABLE_ASSERTIONS) assert(ctx->qp_meta[W_QP_ID].credits[ack->m_id] <= W_CREDITS);
    ack->opcode = INVALID_OPCODE;
    ack->ack_num = 0;
  } // while

  if (qp_meta->polled_messages > 0) {
    if (ENABLE_ASSERTIONS) qp_meta->wait_for_reps_ctr = 0;
  }
  else {
    if (ENABLE_ASSERTIONS && ctx->qp_meta[W_QP_ID].outstanding_messages > 0)
      qp_meta->wait_for_reps_ctr++;
    if (ENABLE_STAT_COUNTING && ctx->qp_meta[W_QP_ID].outstanding_messages > 0)
      t_stats[ctx->t_id].stalled_ack++;
  }
  if (ENABLE_ASSERTIONS) assert(qp_meta->recv_info->posted_recvs >= qp_meta->polled_messages);
  qp_meta->recv_info->posted_recvs -= qp_meta->polled_messages;
}




//Poll for read replies
static inline void poll_for_read_replies(context_t *ctx)
{
  per_qp_meta_t *qp_meta = &ctx->qp_meta[PROP_REP_QP_ID];
  fifo_t *recv_fifo = qp_meta->recv_fifo;
  p_ops_t *p_ops = (p_ops_t *) ctx->appl_ctx;
  if (p_ops->r_rep_fifo->mes_size == R_REP_FIFO_SIZE) return;
  int completed_messages =
    find_how_many_messages_can_be_polled(qp_meta->recv_cq, qp_meta->recv_wc,
                                         &qp_meta->completed_but_not_polled,
                                         qp_meta->recv_buf_slot_num, ctx->t_id);
  if (completed_messages <= 0) return;
  qp_meta->polled_messages = 0;
  // Start polling
  while (qp_meta->polled_messages < completed_messages) {

    volatile r_rep_mes_ud_t *incoming_r_reps = (volatile r_rep_mes_ud_t *) qp_meta->recv_fifo->fifo;
    r_rep_mes_t *r_rep_mes = (r_rep_mes_t *) &incoming_r_reps[recv_fifo->pull_ptr].r_rep_mes;

    print_and_check_mes_when_polling_r_reps(r_rep_mes, recv_fifo->pull_ptr, ctx->t_id);
    bool is_propose = r_rep_mes->opcode == PROP_REPLY;
    bool is_accept = r_rep_mes->opcode == ACCEPT_REPLY ||
                     r_rep_mes->opcode == ACCEPT_REPLY_NO_CREDITS;
    if (r_rep_mes->opcode != ACCEPT_REPLY_NO_CREDITS)
      increase_credits_when_polling_r_reps(ctx, is_accept, r_rep_mes->m_id);

    qp_meta->polled_messages++;
    fifo_incr_pull_ptr(recv_fifo);
    // If it is a reply to a propose/accept only call a different handler
    if (is_propose || is_accept) {
      handle_rmw_rep_replies(p_ops, r_rep_mes, is_accept, ctx->t_id);
      continue;
    }
    check_state_with_allowed_flags(3, r_rep_mes->opcode, READ_REPLY, READ_PROP_REPLY);
    r_rep_mes->opcode = INVALID_OPCODE; // a random meaningless opcode
    uint8_t r_rep_num = r_rep_mes->coalesce_num;
    // Find the request that the reply is referring to
    uint64_t l_id = r_rep_mes->l_id;
    uint64_t pull_lid = p_ops->local_r_id; // l_id at the pull pointer
    uint32_t r_ptr; // a pointer in the FIFO, from where r_rep refers to

    // if the pending read FIFO is empty it means the r_reps are for committed messages.
    if (find_the_r_ptr_rep_refers_to(&r_ptr, l_id, pull_lid, p_ops,
                                     r_rep_mes->opcode, r_rep_num,  ctx->t_id)) {
      if (ENABLE_ASSERTIONS) assert(r_rep_mes->opcode == READ_REPLY); // there are no rmw reps
      continue;
    }

    uint16_t byte_ptr = PROP_REP_MES_HEADER;
    int read_i = -1; // count non-rmw read replies
    for (uint16_t i = 0; i < r_rep_num; i++) {
      struct r_rep_big *r_rep = (struct r_rep_big *)(((void *) r_rep_mes) + byte_ptr);
      //if (r_rep->opcode > CARTS_EQUAL) printf("big opcode comes \n");
      check_a_polled_r_rep(r_rep, r_rep_mes, i, r_rep_num, ctx->t_id);
      byte_ptr += get_size_from_opcode(r_rep->opcode);
      bool is_rmw_rep = opcode_is_rmw_rep(r_rep->opcode);
      //printf("Wrkr %u, polling read %u/%u opcode %u irs_rmw %u\n",
      //       t_id, i, r_rep_num, r_rep->opcode, is_rmw_rep);
      if (!is_rmw_rep) {
        read_i++;
        if (handle_single_r_rep(r_rep, &r_ptr, l_id, pull_lid, p_ops, read_i, i,
                                &ctx->qp_meta[PROP_QP_ID].outstanding_messages, ctx->t_id))
          continue;
      }
      else handle_single_rmw_rep(p_ops, (struct rmw_rep_last_committed *) r_rep,
        (struct rmw_rep_message *) r_rep_mes, byte_ptr, is_accept, i, ctx->t_id);
    }
    if (ENABLE_STAT_COUNTING) {
      if (ENABLE_ASSERTIONS) t_stats[ctx->t_id].per_worker_r_reps_received[r_rep_mes->m_id] += r_rep_num;
      t_stats[ctx->t_id].received_r_reps += r_rep_num;
      t_stats[ctx->t_id].received_r_reps_mes_num++;
    }
  }
  // Poll for the completion of the receives
  if (qp_meta->polled_messages > 0) {
    if (ENABLE_ASSERTIONS) assert(qp_meta->recv_info->posted_recvs >= qp_meta->polled_messages);
    qp_meta->recv_info->posted_recvs -= qp_meta->polled_messages;
    if (ENABLE_ASSERTIONS) qp_meta->wait_for_reps_ctr = 0;
  }
  else {
    if (ENABLE_ASSERTIONS && ctx->qp_meta[PROP_QP_ID].outstanding_messages > 0)
      qp_meta->wait_for_reps_ctr++;
    if (ENABLE_STAT_COUNTING && ctx->qp_meta[PROP_QP_ID].outstanding_messages > 0)
      t_stats[ctx->t_id].stalled_r_rep++;
  }
}



/* ---------------------------------------------------------------------------
//------------------------------ COMMITTING-------------------------------------
//---------------------------------------------------------------------------*/



static inline void cp_checks_at_loop_start(context_t *ctx)
{
  p_ops_t *p_ops = (p_ops_t *) ctx->appl_ctx;
  //if (ENABLE_ASSERTIONS && CHECK_DBG_COUNTERS)
  //  check_debug_cntrs(credit_debug_cnt, waiting_dbg_counter, p_ops,
  //                    (void *) cb->dgram_buf, r_buf_pull_ptr,
  //                    w_buf_pull_ptr, ack_buf_pull_ptr, r_rep_buf_pull_ptr, t_id);

  if (PUT_A_MACHINE_TO_SLEEP && (machine_id == MACHINE_THAT_SLEEPS) &&
      (t_stats[WORKERS_PER_MACHINE -1].cache_hits_per_thread > 4000000) && (!p_ops->debug_loop->slept)) {
    uint seconds = 15;
    if (ctx->t_id == 0) my_printf(yellow, "Workers are going to sleep for %u secs\n", seconds);
    sleep(seconds); p_ops->debug_loop->slept = true;
    if (ctx->t_id == 0) my_printf(green, "Worker %u is back\n", ctx->t_id);
  }
  if (ENABLE_INFO_DUMP_ON_STALL && print_for_debug) {
    //print_verbouse_debug_info(p_ops, t_id, credits);
  }
  if (ENABLE_ASSERTIONS) {
    if (ENABLE_ASSERTIONS && ctx->t_id == 0)  time_approx++;
    p_ops->debug_loop->loop_counter++;
    if (p_ops->debug_loop->loop_counter == M_16) {
      //if (t_id == 0) print_all_stalled_sessions(p_ops, t_id);

      //printf("Wrkr %u is working rectified keys %lu \n",
      //       t_id, t_stats[t_id].rectified_keys);

//        if (t_id == 0) {
//          printf("Wrkr %u sleeping machine bit %u, q-reads %lu, "
//                   "epoch_id %u, reqs %lld failed writes %lu, writes done %lu/%lu \n", t_id,
//                 conf_bit_vec[MACHINE_THAT_SLEEPS].bit,
//                 t_stats[t_id].quorum_reads, (uint16_t) epoch_id,
//                 t_stats[t_id].cache_hits_per_thread, t_stats[t_id].failed_rem_writes,
//                 t_stats[t_id].writes_sent, t_stats[t_id].writes_asked_by_clients);
//        }
      p_ops->debug_loop->loop_counter = 0;
    }
  }
}


static void cp_main_loop(context_t *ctx)
{
  p_ops_t *p_ops = (p_ops_t *) ctx->appl_ctx;
  while(true) {

    cp_checks_at_loop_start(ctx);

    batch_requests_to_KVS(ctx);
    ctx_send_broadcasts(ctx, PROP_QP_ID);
    ctx_poll_incoming_messages(ctx, PROP_QP_ID);

    broadcast_writes(ctx);

    poll_for_writes(ctx, W_QP_ID);

    od_send_acks(ctx, ACK_QP_ID);

    poll_for_reads(ctx);

    send_r_reps(ctx);

    poll_for_read_replies(ctx);

    inspect_rmws(p_ops, ctx->t_id);

    poll_acks(ctx);

    // Get a new batch from the trace, pass it through the kvs and create
    // the appropriate write/r_rep messages



  }
}

#endif /* CP_INLINE_UTIL_H */
