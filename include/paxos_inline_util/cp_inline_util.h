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
    insert_rmw(ctx, &ops[i], ctx->t_id);
  }
}

/* ---------------------------------------------------------------------------
//------------------------------ RMW FSM ----------------------------------
//---------------------------------------------------------------------------*/

// Worker inspects its local RMW entries
static inline void inspect_rmws(context_t *ctx, uint16_t t_id)
{
  p_ops_t *p_ops = (p_ops_t *) ctx->appl_ctx;
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
        inspect_proposes(ctx, loc_entry, t_id);
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
        cp_com_insert(ctx, loc_entry, state);
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
        cp_prop_insert(ctx, loc_entry);
      }
    }

    /* =============== NEEDS_KV_PTR ======================== */
    if (state == NEEDS_KV_PTR) {
      handle_needs_kv_ptr_state(ctx, loc_entry, sess_i, t_id);
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

static inline void send_accs_helper(context_t *ctx)
{
  send_acc_checks(ctx);
}


static inline void cp_send_coms_helper(context_t *ctx)
{
  send_com_checks(ctx);
}


/* ---------------------------------------------------------------------------
//------------------------------ UNICASTS-------------------------------------
//---------------------------------------------------------------------------*/

static inline void cp_prop_rep_helper(context_t *ctx)
{
  send_rmw_rep_checks(ctx, PROP_REP_QP_ID);
}

static inline void cp_acc_rep_helper(context_t *ctx)
{
  send_rmw_rep_checks(ctx, ACC_REP_QP_ID);
}


static inline void cp_send_ack_helper(context_t *ctx)
{

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

  cp_ptrs_to_ops_t *ptrs_to_prop = p_ops->ptrs_to_ops;
  if (qp_meta->polled_messages == 0) ptrs_to_prop->polled_ops = 0;

  for (uint16_t i = 0; i < coalesce_num; i++) {
    cp_prop_t *prop = &prop_mes->prop[i];
    check_state_with_allowed_flags(2, prop->opcode, PROPOSE_OP);
    ptrs_to_prop->ptr_to_ops[ptrs_to_prop->polled_ops] = (void *) prop;
    ptrs_to_prop->ptr_to_mes[ptrs_to_prop->polled_ops] = (void *) prop_mes;
    ptrs_to_prop->break_message[ptrs_to_prop->polled_ops] = i == 0;
    ptrs_to_prop->polled_ops++;
  }

  return true;
}

static inline bool acc_recv_handler(context_t* ctx)
{
  p_ops_t *p_ops = (p_ops_t *) ctx->appl_ctx;
  per_qp_meta_t *qp_meta = &ctx->qp_meta[ACC_QP_ID];
  fifo_t *recv_fifo = qp_meta->recv_fifo;
  volatile cp_acc_mes_ud_t *incoming_accs = (volatile cp_acc_mes_ud_t *) recv_fifo->fifo;
  cp_acc_mes_t *acc_mes = (cp_acc_mes_t *) &incoming_accs[recv_fifo->pull_ptr].acc_mes;

  check_when_polling_for_accs(ctx, acc_mes);

  uint8_t coalesce_num = acc_mes->coalesce_num;

  cp_ptrs_to_ops_t *ptrs_to_acc = p_ops->ptrs_to_ops;
  if (qp_meta->polled_messages == 0) ptrs_to_acc->polled_ops = 0;

  for (uint16_t i = 0; i < coalesce_num; i++) {
    cp_acc_t *acc = &acc_mes->acc[i];
    check_state_with_allowed_flags(2, acc->opcode, ACCEPT_OP);
    ptrs_to_acc->ptr_to_ops[ptrs_to_acc->polled_ops] = (void *) acc;
    ptrs_to_acc->ptr_to_mes[ptrs_to_acc->polled_ops] = (void *) acc_mes;
    ptrs_to_acc->break_message[ptrs_to_acc->polled_ops] = i == 0;
    ptrs_to_acc->polled_ops++;
  }
  return true;
}

static inline void cp_rmw_rep_recv_handler(context_t* ctx, uint16_t qp_id)
{
  p_ops_t *p_ops = (p_ops_t *) ctx->appl_ctx;
  per_qp_meta_t *qp_meta = &ctx->qp_meta[qp_id];
  fifo_t *recv_fifo = qp_meta->recv_fifo;
  volatile cp_rmw_rep_mes_ud_t *incoming_reps =
      (volatile cp_rmw_rep_mes_ud_t *) recv_fifo->fifo;
  cp_rmw_rep_mes_t *rep_mes =
      (cp_rmw_rep_mes_t *) &incoming_reps[recv_fifo->pull_ptr].rep_mes;


  bool is_accept = qp_id == ACC_QP_ID;
  handle_rmw_rep_replies(p_ops, rep_mes, is_accept, ctx->t_id);

  //if (ENABLE_STAT_COUNTING) {
  //  if (ENABLE_ASSERTIONS)
  //    t_stats[ctx->t_id].per_worker_r_reps_received[prop_rep_mes->m_id] += prop_rep_mes->coalesce_num;
  //  t_stats[ctx->t_id].received_r_reps += prop_rep_mes->coalesce_num;
  //  t_stats[ctx->t_id].received_r_reps_mes_num++;
  //}
}

static inline bool cp_prop_rep_recv_handler(context_t* ctx)
{
  cp_rmw_rep_recv_handler(ctx, PROP_QP_ID);
  return true;
}

static inline bool cp_acc_rep_recv_handler(context_t* ctx)
{
  cp_rmw_rep_recv_handler(ctx, ACC_QP_ID);
  return true;
}

static inline bool cp_com_recv_handler(context_t* ctx)
{
  p_ops_t *p_ops = (p_ops_t *) ctx->appl_ctx;
  per_qp_meta_t *qp_meta = &ctx->qp_meta[ACC_QP_ID];
  fifo_t *recv_fifo = qp_meta->recv_fifo;
  volatile cp_com_mes_ud_t *incoming_coms = (volatile cp_com_mes_ud_t *) recv_fifo->fifo;
  cp_com_mes_t *com_mes = (cp_com_mes_t *) &incoming_coms[recv_fifo->pull_ptr].com_mes;

  check_when_polling_for_coms(ctx, com_mes);

  uint8_t coalesce_num = com_mes->coalesce_num;
  bool can_send_acks = ctx_ack_insert(ctx, ACK_QP_ID, coalesce_num,  com_mes->l_id, com_mes->m_id);
  if (!can_send_acks) return false;

  cp_ptrs_to_ops_t *ptrs_to_com = p_ops->ptrs_to_ops;
  if (qp_meta->polled_messages == 0) ptrs_to_com->polled_ops = 0;

  for (uint16_t i = 0; i < coalesce_num; i++) {
    cp_com_t *com = &com_mes->com[i];
    check_state_with_allowed_flags(2, com->opcode, COMMIT_OP);
    ptrs_to_com->ptr_to_ops[ptrs_to_com->polled_ops] = (void *) com;
    ptrs_to_com->ptr_to_mes[ptrs_to_com->polled_ops] = (void *) com_mes;
    ptrs_to_com->polled_ops++;
  }
  return true;
}

inline bool cp_ack_recv_handler(context_t *ctx)
{
  p_ops_t *p_ops = (p_ops_t *) ctx->appl_ctx;
  per_qp_meta_t *qp_meta = &ctx->qp_meta[ACK_QP_ID];
  fifo_t *recv_fifo = qp_meta->recv_fifo;
  volatile ctx_ack_mes_ud_t *incoming_acks = (volatile ctx_ack_mes_ud_t *) recv_fifo->fifo;
  ctx_ack_mes_t *ack = (ctx_ack_mes_t *) &incoming_acks[recv_fifo->pull_ptr].ack;

  ctx_increase_credits_on_polling_ack(ctx, ACK_QP_ID, ack);

  if (od_is_ack_too_old(ack, p_ops->com_rob, p_ops->applied_com_id;))
    return true;

  cp_apply_acks(ctx, ack);
  return true;
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
    //          ack->credits, ctx->qp_meta[ACC_QP_ID].credits[ack->m_id], ack->m_id);
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
    if (ENABLE_ASSERTIONS) assert(ctx->qp_meta[ACC_QP_ID].credits[ack->m_id] <= W_CREDITS);
    ack->opcode = INVALID_OPCODE;
    ack->ack_num = 0;
  } // while

  if (qp_meta->polled_messages > 0) {
    if (ENABLE_ASSERTIONS) qp_meta->wait_for_reps_ctr = 0;
  }
  else {
    if (ENABLE_ASSERTIONS && ctx->qp_meta[ACC_QP_ID].outstanding_messages > 0)
      qp_meta->wait_for_reps_ctr++;
    if (ENABLE_STAT_COUNTING && ctx->qp_meta[ACC_QP_ID].outstanding_messages > 0)
      t_stats[ctx->t_id].stalled_ack++;
  }
  if (ENABLE_ASSERTIONS) assert(qp_meta->recv_info->posted_recvs >= qp_meta->polled_messages);
  qp_meta->recv_info->posted_recvs -= qp_meta->polled_messages;
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


_Noreturn static void cp_main_loop(context_t *ctx)
{
  p_ops_t *p_ops = (p_ops_t *) ctx->appl_ctx;
  while(true) {

    cp_checks_at_loop_start(ctx);

    batch_requests_to_KVS(ctx);
    ctx_send_broadcasts(ctx, PROP_QP_ID);
    for (uint16_t qp_i = 0; qp_i < QP_NUM; qp_i ++)
      ctx_poll_incoming_messages(ctx, qp_i);

    ctx_send_unicasts(ctx, PROP_REP_QP_ID);
    ctx_send_unicasts(ctx, ACC_REP_QP_ID);
    od_send_acks(ctx, ACK_QP_ID);

    inspect_rmws(ctx, ctx->t_id);
    ctx_send_broadcasts(ctx, ACC_QP_ID);
    ctx_send_broadcasts(ctx, COM_QP_ID);
  }
}

#endif /* CP_INLINE_UTIL_H */
