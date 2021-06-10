#ifndef CP_INLINE_UTIL_H
#define CP_INLINE_UTIL_H

//#include "kvs.h"
#include "od_hrd.h"

#include "od_inline_util.h"
#include "cp_generic_util.h"
#include "cp_kvs_util.h"
#include "cp_debug_util.h"
#include "cp_paxos_util.h"
#include "cp_reserve_stations_util.h"

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
    return;
  }
  for (uint16_t i = 0; i < SESSIONS_PER_THREAD; i++) {
    uint16_t sess_i = (uint16_t)((p_ops->last_session + i) % SESSIONS_PER_THREAD);
    if (od_pull_request_from_this_session(p_ops->stalled[sess_i], sess_i, ctx->t_id)) {
      working_session = sess_i;
      break;
    }
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
      if (p_ops->com_rob->capacity < COM_ROB_SIZE) {
        if (state == MUST_BCAST_COMMITS_FROM_HELP && loc_entry->helping_flag == PROPOSE_NOT_LOCALLY_ACKED) {
          my_printf(green, "Wrkr %u sess %u will bcast commits for the latest committed RMW,"
                      " after learning its proposed RMW has already been committed \n",
                    t_id, loc_entry->sess_id);
        }
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
  //ctx_refill_recvs(ctx, COM_QP_ID);
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

  if (od_is_ack_too_old(ack, p_ops->com_rob, p_ops->applied_com_id))
    return true;

  cp_apply_acks(ctx, ack);
  return true;
}


static inline void cp_bookkeep_commits(context_t *ctx)
{
  uint16_t com_num = 0;
  p_ops_t *p_ops = (p_ops_t *) ctx->appl_ctx;
  cp_com_rob_t *com_rob = (cp_com_rob_t *) get_fifo_pull_slot(p_ops->com_rob);

  while (com_rob->state == READY_COMMIT) {
    com_rob->state = INVALID;
    my_printf(green, "Commit sess %u commit %lu\n",
              com_rob->sess_id, p_ops->applied_com_id + com_num);

    fifo_incr_pull_ptr(p_ops->com_rob);
    fifo_decrem_capacity(p_ops->com_rob);
    com_rob = (cp_com_rob_t *) get_fifo_pull_slot(p_ops->com_rob);
    com_num++;
  }
  p_ops->applied_com_id += com_num;
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
    cp_bookkeep_commits(ctx);
  }
}

#endif /* CP_INLINE_UTIL_H */
