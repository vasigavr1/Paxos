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

  cp_ctx_t* cp_ctx = (cp_ctx_t*) ctx->appl_ctx;
  trace_op_t *ops = cp_ctx->ops;
  trace_t *trace = cp_ctx->trace_info.trace;

  uint16_t writes_num = 0, reads_num = 0, op_i = 0;
  int working_session = -1;
  // if there are clients the "all_sessions_stalled" flag is not used,
  // so we need not bother checking it
  if (!ENABLE_CLIENTS && cp_ctx->stall_info.all_stalled) {
    return;
  }
  for (uint16_t i = 0; i < SESSIONS_PER_THREAD; i++) {
    uint16_t sess_i = (uint16_t)((cp_ctx->trace_info.last_session + i) % SESSIONS_PER_THREAD);
    if (od_pull_request_from_this_session(cp_ctx->stall_info.stalled[sess_i], sess_i, ctx->t_id)) {
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
    fill_trace_op(ctx, cp_ctx, &ops[op_i], &trace[cp_ctx->trace_info.trace_iter],
                  working_session, ctx->t_id);

    // Find out next session to work on
    passed_over_all_sessions =
        od_find_next_working_session(ctx, &working_session,
                                     cp_ctx->stall_info.stalled,
                                     cp_ctx->trace_info.last_session,
                                     &cp_ctx->stall_info.all_stalled);
    if (!ENABLE_CLIENTS) {
      cp_ctx->trace_info.trace_iter++;
      if (trace[cp_ctx->trace_info.trace_iter].opcode == NOP) cp_ctx->trace_info.trace_iter = 0;
    }
    op_i++;
  }

  cp_ctx->trace_info.last_session = (uint16_t) working_session;
  t_stats[ctx->t_id].total_reqs += op_i;
  cp_KVS_batch_op_trace(op_i, ops, cp_ctx, ctx->t_id);
  for (uint16_t i = 0; i < op_i; i++) {
    insert_rmw(ctx, &ops[i], ctx->t_id);
  }
}

/* ---------------------------------------------------------------------------
//------------------------------ RMW FSM ----------------------------------
//---------------------------------------------------------------------------*/

//static inline void handle_accept_state(context_t *ctx,
//                                       loc_entry_t* loc_entry)
//{
//  check_sum_of_reps(loc_entry);
//  //printf("reps %u \n", loc_entry->rmw_reps.tot_replies);
//  if (loc_entry->rmw_reps.ready_to_inspect) {
//    loc_entry->rmw_reps.inspected = true;
//    inspect_accepts(ctx->appl_ctx, loc_entry, ctx->t_id);
//    check_state_with_allowed_flags(7, (int) loc_entry->state, ACCEPTED, INVALID_RMW, RETRY_WITH_BIGGER_TS,
//                                   NEEDS_KV_PTR, MUST_BCAST_COMMITS, MUST_BCAST_COMMITS_FROM_HELP);
//    if (ENABLE_ASSERTIONS && loc_entry->rmw_reps.ready_to_inspect)
//      assert(loc_entry->state == ACCEPTED && loc_entry->all_aboard);
//  }
//}

// Worker inspects its local RMW entries
static inline void inspect_rmws(context_t *ctx, uint16_t t_id)
{
  cp_ctx_t *cp_ctx = (cp_ctx_t *) ctx->appl_ctx;
  for (uint16_t sess_i = 0; sess_i < SESSIONS_PER_THREAD; sess_i++) {
    loc_entry_t* loc_entry = &cp_ctx->prop_info->entry[sess_i];
    uint8_t state = loc_entry->state;
    if (state == INVALID_RMW) continue;
    check_when_inspecting_rmw(loc_entry, &cp_ctx->stall_info, sess_i);

    if (state == ACCEPTED)
       inspect_accepts(cp_ctx, loc_entry, t_id);
    if (state == PROPOSED)
      inspect_proposes(ctx, loc_entry, t_id);



    /* =============== RETRY ======================== */
    if (loc_entry->state == RETRY_WITH_BIGGER_TS) {
      take_kv_ptr_with_higher_TS(cp_ctx, loc_entry, false, t_id);
      check_state_with_allowed_flags(5, (int) loc_entry->state, INVALID_RMW, PROPOSED,
                                     NEEDS_KV_PTR, MUST_BCAST_COMMITS);
      if (loc_entry->state == PROPOSED) {
        cp_prop_insert(ctx, loc_entry);
      }
    }

    if (loc_entry->state == MUST_BCAST_COMMITS ||
        loc_entry->state == MUST_BCAST_COMMITS_FROM_HELP) {
      if (inspect_commits(ctx, loc_entry, cp_ctx->com_rob->capacity))
        continue;
    }

    /* =============== NEEDS_KV_PTR ======================== */
    if (state == NEEDS_KV_PTR) {
      handle_needs_kv_ptr_state(ctx, loc_entry, sess_i, t_id);
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

static inline void rmw_prop_rep_helper(context_t *ctx)
{
  ctx_refill_recvs(ctx, ACC_QP_ID);
  send_rmw_rep_checks(ctx);
}


static inline void cp_send_ack_helper(context_t *ctx)
{
  //ctx_refill_recvs(ctx, COM_QP_ID);
}

static inline void cp_send_ack_debug(context_t *ctx,
                                     void* ack_ptr,
                                     uint32_t m_i)
{
  checks_stats_prints_when_sending_acks(ctx,
                                        (ctx_ack_mes_t *) ack_ptr,
                                        (uint8_t) m_i);
}

/* ---------------------------------------------------------------------------
//------------------------------ POLLING-------------------------------------
//---------------------------------------------------------------------------*/


static inline bool prop_recv_handler(context_t* ctx)
{
  cp_ctx_t *cp_ctx = (cp_ctx_t *) ctx->appl_ctx;
  per_qp_meta_t *qp_meta = &ctx->qp_meta[PROP_QP_ID];
  fifo_t *recv_fifo = qp_meta->recv_fifo;
  volatile cp_prop_mes_ud_t *incoming_props =
      (volatile cp_prop_mes_ud_t *) recv_fifo->fifo;
  cp_prop_mes_t *prop_mes = (cp_prop_mes_t *)
      &incoming_props[recv_fifo->pull_ptr].prop_mes;

  check_when_polling_for_props(ctx, prop_mes);

  uint8_t coalesce_num = prop_mes->coalesce_num;

  cp_ptrs_to_ops_t *ptrs_to_prop = cp_ctx->ptrs_to_ops;
  if (qp_meta->polled_messages == 0) ptrs_to_prop->polled_ops = 0;

  for (uint16_t i = 0; i < coalesce_num; i++) {
    cp_prop_t *prop = &prop_mes->prop[i];
    check_state_with_allowed_flags(2, prop->opcode, PROPOSE_OP);
    fill_ptr_to_ops_for_reps(ptrs_to_prop, (void *) prop,
                             (void *) prop_mes, i);
  }

  return true;
}

static inline bool acc_recv_handler(context_t* ctx)
{
  cp_ctx_t *cp_ctx = (cp_ctx_t *) ctx->appl_ctx;
  per_qp_meta_t *qp_meta = &ctx->qp_meta[ACC_QP_ID];
  fifo_t *recv_fifo = qp_meta->recv_fifo;
  volatile cp_acc_mes_ud_t *incoming_accs = (volatile cp_acc_mes_ud_t *) recv_fifo->fifo;
  cp_acc_mes_t *acc_mes = (cp_acc_mes_t *) &incoming_accs[recv_fifo->pull_ptr].acc_mes;

  check_when_polling_for_accs(ctx, acc_mes);

  uint8_t coalesce_num = acc_mes->coalesce_num;

  cp_ptrs_to_ops_t *ptrs_to_acc = cp_ctx->ptrs_to_ops;
  if (qp_meta->polled_messages == 0) ptrs_to_acc->polled_ops = 0;

  for (uint16_t i = 0; i < coalesce_num; i++) {
    cp_acc_t *acc = &acc_mes->acc[i];
    check_state_with_allowed_flags(2, acc->opcode, ACCEPT_OP);
    fill_ptr_to_ops_for_reps(ptrs_to_acc, (void *) acc,
                             (void *) acc_mes, i);
  }
  return true;
}

static inline bool rmw_recv_handler(context_t* ctx,
                                    uint16_t qp_id)
{
  cp_ctx_t *cp_ctx = (cp_ctx_t *) ctx->appl_ctx;
  per_qp_meta_t *qp_meta = &ctx->qp_meta[qp_id];
  fifo_t *recv_fifo = qp_meta->recv_fifo;
  volatile cp_acc_mes_ud_t *incoming_accs = (volatile cp_acc_mes_ud_t *) recv_fifo->fifo;
  cp_acc_mes_t *acc_mes = (cp_acc_mes_t *) &incoming_accs[recv_fifo->pull_ptr].acc_mes;

  check_when_polling_for_accs(ctx, acc_mes);

  uint8_t coalesce_num = acc_mes->coalesce_num;

  cp_ptrs_to_ops_t *ptrs_to_acc = cp_ctx->ptrs_to_ops;
  if (qp_meta->polled_messages == 0) ptrs_to_acc->polled_ops = 0;

  for (uint16_t i = 0; i < coalesce_num; i++) {
    cp_acc_t *acc = &acc_mes->acc[i];
    check_state_with_allowed_flags(2, acc->opcode, ACCEPT_OP);
    fill_ptr_to_ops_for_reps(ptrs_to_acc, (void *) acc,
                             (void *) acc_mes, i);
  }
  return true;
}



static inline bool cp_rmw_rep_recv_handler(context_t* ctx)
{
  cp_ctx_t *cp_ctx = (cp_ctx_t *) ctx->appl_ctx;
  per_qp_meta_t *qp_meta = &ctx->qp_meta[RMW_REP_QP_ID];
  fifo_t *recv_fifo = qp_meta->recv_fifo;
  volatile cp_rmw_rep_mes_ud_t *incoming_reps =
      (volatile cp_rmw_rep_mes_ud_t *) recv_fifo->fifo;
  cp_rmw_rep_mes_t *rep_mes =
      (cp_rmw_rep_mes_t *) &incoming_reps[recv_fifo->pull_ptr].rep_mes;

  bool is_accept = rep_mes->opcode == ACCEPT_REPLY;
  increment_prop_acc_credits(ctx, rep_mes, is_accept);
  handle_rmw_rep_replies(cp_ctx, rep_mes, is_accept, ctx->t_id);
  return true;
}



static inline bool cp_com_recv_handler(context_t* ctx)
{
  cp_ctx_t *cp_ctx = (cp_ctx_t *) ctx->appl_ctx;
  per_qp_meta_t *qp_meta = &ctx->qp_meta[COM_QP_ID];
  fifo_t *recv_fifo = qp_meta->recv_fifo;
  volatile cp_com_mes_ud_t *incoming_coms = (volatile cp_com_mes_ud_t *) recv_fifo->fifo;
  cp_com_mes_t *com_mes = (cp_com_mes_t *) &incoming_coms[recv_fifo->pull_ptr].com_mes;

  check_when_polling_for_coms(ctx, com_mes);
  //printf("received commit \n");

  uint8_t coalesce_num = com_mes->coalesce_num;
  bool can_send_acks = ctx_ack_insert(ctx, ACK_QP_ID, coalesce_num,  com_mes->l_id, com_mes->m_id);
  if (!can_send_acks) return false;

  cp_ptrs_to_ops_t *ptrs_to_com = cp_ctx->ptrs_to_ops;
  if (qp_meta->polled_messages == 0) ptrs_to_com->polled_ops = 0;

  for (uint16_t i = 0; i < coalesce_num; i++) {
    cp_com_t *com = &com_mes->com[i];
    check_state_with_allowed_flags(3, com->opcode, COMMIT_OP, COMMIT_OP_NO_VAL);
    ptrs_to_com->ptr_to_ops[ptrs_to_com->polled_ops] = (void *) com;
    ptrs_to_com->ptr_to_mes[ptrs_to_com->polled_ops] = (void *) com_mes;
    ptrs_to_com->polled_ops++;
  }
  return true;
}

static inline bool cp_ack_recv_handler(context_t *ctx)
{
  cp_ctx_t *cp_ctx = (cp_ctx_t *) ctx->appl_ctx;
  per_qp_meta_t *qp_meta = &ctx->qp_meta[ACK_QP_ID];
  fifo_t *recv_fifo = qp_meta->recv_fifo;
  volatile ctx_ack_mes_ud_t *incoming_acks = (volatile ctx_ack_mes_ud_t *) recv_fifo->fifo;
  ctx_ack_mes_t *ack = (ctx_ack_mes_t *) &incoming_acks[recv_fifo->pull_ptr].ack;

  ctx_increase_credits_on_polling_ack(ctx, ACK_QP_ID, ack);

  if (od_is_ack_too_old(ack, cp_ctx->com_rob, cp_ctx->l_ids.applied_com_id))
    return true;

  cp_apply_acks(ctx, ack);
  return true;
}


static inline void cp_bookkeep_commits(context_t *ctx)
{
  uint16_t com_num = 0;
  cp_ctx_t *cp_ctx = (cp_ctx_t *) ctx->appl_ctx;
  cp_com_rob_t *com_rob = (cp_com_rob_t *) get_fifo_pull_slot(cp_ctx->com_rob);

  while (com_rob->state == READY_COMMIT) {
    com_rob->state = INVALID;
    if (DEBUG_COMMITS)
      my_printf(green, "Commit sess %u commit %lu\n",
               com_rob->sess_id, cp_ctx->l_ids.applied_com_id + com_num);

    fifo_incr_pull_ptr(cp_ctx->com_rob);
    fifo_decrem_capacity(cp_ctx->com_rob);
    com_rob = (cp_com_rob_t *) get_fifo_pull_slot(cp_ctx->com_rob);
    com_num++;
  }
  cp_ctx->l_ids.applied_com_id += com_num;
}



/* ---------------------------------------------------------------------------
//------------------------------ COMMITTING-------------------------------------
//---------------------------------------------------------------------------*/



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
  if (ENABLE_INFO_DUMP_ON_STALL && print_for_debug) {
    //print_verbouse_debug_info(cp_ctx, t_id, credits);
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


_Noreturn static void cp_main_loop(context_t *ctx)
{
  cp_ctx_t *cp_ctx = (cp_ctx_t *) ctx->appl_ctx;
  while(true) {

    cp_checks_at_loop_start(ctx);

    batch_requests_to_KVS(ctx);
    ctx_send_broadcasts(ctx, PROP_QP_ID);
    for (uint16_t qp_i = 0; qp_i < QP_NUM; qp_i ++)
      ctx_poll_incoming_messages(ctx, qp_i);

    ctx_send_unicasts(ctx, RMW_REP_QP_ID);
    //ctx_send_unicasts(ctx, ACC_REP_QP_ID);
    od_send_acks(ctx, ACK_QP_ID);

    inspect_rmws(ctx, ctx->t_id);
    ctx_send_broadcasts(ctx, ACC_QP_ID);
    ctx_send_broadcasts(ctx, COM_QP_ID);
    cp_bookkeep_commits(ctx);
  }
}

#endif /* CP_INLINE_UTIL_H */
