//
// Created by vasilis on 22/05/20.
//

#ifndef CP_RESERVE_STATIONS_UTIL_H
#define CP_RESERVE_STATIONS_UTIL_H


#include <od_inline_util.h>
#include "od_wrkr_side_calls.h"
#include "od_latency_util.h"
#include "cp_main.h"
#include "cp_debug_util.h"
#include "cp_paxos_util.h"
#include "cp_paxos_generic_util.h"
#include "cp_config.h"

//-------------------------------------------------------------------------------------
// -------------------------------FORWARD DECLARATIONS--------------------------------
//-------------------------------------------------------------------------------------
static inline void fill_commit_message_from_l_entry(struct commit *com, loc_entry_t *loc_entry,
                                                    uint8_t broadcast_state, uint16_t t_id);
static inline void create_prop_rep(cp_prop_t *,
                                   cp_prop_mes_t *prop_mes,
                                   cp_rmw_rep_t *,
                                   mica_op_t *,
                                   uint16_t t_id);
static inline void create_acc_rep(cp_acc_t *acc,
                                  cp_acc_mes_t *acc_mes,
                                  cp_rmw_rep_t *acc_rep,
                                  mica_op_t *kv_ptr,
                                  uint16_t t_id);

static inline void act_on_quorum_of_commit_acks(p_ops_t *p_ops,
                                                uint32_t ack_ptr,
                                                uint16_t t_id);


// Fill the trace_op to be passed to the KVS. Returns whether no more requests can be processed
static inline bool fill_trace_op(context_t *ctx,
                                 p_ops_t *p_ops, trace_op_t *op,
                                 trace_t *trace,
                                 int working_session,
                                 uint16_t t_id)
{
  create_inputs_of_op(&op->value_to_write, &op->value_to_read, &op->real_val_len,
                      &op->opcode, &op->index_to_req_array,
                      &op->key, op->value, trace, working_session, t_id);
  if (!ENABLE_CLIENTS) check_trace_req(p_ops, trace, op, working_session, t_id);


  bool is_rmw = opcode_is_rmw(op->opcode);
  if (ENABLE_ASSERTIONS) assert(is_rmw);
  if (ENABLE_ASSERTIONS && !ENABLE_CLIENTS && op->opcode == FETCH_AND_ADD) {
    assert(is_rmw);
    assert(op->value_to_write == op->value);
    assert(*(uint64_t *) op->value_to_write == 1);
  }
  if (is_rmw && ENABLE_ALL_ABOARD) {
    op->attempt_all_aboard = ctx->q_info->missing_num == 0;
  }
  increment_per_req_counters(op->opcode, t_id);
  if (ENABLE_ASSERTIONS) assert(!p_ops->stalled[working_session]);
  p_ops->stalled[working_session] = true;
  op->session_id = (uint16_t) working_session;

  if (ENABLE_ASSERTIONS && DEBUG_SESSIONS)
    p_ops->debug_loop->ses_dbg->dbg_cnt[working_session] = 0;
  //if (w_pull_ptr[[working_session]] == 100000) my_printf(yellow, "Working ses %u \n", working_session);
  //my_printf(yellow, "BEFORE: OP_i %u -> session %u, opcode: %u \n", op_i, working_session, ops[op_i].opcode);
  //my_printf(yellow, "Wrkr %u, session %u, opcode %u \n", t_id, working_session, op->opcode);

  if (ENABLE_CLIENTS) {
    signal_in_progress_to_client(op->session_id, op->index_to_req_array, t_id);
    if (ENABLE_ASSERTIONS) assert(interface[t_id].wrkr_pull_ptr[working_session] == op->index_to_req_array);
    MOD_INCR(interface[t_id].wrkr_pull_ptr[working_session], PER_SESSION_REQ_NUM);
  }
}



/* ---------------------------------------------------------------------------
//------------------------------ Inserting-utility----------------------------
//---------------------------------------------------------------------------*/

static inline void cp_fill_prop(cp_prop_t *prop,
                                loc_entry_t *loc_entry,
                                uint16_t t_id)
{
  check_loc_entry_metadata_is_reset(loc_entry, "insert_prop_to_read_fifo", t_id);
  assign_ts_to_netw_ts(&prop->ts, &loc_entry->new_ts);
  memcpy(&prop->key, (void *)&loc_entry->key, KEY_SIZE);
  prop->opcode = PROPOSE_OP;
  prop->l_id = loc_entry->l_id;
  prop->t_rmw_id = loc_entry->rmw_id.id;
  prop->log_no = loc_entry->log_no;
  if (!loc_entry->base_ts_found)
    prop->base_ts = loc_entry->base_ts;
  else prop->base_ts.version = DO_NOT_CHECK_BASE_TS;
  if (ENABLE_ASSERTIONS) {
    assert(prop->ts.version >= PAXOS_TS);
  }
}

static inline void cp_fill_acc(cp_acc_t *acc,
                               loc_entry_t *loc_entry,
                               bool helping,
                               uint16_t t_id)
{
  check_loc_entry_metadata_is_reset(loc_entry, "insert_accept_in_writes_message_fifo", t_id);
  if (ENABLE_ASSERTIONS) assert(loc_entry->helping_flag != PROPOSE_NOT_LOCALLY_ACKED);
  if (DEBUG_RMW) {
    my_printf(yellow, "Wrkr %u Inserting an accept, l_id %lu, "
                      "rmw_id %lu, log %u,  helping %u,\n",
              t_id, loc_entry->l_id, loc_entry->rmw_id.id,
              loc_entry->log_no, helping);
  }

  acc->l_id = loc_entry->l_id;
  acc->t_rmw_id = loc_entry->rmw_id.id;
  acc->base_ts = loc_entry->base_ts;
  assign_ts_to_netw_ts(&acc->ts, &loc_entry->new_ts);
  memcpy(&acc->key, &loc_entry->key, KEY_SIZE);
  acc->opcode = ACCEPT_OP;
  if (!helping && !loc_entry->rmw_is_successful)
    memcpy(acc->value, loc_entry->value_to_read, (size_t) RMW_VALUE_SIZE);
  else memcpy(acc->value, loc_entry->value_to_write, (size_t) RMW_VALUE_SIZE);
  acc->log_no = loc_entry->log_no;
  acc->val_len = (uint8_t) loc_entry->rmw_val_len;
}


static inline void cp_insert_prop_help(context_t *ctx, void* prop_ptr,
                                       void *source, uint32_t source_flag)
{
  per_qp_meta_t *qp_meta = &ctx->qp_meta[PROP_QP_ID];
  fifo_t *send_fifo = qp_meta->send_fifo;
  p_ops_t *p_ops = (p_ops_t *) ctx->appl_ctx;

  cp_prop_t *prop = (cp_prop_t *) prop_ptr;
  loc_entry_t *loc_entry = (loc_entry_t *) source;

  cp_fill_prop(prop, loc_entry, ctx->t_id);

  slot_meta_t *slot_meta = get_fifo_slot_meta_push(send_fifo);
  cp_prop_mes_t *prop_mes = (cp_prop_mes_t *) get_fifo_push_slot(send_fifo);
  prop_mes->coalesce_num = (uint8_t) slot_meta->coalesce_num;
  // If it's the first message give it an lid
  if (slot_meta->coalesce_num == 1) {
    prop_mes->l_id = p_ops->inserted_prop_id;
    p_ops->inserted_prop_id++;
  }
}


static inline uint64_t get_rmw_mes_l_id(void *mes, bool is_accept)
{
  return is_accept?
    ((cp_acc_mes_t *) mes)->l_id :
    ((cp_prop_mes_t *) mes)->l_id;
}

static inline uint8_t get_rmw_mes_m_id(void *mes, bool is_accept)
{
  return is_accept?
         ((cp_acc_mes_t *) mes)->m_id :
         ((cp_prop_mes_t *) mes)->m_id;
}

static inline void cp_insert_rmw_rep_helper(context_t *ctx,
                                            cp_rmw_rep_t *rep,
                                            mica_op_t *kv_ptr,
                                            uint32_t op_i,
                                            uint8_t qp_id)
{
  p_ops_t *p_ops = (p_ops_t *) ctx->appl_ctx;
  per_qp_meta_t *qp_meta = &ctx->qp_meta[qp_id];
  fifo_t *send_fifo = qp_meta->send_fifo;
  cp_ptrs_to_ops_t *ptrs_to_ops = p_ops->ptrs_to_ops;
  bool is_accept = qp_id == ACC_QP_ID;
  void *op = ptrs_to_ops->ptr_to_ops[op_i];
  void *mes = ptrs_to_ops->ptr_to_mes[op_i];

  is_accept ? create_acc_rep(op, mes, rep, kv_ptr, ctx->t_id) :
              create_prop_rep(op, mes, rep, kv_ptr, ctx->t_id);

  slot_meta_t *slot_meta = get_fifo_slot_meta_push(send_fifo);
  uint16_t rep_size = get_size_from_opcode(rep->opcode) - RMW_REP_SMALL_SIZE;
  slot_meta->byte_size += rep_size;

  cp_rmw_rep_mes_t *rep_mes = (cp_rmw_rep_mes_t *) get_fifo_push_slot(send_fifo);
  if (slot_meta->coalesce_num == 1) {
    rep_mes->l_id = get_rmw_mes_l_id(mes, is_accept);
    slot_meta->rm_id = get_rmw_mes_m_id(mes, is_accept);
    rep_mes->opcode = PROP_REPLY; //TODO remove the opcode field
  }
}


static inline void cp_insert_prop_rep_helper(context_t *ctx, void* prop_rep_ptr,
                                             void *source, uint32_t op_i)
{
  cp_insert_rmw_rep_helper(ctx, (cp_rmw_rep_t *) prop_rep_ptr,
                           (mica_op_t *) source, op_i, PROP_QP_ID);
}

static inline void cp_insert_acc_rep_helper(context_t *ctx, void* acc_rep_ptr,
                                             void *source, uint32_t op_i)
{
  cp_insert_rmw_rep_helper(ctx, (cp_rmw_rep_t *) acc_rep_ptr,
                           (mica_op_t *) source, op_i, ACC_QP_ID);
}


static inline void cp_insert_acc_help(context_t *ctx, void* acc_ptr,
                                      void *source, uint32_t source_flag)
{
  per_qp_meta_t *qp_meta = &ctx->qp_meta[ACC_QP_ID];
  fifo_t *send_fifo = qp_meta->send_fifo;
  p_ops_t *p_ops = (p_ops_t *) ctx->appl_ctx;

  cp_acc_t *acc = (cp_acc_t *) acc_ptr;
  loc_entry_t *loc_entry = (loc_entry_t *) source;
  cp_fill_acc(acc, loc_entry, (bool) source_flag, ctx->t_id);

  slot_meta_t *slot_meta = get_fifo_slot_meta_push(send_fifo);
  cp_acc_mes_t *acc_mes = (cp_acc_mes_t *) get_fifo_push_slot(send_fifo);
  acc_mes->coalesce_num = (uint8_t) slot_meta->coalesce_num;

  if (slot_meta->coalesce_num == 1) {
    acc_mes->l_id = p_ops->inserted_acc_id;
    p_ops->inserted_acc_id++;
  }
}

static inline void fill_com_rob_entry(p_ops_t *p_ops,
                                      loc_entry_t *loc_entry)
{
  cp_com_rob_t *com_rob = (cp_com_rob_t *) get_fifo_push_slot(p_ops->com_rob);
  if (ENABLE_ASSERTIONS) {
    assert(com_rob->state == INVALID);
    assert(p_ops->stalled[loc_entry->sess_id]);
  }
  com_rob->state = VALID;
  com_rob->sess_id = loc_entry->sess_id;
  com_rob->acks_seen = 0;
  com_rob->l_id = p_ops->inserted_com_id;
  fifo_incr_push_ptr(p_ops->com_rob);
  fifo_increm_capacity(p_ops->com_rob);
}

static inline void cp_insert_com_help(context_t *ctx, void* com_ptr,
                                      void *source, uint32_t source_flag)
{
  per_qp_meta_t *qp_meta = &ctx->qp_meta[ACC_QP_ID];
  fifo_t *send_fifo = qp_meta->send_fifo;
  p_ops_t *p_ops = (p_ops_t *) ctx->appl_ctx;
  cp_com_t *com = (cp_com_t *) com_ptr;
  loc_entry_t *loc_entry = (loc_entry_t *) source;

  fill_commit_message_from_l_entry(com, loc_entry, source_flag, ctx->t_id);

  slot_meta_t *slot_meta = get_fifo_slot_meta_push(send_fifo);
  cp_com_mes_t *com_mes = (cp_com_mes_t *) get_fifo_push_slot(send_fifo);
  com_mes->coalesce_num = (uint8_t) slot_meta->coalesce_num;

  if (slot_meta->coalesce_num == 1) {
    com_mes->l_id = p_ops->inserted_com_id;
    fifo_set_push_backward_ptr(send_fifo, p_ops->com_rob->push_ptr); // for debug
  }

  fill_com_rob_entry(p_ops, loc_entry);
  p_ops->inserted_com_id++;
}


// Apply the acks that refer to stored writes
static inline void cp_apply_acks(context_t *ctx,
                                 ctx_ack_mes_t *ack)
{
  p_ops_t *p_ops = (p_ops_t *) ctx->appl_ctx;
  uint32_t ack_num = ack->ack_num;
  uint64_t pull_lid = p_ops->applied_com_id;
  uint32_t ack_ptr =
      ctx_find_when_the_ack_points_acked(ack, p_ops->com_rob, pull_lid, &ack_num);

  for (uint32_t ack_i = 0; ack_i < ack_num; ack_i++) {
    cp_com_rob_t *com_rob = (cp_com_rob_t *) get_fifo_slot(p_ops->com_rob, ack_ptr);
    com_rob->acks_seen++;
    cp_check_ack_and_print(ctx, com_rob, ack, ack_i, ack_ptr, ack_num);
    if (com_rob->acks_seen == REMOTE_QUORUM) {
      act_on_quorum_of_commit_acks(&p_ops->prop_info[com_rob->sess_id], ack_ptr, ctx->t_id);
      com_rob->state = READY_COMMIT;
    }
    MOD_INCR(ack_ptr, COM_ROB_SIZE);
  }
}




#endif //CP_RESERVE_STATIONS_UTIL_H
