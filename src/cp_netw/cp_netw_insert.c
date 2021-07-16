//
// Created by vasilis on 16/07/2021.
//

#include "cp_core_interface.h"
#include "cp_debug_util.h"
#include <cp_inline_util.h>




/* ---------------------------------------------------------------------------
//------------------------------ Inserting-utility----------------------------
//---------------------------------------------------------------------------*/



inline void cp_insert_prop_help(context_t *ctx, void* prop_ptr,
                                       void *source, uint32_t source_flag)
{
  per_qp_meta_t *qp_meta = &ctx->qp_meta[PROP_QP_ID];
  fifo_t *send_fifo = qp_meta->send_fifo;
  cp_ctx_t *cp_ctx = (cp_ctx_t *) ctx->appl_ctx;

  cp_prop_t *prop = (cp_prop_t *) prop_ptr;

  cp_fill_prop(prop, source, ctx->t_id);

  slot_meta_t *slot_meta = get_fifo_slot_meta_push(send_fifo);
  cp_prop_mes_t *prop_mes = (cp_prop_mes_t *) get_fifo_push_slot(send_fifo);
  prop_mes->coalesce_num = (uint8_t) slot_meta->coalesce_num;
  // If it's the first message give it an lid
  if (slot_meta->coalesce_num == 1) {
    prop_mes->l_id = cp_ctx->l_ids.inserted_prop_id;
    cp_ctx->l_ids.inserted_prop_id++;
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


inline void cp_insert_rmw_rep_helper(context_t *ctx,
                                     void* prop_rep_ptr,
                                     void *kv_ptr,
                                     uint32_t source_flag)
{
  cp_ctx_t *cp_ctx = (cp_ctx_t *) ctx->appl_ctx;
  per_qp_meta_t *qp_meta = &ctx->qp_meta[RMW_REP_QP_ID];
  fifo_t *send_fifo = qp_meta->send_fifo;
  cp_ptrs_to_ops_t *ptrs_to_ops = cp_ctx->ptrs_to_ops;
  rmw_rep_flag_t  *flag = (rmw_rep_flag_t *) &source_flag;

  void *op = ptrs_to_ops->ptr_to_ops[flag->op_i];
  void *mes = ptrs_to_ops->ptr_to_mes[flag->op_i];
  cp_rmw_rep_t * rep = (cp_rmw_rep_t *) prop_rep_ptr;

  flag->is_accept ? create_acc_rep(op, mes, rep, (mica_op_t *) kv_ptr, ctx->t_id) :
  create_prop_rep(op, mes, rep, (mica_op_t *) kv_ptr, ctx->t_id);

  slot_meta_t *slot_meta = get_fifo_slot_meta_push(send_fifo);
  uint16_t rep_size = get_size_from_opcode(rep->opcode) - RMW_REP_SMALL_SIZE;
  slot_meta->byte_size += rep_size;

  cp_rmw_rep_mes_t *rep_mes = (cp_rmw_rep_mes_t *) get_fifo_push_slot(send_fifo);
  if (slot_meta->coalesce_num == 1) {
    rep_mes->l_id = get_rmw_mes_l_id(mes, flag->is_accept);
    slot_meta->rm_id = get_rmw_mes_m_id(mes, flag->is_accept);
    rep_mes->opcode = flag->is_accept? ACCEPT_REPLY : PROP_REPLY;
    rep_mes->m_id = ctx->m_id;
  }

  rep_mes->coalesce_num = slot_meta->coalesce_num;
}

inline void cp_insert_acc_help(context_t *ctx,
                               void* acc_ptr,
                               void *source,
                               uint32_t source_flag)
{
  per_qp_meta_t *qp_meta = &ctx->qp_meta[ACC_QP_ID];
  fifo_t *send_fifo = qp_meta->send_fifo;
  cp_ctx_t *cp_ctx = (cp_ctx_t *) ctx->appl_ctx;

  cp_acc_t *acc = (cp_acc_t *) acc_ptr;
  cp_fill_acc(acc, source, (bool) source_flag, ctx->t_id);

  slot_meta_t *slot_meta = get_fifo_slot_meta_push(send_fifo);
  cp_acc_mes_t *acc_mes = (cp_acc_mes_t *) get_fifo_push_slot(send_fifo);
  acc_mes->coalesce_num = (uint8_t) slot_meta->coalesce_num;

  if (slot_meta->coalesce_num == 1) {
    acc_mes->l_id = cp_ctx->l_ids.inserted_acc_id;
    cp_ctx->l_ids.inserted_acc_id++;
  }
}

static inline void fill_com_rob_entry(cp_ctx_t *cp_ctx,
                                      uint16_t sess_id)
{
  cp_com_rob_t *com_rob = (cp_com_rob_t *) get_fifo_push_slot(cp_ctx->com_rob);
  if (ENABLE_ASSERTIONS) {
    assert(com_rob->state == INVALID);
    assert(cp_ctx->stall_info.stalled[sess_id]);
  }
  com_rob->state = VALID;
  com_rob->sess_id = sess_id;
  com_rob->acks_seen = 0;
  com_rob->l_id = cp_ctx->l_ids.inserted_com_id;
  fifo_incr_push_ptr(cp_ctx->com_rob);
  fifo_increm_capacity(cp_ctx->com_rob);
}

inline void cp_insert_com_help(context_t *ctx,
                               void* com_ptr,
                               void *source,
                               uint32_t source_flag)
{
  per_qp_meta_t *qp_meta = &ctx->qp_meta[COM_QP_ID];
  fifo_t *send_fifo = qp_meta->send_fifo;
  cp_ctx_t *cp_ctx = (cp_ctx_t *) ctx->appl_ctx;
  cp_com_t *com = (cp_com_t *) com_ptr;
  uint16_t sess_id =
      fill_commit_message_from_l_entry(com, source, source_flag, ctx->t_id);

  slot_meta_t *slot_meta = get_fifo_slot_meta_push(send_fifo);
  if (com->opcode == COMMIT_OP_NO_VAL)
    slot_meta->byte_size -= COM_SIZE - COMMIT_NO_VAL_SIZE;
  cp_com_mes_t *com_mes = (cp_com_mes_t *) get_fifo_push_slot(send_fifo);
  com_mes->coalesce_num = (uint8_t) slot_meta->coalesce_num;

  if (slot_meta->coalesce_num == 1) {
    com_mes->l_id = cp_ctx->l_ids.inserted_com_id;
    fifo_set_push_backward_ptr(send_fifo, cp_ctx->com_rob->push_ptr); // for debug
  }

  fill_com_rob_entry(cp_ctx, sess_id);
  cp_ctx->l_ids.inserted_com_id++;
}


