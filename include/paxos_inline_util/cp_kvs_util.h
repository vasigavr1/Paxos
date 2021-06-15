//
// Created by vasilis on 11/05/20.
//

#ifndef CP_KVS_UTILITY_H
#define CP_KVS_UTILITY_H

#include <cp_config.h>
#include "od_kvs.h"
#include "cp_generic_util.h"
#include "cp_debug_util.h"
#include "od_wrkr_side_calls.h"
#include "cp_paxos_util.h"





static inline void KVS_from_trace_rmw(trace_op_t *op,
                                      mica_op_t *kv_ptr,
                                      cp_ctx_t *cp_ctx,
                                      uint16_t op_i, uint16_t t_id)
{
  loc_entry_t *loc_entry = &cp_ctx->prop_info->entry[op->session_id];
  init_loc_entry(cp_ctx, op, t_id, loc_entry);
  if (DEBUG_RMW) my_printf(green, "Worker %u trying a local RMW on op %u\n", t_id, op_i);
  uint32_t new_version = (ENABLE_ALL_ABOARD && op->attempt_all_aboard) ?
                         ALL_ABOARD_TS : PAXOS_TS;
  uint8_t state = (uint8_t) (loc_entry->all_aboard ? ACCEPTED : PROPOSED);
  __builtin_prefetch(loc_entry->compare_val, 0, 0);
  lock_seqlock(&kv_ptr->seqlock);
  {
    check_trace_op_key_vs_kv_ptr(op, kv_ptr);
    check_log_nos_of_kv_ptr(kv_ptr, "KVS_batch_op_trace", t_id);
    if (does_rmw_fail_early(op, kv_ptr, t_id)) {
      loc_entry->state = CAS_FAILED;
    }
    else if (kv_ptr->state == INVALID_RMW) {
      activate_kv_pair(state, new_version, kv_ptr, op->opcode,
                       (uint8_t) machine_id, loc_entry, loc_entry->rmw_id.id,
                       kv_ptr->last_committed_log_no + 1, t_id, ENABLE_ASSERTIONS ? "batch to trace" : NULL);
      loc_entry->state = state;
      if (ENABLE_ASSERTIONS) assert(kv_ptr->log_no == kv_ptr->last_committed_log_no + 1);
      loc_entry->log_no = kv_ptr->log_no;
    }
    else {
      // This is the state the RMW will wait on
      loc_entry->state = NEEDS_KV_PTR;
      // Set up the state that the RMW should wait on
      loc_entry->help_rmw->rmw_id = kv_ptr->rmw_id;
      loc_entry->help_rmw->state = kv_ptr->state;
      loc_entry->help_rmw->ts = kv_ptr->prop_ts;
      loc_entry->help_rmw->log_no = kv_ptr->log_no;
    }
  }
  loc_entry->base_ts = kv_ptr->ts;
  unlock_seqlock(&kv_ptr->seqlock);

  loc_entry->kv_ptr = kv_ptr;
  if (ENABLE_ASSERTIONS) {
    loc_entry->help_loc_entry->kv_ptr = kv_ptr;
  }
  // We need to put the new timestamp in the op too, both to send it and to store it for later
  op->ts.version = new_version;
}


static inline void cp_KVS_batch_op_trace(uint16_t op_num,
                                         trace_op_t *op,
                                         cp_ctx_t *cp_ctx,
                                         uint16_t t_id)
{
  uint16_t op_i;
  if (ENABLE_ASSERTIONS) assert (op_num <= MAX_OP_BATCH);
  unsigned int bkt[MAX_OP_BATCH];
  struct mica_bkt *bkt_ptr[MAX_OP_BATCH];
  unsigned int tag[MAX_OP_BATCH];
  mica_op_t *kv_ptr[MAX_OP_BATCH];	/* Ptr to KV item in log */
  for(op_i = 0; op_i < op_num; op_i++) {
    KVS_locate_one_bucket(op_i, bkt, &op[op_i].key, bkt_ptr, tag, kv_ptr, KVS);
  }
  KVS_locate_all_kv_pairs(op_num, tag, bkt_ptr, kv_ptr, KVS);
  for(op_i = 0; op_i < op_num; op_i++) {
    od_KVS_check_key(kv_ptr[op_i], op[op_i].key, op_i);
    switch (op[op_i].opcode) {
      case FETCH_AND_ADD:
      case COMPARE_AND_SWAP_WEAK:
      case COMPARE_AND_SWAP_STRONG:
      case RMW_PLAIN_WRITE:
        KVS_from_trace_rmw(&op[op_i], kv_ptr[op_i],
                           cp_ctx, op_i, t_id);
        break;
      default: if (ENABLE_ASSERTIONS) {
          my_printf(red, "Wrkr %u: KVS_batch_op_trace wrong opcode in KVS: %d, req %d \n",
                    t_id, op[op_i].opcode, op_i);
          assert(false);
        }
    }
  }
}


static inline void cp_KVS_batch_op_rmws(context_t *ctx, bool is_accept)
{
  cp_ctx_t *cp_ctx = (cp_ctx_t *) ctx->appl_ctx;
  void *ops = (void *) cp_ctx->ptrs_to_ops->ptr_to_ops;
  uint16_t op_num = cp_ctx->ptrs_to_ops->polled_ops;
  uint16_t op_i;

  if (ENABLE_ASSERTIONS) {
    assert(ops != NULL);
    assert(op_num <= MAX_INCOMING_RMW);
  }
  unsigned int bkt[MAX_INCOMING_RMW];
  struct mica_bkt *bkt_ptr[MAX_INCOMING_RMW];
  unsigned int tag[MAX_INCOMING_RMW];
  mica_op_t *kv_ptr[MAX_INCOMING_RMW];	/* Ptr to KV item in log */
  /*
     * We first lookup the key in the datastore. The first two @I loops work
     * for both GETs and PUTs.
     */
  for(op_i = 0; op_i < op_num; op_i++) {
    KVS_locate_one_bucket(op_i, bkt, key_ptr_of_rmw_op(ops, op_i, is_accept),
                          bkt_ptr, tag, kv_ptr, KVS);
  }
  KVS_locate_all_kv_pairs(op_num, tag, bkt_ptr, kv_ptr, KVS);

  for(op_i = 0; op_i < op_num; op_i++) {
    od_KVS_check_key(kv_ptr[op_i], key_of_rmw_op(ops, op_i, is_accept), op_i);
    check_received_rmw_in_KVS(ops, op_i, is_accept);
    cp_rmw_rep_insert(ctx, kv_ptr, op_i, is_accept);
  }
}


static inline void cp_KVS_batch_op_props(context_t *ctx)
{
  cp_KVS_batch_op_rmws(ctx, false);
}


static inline void cp_KVS_batch_op_accs(context_t *ctx)
{
  cp_KVS_batch_op_rmws(ctx, true);
}







static inline void cp_KVS_batch_op_coms(context_t *ctx)
{
  cp_ctx_t *cp_ctx = (cp_ctx_t *) ctx->appl_ctx;
  cp_ptrs_to_ops_t *ptrs_to_com = cp_ctx->ptrs_to_ops;
  cp_com_t **coms = (cp_com_t **) ptrs_to_com->ptr_to_ops;
  uint16_t op_num = ptrs_to_com->polled_ops;
  uint16_t op_i;

  if (ENABLE_ASSERTIONS) {
    assert(coms != NULL);
    assert(op_num <= MAX_INCOMING_COM);
  }
  unsigned int bkt[MAX_INCOMING_COM];
  struct mica_bkt *bkt_ptr[MAX_INCOMING_COM];
  unsigned int tag[MAX_INCOMING_COM];
  mica_op_t *kv_ptr[MAX_INCOMING_COM];	/* Ptr to KV item in log */
  /*
     * We first lookup the key in the datastore. The first two @I loops work
     * for both GETs and PUTs.
     */
  for(op_i = 0; op_i < op_num; op_i++) {
    KVS_locate_one_bucket(op_i, bkt, &coms[op_i]->key , bkt_ptr, tag, kv_ptr, KVS);
  }
  KVS_locate_all_kv_pairs(op_num, tag, bkt_ptr, kv_ptr, KVS);

  for(op_i = 0; op_i < op_num; op_i++) {
    od_KVS_check_key(kv_ptr[op_i], coms[op_i]->key, op_i);
    cp_com_t *com = coms[op_i];
    if (ENABLE_ASSERTIONS) assert(com->opcode == COMMIT_OP || com->opcode == COMMIT_OP_NO_VAL);
    print_on_remote_com(com, op_i, ctx->t_id);
    commit_rmw(kv_ptr[op_i], (void*) com, NULL, FROM_REMOTE_COMMIT, ctx->t_id);
    print_log_remote_com(com, ptrs_to_com->ptr_to_mes[op_i], kv_ptr[op_i], ctx->t_id);
  }
}


#endif //CP_KVS_UTILITY_H
