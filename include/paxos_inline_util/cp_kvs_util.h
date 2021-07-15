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
#include "cp_core_interface.h"




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
        rmw_tries_to_get_kv_ptr_first_time(&op[op_i], kv_ptr[op_i],
                                           cp_ctx->cp_core_ctx, op_i, t_id);
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
    on_receiving_remote_commit(kv_ptr[op_i], com, ptrs_to_com->ptr_to_mes[op_i], op_i, ctx->t_id);
  }
}


#endif //CP_KVS_UTILITY_H
