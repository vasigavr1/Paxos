//
// Created by vasilis on 16/07/2021.
//

#include <cp_netw_interface.h>
#include <od_netw_func.h>
#include <cp_netw_structs.h>


inline void cp_rmw_rep_insert(void *ctx,
                              mica_op_t **kv_ptr,
                              uint32_t op_i,
                              bool is_accept)
{
  context_t * od_ctx = (context_t *) ctx;
  cp_ctx_t *cp_ctx = (cp_ctx_t *) od_ctx->appl_ctx;
  cp_ptrs_to_ops_t *ptrs_to_op = cp_ctx->ptrs_to_ops;
  rmw_rep_flag_t flag = {
      .is_accept = is_accept,
      .op_i = (uint16_t) op_i
  };
  uint32_t source_flag = *(uint32_t*) &flag;
  od_insert_mes(od_ctx, RMW_REP_QP_ID, RMW_REP_SMALL_SIZE, 0,
                ptrs_to_op->break_message[op_i],
                (void *) kv_ptr[op_i], source_flag, 0);
}


inline void cp_prop_insert(void *ctx,
                           void *loc_entry)
{
  od_insert_mes((context_t *) ctx, PROP_QP_ID,
                (uint32_t) PROP_SIZE,
                PROP_REP_SIZE,
                false, loc_entry,
                0, 0);
}

inline void cp_acc_insert(void *ctx,
                          void *loc_entry,
                          bool helping)
{
  od_insert_mes((context_t*) ctx, ACC_QP_ID,
                (uint32_t) ACC_SIZE,
                ACC_REP_MES_SIZE,
                false, loc_entry,
                helping, 0);
}

inline bool cp_com_insert(void *ctx,
                          void *loc_entry,
                          uint32_t state)
{
  context_t * od_ctx = (context_t *) ctx;
  cp_ctx_t *cp_ctx = (cp_ctx_t *) od_ctx->appl_ctx;
  if (cp_ctx->com_rob->capacity < COM_ROB_SIZE) {
    od_insert_mes(od_ctx, COM_QP_ID,
                  (uint32_t) COM_SIZE,
                  1,
                  false, loc_entry,
                  state, 0);
    return true;
  }
  else return false;
}