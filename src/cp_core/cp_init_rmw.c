//
// Created by vasilis on 15/07/2021.
//

#include <cp_core_interface.h>
#include <cp_core_generic_util.h>




static inline void loc_entry_was_successful_first_time(loc_entry_t *loc_entry,
                                                       cp_core_ctx_t *cp_core_ctx,
                                                       trace_op_t *op,
                                                       uint16_t t_id)
{
  fill_loc_rmw_entry_on_grabbing_kv_ptr(loc_entry, op->ts.version,
                                        loc_entry->state, op->session_id, t_id);

  bool doing_all_aboard = loc_entry->state == ACCEPTED;
  check_op_version(op, doing_all_aboard);

  if (doing_all_aboard) {
    loc_entry->accepted_log_no = loc_entry->log_no;
    cp_acc_insert(cp_core_ctx->netw_ctx, loc_entry, false);
    loc_entry->killable = false;
    loc_entry->all_aboard = true;
    loc_entry->all_aboard_time_out = 0;
  }
  else
    cp_prop_insert(cp_core_ctx->netw_ctx, loc_entry);
}

static inline void handle_loc_entry_cas_failed_first_time(loc_entry_t *loc_entry,
                                                          cp_core_ctx_t *cp_core_ctx,
                                                          trace_op_t *op,
                                                          uint16_t t_id)
{
  if (ENABLE_CAS_CANCELLING) {
    if (loc_entry->state == CAS_FAILED) {
      signal_completion_to_client(op->session_id, op->index_to_req_array, t_id);
      cp_core_ctx->stall_info->stalled[op->session_id] = false;
      cp_core_ctx->stall_info->all_stalled = false;
      loc_entry->state = INVALID_RMW;
    }
  }
  else my_assert(false, "Wrong loc_entry in RMW");
}

// Insert an RMW in the local RMW structs
inline void insert_rmw(cp_core_ctx_t *cp_core_ctx,
                       trace_op_t *op,
                       uint16_t t_id)
{
  uint16_t session_id = op->session_id;
  check_session_id(session_id);
  loc_entry_t *loc_entry = &cp_core_ctx->rmw_entries[session_id];
  uint8_t success_state = (uint8_t) (ENABLE_ALL_ABOARD && op->attempt_all_aboard ? ACCEPTED : PROPOSED);
  bool kv_ptr_taken = loc_entry->state == success_state;

  if (kv_ptr_taken)  loc_entry_was_successful_first_time(loc_entry, cp_core_ctx, op, t_id);
  else if (loc_entry->state == NEEDS_KV_PTR) {
    if (ENABLE_ALL_ABOARD) loc_entry->all_aboard = false;
  }
  else handle_loc_entry_cas_failed_first_time(loc_entry, cp_core_ctx, op, t_id);
}