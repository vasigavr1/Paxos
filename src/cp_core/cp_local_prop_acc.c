//
// Created by vasilis on 15/07/2021.
//

#include <cp_core_util.h>

static inline void take_kv_ptr_to_acc_state(mica_op_t *kv_ptr,
                                            loc_entry_t *loc_entry,
                                            uint16_t t_id)
{
  checks_before_local_accept(kv_ptr, loc_entry, t_id);

  kv_ptr->state = ACCEPTED;
  // calculate the new value depending on the type of RMW
  perform_the_rmw_on_the_loc_entry(kv_ptr, loc_entry, t_id);
  //when last_accepted_value is update also update the acc_base_ts
  kv_ptr->base_acc_ts = kv_ptr->ts;
  kv_ptr->accepted_ts = loc_entry->new_ts;
  kv_ptr->accepted_log_no = kv_ptr->log_no;
  checks_after_local_accept(kv_ptr, loc_entry, t_id);
}

// After gathering a quorum of proposal acks, check if you can accept locally-- THIS IS STRICTLY LOCAL RMWS -- no helps
// Every RMW that gets committed must pass through this function successfully (at least one time)
inline void attempt_local_accept(loc_entry_t *loc_entry,
                                 uint16_t t_id)
{
  mica_op_t *kv_ptr = loc_entry->kv_ptr;
  checks_preliminary_local_accept(kv_ptr, loc_entry, t_id);

  lock_kv_ptr(loc_entry->kv_ptr, t_id);

  if (if_already_committed_bcast_commits(loc_entry, t_id)) {
    unlock_kv_ptr(loc_entry->kv_ptr, t_id);
    return;
  }

  if (same_rmw_id_same_ts_and_invalid(kv_ptr, loc_entry)) {
    take_kv_ptr_to_acc_state(kv_ptr, loc_entry, t_id);
    unlock_kv_ptr(loc_entry->kv_ptr, t_id);
    loc_entry->state = ACCEPTED;
  }
  else { // the entry stores a different rmw_id and thus our proposal has been won by another
    checks_after_failure_to_locally_accept(kv_ptr, loc_entry, t_id);
    unlock_kv_ptr(loc_entry->kv_ptr, t_id);
    loc_entry->state = NEEDS_KV_PTR;
  }
}

static inline void take_local_kv_ptr_to_acc_state_when_helping(mica_op_t *kv_ptr,
                                                               loc_entry_t *loc_entry,
                                                               loc_entry_t* help_loc_entry,
                                                               uint16_t t_id)
{
  kv_ptr->state = ACCEPTED;
  kv_ptr->rmw_id = help_loc_entry->rmw_id;
  kv_ptr->accepted_ts = help_loc_entry->new_ts;
  kv_ptr->accepted_log_no = kv_ptr->log_no;
  write_kv_ptr_acc_val(kv_ptr, help_loc_entry->value_to_write, (size_t) RMW_VALUE_SIZE);
  kv_ptr->base_acc_ts = help_loc_entry->base_ts;
  checks_after_local_accept_help(kv_ptr, loc_entry, t_id);
  unlock_kv_ptr(loc_entry->kv_ptr, t_id);
  loc_entry->state = ACCEPTED;
}

static inline void clean_up_if_cannot_accept_locally(mica_op_t *kv_ptr,
                                                     loc_entry_t *loc_entry,
                                                     uint16_t t_id)
{
  checks_after_failure_to_locally_accept_help(kv_ptr, loc_entry, t_id);
  unlock_kv_ptr(loc_entry->kv_ptr, t_id);
  loc_entry->state = NEEDS_KV_PTR;
  loc_entry->help_loc_entry->state = INVALID_RMW;
}


// After gathering a quorum of proposal reps, one of them was a lower TS accept, try and help it
inline void attempt_local_accept_to_help(loc_entry_t *loc_entry,
                                         uint16_t t_id)
{
  mica_op_t *kv_ptr = loc_entry->kv_ptr;
  loc_entry_t* help_loc_entry = loc_entry->help_loc_entry;

  help_loc_entry->new_ts = loc_entry->new_ts;
  checks_preliminary_local_accept_help(kv_ptr, loc_entry, help_loc_entry);

  lock_kv_ptr(loc_entry->kv_ptr, t_id);
  comment_on_why_we_dont_check_if_rmw_committed();

  bool can_accept_locally = find_out_if_can_accept_help_locally(kv_ptr, loc_entry,
                                                                help_loc_entry, t_id);
  if (can_accept_locally)
    take_local_kv_ptr_to_acc_state_when_helping(kv_ptr, loc_entry,
                                                help_loc_entry, t_id);
  else clean_up_if_cannot_accept_locally(kv_ptr, loc_entry, t_id);

}


static inline void rmw_fails_or_grabs_if_invalid_or_must_wait(trace_op_t *op,
                                                              mica_op_t *kv_ptr,
                                                              loc_entry_t *loc_entry,
                                                              uint32_t new_version,
                                                              uint8_t success_state,
                                                              uint16_t t_id)
{
  check_trace_op_key_vs_kv_ptr(op, kv_ptr);
  if (does_rmw_fail_early(op, kv_ptr, t_id)) {
    loc_entry->state = CAS_FAILED;
  }
  else if (kv_ptr->state == INVALID_RMW) {
    activate_kv_pair(success_state, new_version, kv_ptr, op->opcode,
                     (uint8_t) machine_id, loc_entry, loc_entry->rmw_id.id,
                     kv_ptr->last_committed_log_no + 1, t_id,
                     ENABLE_ASSERTIONS ? "batch to trace" : NULL);
    loc_entry->state = success_state;
    loc_entry->log_no = kv_ptr->log_no;
  }
  else {
    loc_entry->state = NEEDS_KV_PTR;
    save_the_info_of_the_kv_ptr_owner(kv_ptr, loc_entry);
  }

  loc_entry->base_ts = kv_ptr->ts;
}


static inline void set_up_for_trying_rmw_trying_first_time(trace_op_t *op,
                                                           loc_entry_t *loc_entry,
                                                           uint32_t *new_version,
                                                           uint8_t *success_state,
                                                           uint16_t t_id)
{
  init_loc_entry(op, t_id, loc_entry);


  *new_version = (ENABLE_ALL_ABOARD && op->attempt_all_aboard) ?
                 ALL_ABOARD_TS : PAXOS_TS;
  *success_state = (uint8_t) (loc_entry->all_aboard ? ACCEPTED : PROPOSED);
  __builtin_prefetch(loc_entry->compare_val, 0, 0);
}

static inline void clean_up_for_trying_rmw_trying_first_time(trace_op_t *op,
                                                             mica_op_t *kv_ptr,
                                                             loc_entry_t *loc_entry,
                                                             uint32_t new_version)
{
  loc_entry->kv_ptr = kv_ptr;
  debug_assign_help_loc_entry_kv_ptr(kv_ptr, loc_entry);
  op->ts.version = new_version;
}

inline void rmw_tries_to_get_kv_ptr_first_time(trace_op_t *op,
                                               mica_op_t *kv_ptr,
                                               cp_core_ctx_t *cp_core_ctx,
                                               uint16_t op_i,
                                               uint16_t t_id)
{
  print_rmw_tries_first_time(op_i, t_id);
  loc_entry_t *loc_entry = &cp_core_ctx->rmw_entries[op->session_id];

  uint32_t new_version; uint8_t success_state;
  set_up_for_trying_rmw_trying_first_time(op, loc_entry, &new_version, &success_state, t_id);


  lock_kv_ptr(kv_ptr, t_id);
  rmw_fails_or_grabs_if_invalid_or_must_wait(op, kv_ptr, loc_entry, new_version,
                                             success_state, t_id);
  unlock_kv_ptr(kv_ptr, t_id);

  clean_up_for_trying_rmw_trying_first_time(op, kv_ptr, loc_entry, new_version);
}
