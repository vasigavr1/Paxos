//
// Created by vasilis on 15/07/2021.
//

#include <cp_core_generic_util.h>


typedef struct retry_flags{
  bool from_propose;
  bool kv_ptr_was_grabbed ;
  bool rmw_fails;
  bool help_locally_acced;
  bool is_still_accepted;
} retry_flags_t;


static inline void clean_up_after_retrying(sess_stall_t *stall_info,
                                           mica_op_t *kv_ptr,
                                           loc_entry_t *loc_entry,
                                           retry_flags_t flags,
                                           uint16_t t_id)
{
  if (flags.kv_ptr_was_grabbed) {
    print_clean_up_after_retrying(kv_ptr, loc_entry, t_id);
    loc_entry->state = PROPOSED;
    if (flags.help_locally_acced)
      set_up_a_proposed_but_not_locally_acked_entry(stall_info, kv_ptr,
                                                    loc_entry, true, t_id);
    else local_rmw_ack(loc_entry);
  }
  else if (flags.rmw_fails) {
    check_clean_up_after_retrying(kv_ptr, loc_entry,
                                  flags.help_locally_acced, t_id);
    loc_entry->state = INVALID_RMW;
    free_session_from_rmw(loc_entry, stall_info,
                          false, t_id);
  }
  else loc_entry->state = NEEDS_KV_PTR;
}



static inline bool can_kv_ptr_be_taken_with_higher_TS(mica_op_t *kv_ptr,
                                                      loc_entry_t *loc_entry,
                                                      retry_flags_t *flags)
{
  bool is_still_proposed = rmw_ids_are_equal(&kv_ptr->rmw_id, &loc_entry->rmw_id) &&
                           kv_ptr->state == PROPOSED;

  flags->is_still_accepted = rmw_ids_are_equal(&kv_ptr->rmw_id, &loc_entry->rmw_id) &&
                             kv_ptr->state == ACCEPTED &&
                             compare_ts(&kv_ptr->accepted_ts, &loc_entry->new_ts) == EQUAL;

  return kv_ptr->state == INVALID_RMW || is_still_proposed || flags->is_still_accepted;
}


static inline void update_loc_entry_when_taking_kv_ptr_with_higher_TS(mica_op_t *kv_ptr,
                                                                      loc_entry_t *loc_entry)
{
  loc_entry->log_no = kv_ptr->last_committed_log_no + 1;
  loc_entry->new_ts.version = MAX(loc_entry->new_ts.version, kv_ptr->prop_ts.version) + 1;
  loc_entry->base_ts = kv_ptr->ts; // Minimize the possibility for RMW_ACK_BASE_TS_STALE
  loc_entry->new_ts.m_id = (uint8_t) machine_id;
}

static inline void update_kv_ptr_when_taking_kv_ptr_with_higher_TS(mica_op_t *kv_ptr,
                                                                   loc_entry_t *loc_entry)
{
  if (kv_ptr->state == INVALID_RMW) {
    kv_ptr->log_no = kv_ptr->last_committed_log_no + 1;
    kv_ptr->opcode = loc_entry->opcode;
    assign_second_rmw_id_to_first(&kv_ptr->rmw_id, &loc_entry->rmw_id);
  }
  kv_ptr->prop_ts = loc_entry->new_ts;
}

static inline void if_accepted_help_else_steal(mica_op_t *kv_ptr,
                                               loc_entry_t *loc_entry,
                                               retry_flags_t *flags)
{
  if (!flags->is_still_accepted) {
    check_kv_ptr_state_is_not_acced(kv_ptr);
    kv_ptr->state = PROPOSED;
  }
  else {
    flags->help_locally_acced = true;
    loc_entry->help_loc_entry->new_ts = kv_ptr->accepted_ts;
  }
}

static inline void rmw_fails_or_steals_kv_ptr_or_helps_kv_ptr(mica_op_t *kv_ptr,
                                                              loc_entry_t *loc_entry,
                                                              retry_flags_t *flags,
                                                              uint16_t t_id)
{
  check_when_retrying_with_higher_TS(kv_ptr, loc_entry, flags->from_propose);
  if (rmw_fails_with_loc_entry(loc_entry, kv_ptr, &flags->rmw_fails, t_id)) {
    check_kv_ptr_state_is_not_acced(kv_ptr);
    kv_ptr->state = INVALID_RMW;
  }
  else {
    update_loc_entry_when_taking_kv_ptr_with_higher_TS(kv_ptr, loc_entry);
    update_kv_ptr_when_taking_kv_ptr_with_higher_TS(kv_ptr, loc_entry);
    if_accepted_help_else_steal(kv_ptr, loc_entry, flags);
    flags->kv_ptr_was_grabbed = true;
  }
}

// local_entry->state == RETRY_WITH_BIGGER_TS
inline void take_kv_ptr_with_higher_TS(sess_stall_t *stall_info,
                                       loc_entry_t *loc_entry,
                                       bool from_propose,
                                       uint16_t t_id)
{
  retry_flags_t flags;
  memset(&flags, 0, sizeof(retry_flags_t));
  flags.from_propose = from_propose;

  mica_op_t *kv_ptr = loc_entry->kv_ptr;
  lock_kv_ptr(kv_ptr, t_id);
  {
    if (if_already_committed_bcast_commits(loc_entry, t_id)) {
      unlock_kv_ptr(loc_entry->kv_ptr, t_id);
      return;
    }
    if (can_kv_ptr_be_taken_with_higher_TS(kv_ptr, loc_entry, &flags)) {
      rmw_fails_or_steals_kv_ptr_or_helps_kv_ptr(kv_ptr, loc_entry, &flags, t_id);
    }
    else print_when_retrying_fails(kv_ptr, loc_entry, t_id);
  }
  unlock_kv_ptr(loc_entry->kv_ptr, t_id);
  clean_up_after_retrying(stall_info, kv_ptr, loc_entry, flags, t_id);
}