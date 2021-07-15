//
// Created by vasilis on 14/07/2021.
//


#include <cp_core_generic_util.h>




// Check if the kv_ptr state that is blocking a local RMW is persisting
static inline bool kv_ptr_state_has_not_changed(mica_op_t *kv_ptr,
                                                struct rmw_help_entry *help_rmw)
{
  return kv_ptr->state == help_rmw->state &&
         rmw_ids_are_equal(&help_rmw->rmw_id, &kv_ptr->rmw_id) &&
         (compare_ts(&kv_ptr->prop_ts, &help_rmw->ts) == EQUAL);
}

// Check if the kv_ptr state that is blocking a local RMW is persisting
static inline bool kv_ptr_state_has_changed(mica_op_t *kv_ptr,
                                            struct rmw_help_entry *help_rmw)
{
  return kv_ptr->state != help_rmw->state ||
         (!rmw_ids_are_equal(&help_rmw->rmw_id, &kv_ptr->rmw_id)) ||
         (compare_ts(&kv_ptr->prop_ts, &help_rmw->ts) != EQUAL);
}

static inline bool grab_invalid_kv_ptr_after_waiting(mica_op_t *kv_ptr,
                                                     loc_entry_t *loc_entry,
                                                     bool *rmw_fails,
                                                     uint16_t t_id)
{
  if (rmw_fails_with_loc_entry(loc_entry, kv_ptr, rmw_fails, t_id))  return false;

  loc_entry->log_no = kv_ptr->last_committed_log_no + 1;
  activate_kv_pair(PROPOSED, PAXOS_TS, kv_ptr, loc_entry->opcode,
                   (uint8_t) machine_id, NULL, loc_entry->rmw_id.id,
                   loc_entry->log_no, t_id,
                   ENABLE_ASSERTIONS ? "attempt_to_grab_kv_ptr_after_waiting" : NULL);
  print_when_grabbing_kv_ptr(loc_entry, t_id);
  return true;
}

static inline void if_invalid_grab_if_changed_keep_track_of_the_new_rmw(mica_op_t *kv_ptr,
                                                                        loc_entry_t *loc_entry,
                                                                        bool *kv_ptr_was_grabbed,
                                                                        bool *rmw_fails,
                                                                        uint16_t t_id)
{
  if (kv_ptr->state == INVALID_RMW)
    *kv_ptr_was_grabbed = grab_invalid_kv_ptr_after_waiting(kv_ptr,loc_entry, rmw_fails, t_id);
  else if (kv_ptr_state_has_changed(kv_ptr, loc_entry->help_rmw)) {
    print_when_state_changed_not_grabbing_kv_ptr(kv_ptr, loc_entry, t_id);
    save_the_info_of_the_kv_ptr_owner(kv_ptr, loc_entry);
    loc_entry->back_off_cntr = 0;
  }
}


static inline void inspect_if_kv_ptr_is_invalid_or_has_changed_owner(mica_op_t *kv_ptr,
                                                                     loc_entry_t *loc_entry,
                                                                     bool *kv_ptr_was_grabbed,
                                                                     bool *rmw_fails,
                                                                     uint16_t t_id)
{
  lock_kv_ptr(kv_ptr, t_id);
  {
    if (!if_already_committed_bcast_commits(loc_entry, t_id))
      if_invalid_grab_if_changed_keep_track_of_the_new_rmw(kv_ptr, loc_entry,
                                                           kv_ptr_was_grabbed,
                                                           rmw_fails, t_id);
  }
  unlock_kv_ptr(loc_entry->kv_ptr, t_id);
}

static inline void if_grabbed_fill_loc_if_failed_free_session(sess_stall_t *stall_info,
                                                              mica_op_t *kv_ptr,
                                                              loc_entry_t *loc_entry,
                                                              uint16_t sess_i,
                                                              bool kv_ptr_was_grabbed,
                                                              bool rmw_fails,
                                                              uint16_t t_id)
{
  if (kv_ptr_was_grabbed) {
    fill_loc_rmw_entry_on_grabbing_kv_ptr(loc_entry, PAXOS_TS,
                                          PROPOSED, sess_i, t_id);
  }
  else if (rmw_fails) {
    check_and_print_when_rmw_fails(kv_ptr, loc_entry, t_id);
    loc_entry->state = INVALID_RMW;
    free_session_from_rmw(loc_entry, stall_info, false, t_id);
  }
}

// When inspecting an RMW that failed to grab a kv_ptr in the past
static inline bool attempt_to_grab_kv_ptr_after_waiting(sess_stall_t *stall_info,
                                                        mica_op_t *kv_ptr,
                                                        loc_entry_t *loc_entry,
                                                        uint16_t sess_i,
                                                        uint16_t t_id)
{
  checks_init_attempt_to_grab_kv_ptr(loc_entry, t_id);
  bool kv_ptr_was_grabbed = false;
  bool rmw_fails = false;

  inspect_if_kv_ptr_is_invalid_or_has_changed_owner(kv_ptr,loc_entry,
                                                    &kv_ptr_was_grabbed, &rmw_fails, t_id);

  if_grabbed_fill_loc_if_failed_free_session(stall_info, kv_ptr, loc_entry, sess_i,
                                             kv_ptr_was_grabbed, rmw_fails, t_id);

  return kv_ptr_was_grabbed || rmw_fails;
}


static inline void fill_help_loc_entry_with_acced_rmw(mica_op_t *kv_ptr,
                                                      loc_entry_t *help_loc_entry)
{

  help_loc_entry->new_ts = kv_ptr->accepted_ts;
  help_loc_entry->rmw_id = kv_ptr->rmw_id;
  memcpy(help_loc_entry->value_to_write, kv_ptr->last_accepted_value,
         (size_t) RMW_VALUE_SIZE);
  help_loc_entry->base_ts = kv_ptr->base_acc_ts;
}



static inline void bookkeep_kv_ptr_and_loc_entry_due_to_lower_accept_rep(mica_op_t *kv_ptr,
                                                                         loc_entry_t *loc_entry)
{
  loc_entry->log_no = kv_ptr->accepted_log_no;
  loc_entry->new_ts.version = kv_ptr->prop_ts.version + 1;
  loc_entry->new_ts.m_id = (uint8_t) machine_id;
  kv_ptr->prop_ts = loc_entry->new_ts;
}


static inline bool will_help_locally_accepted_value(mica_op_t *kv_ptr,
                                                    loc_entry_t *loc_entry,
                                                    uint16_t t_id)
{
  bool help = false;
  loc_entry_t *help_loc_entry = loc_entry->help_loc_entry;
  lock_kv_ptr(loc_entry->kv_ptr, t_id);
  {
    if (kv_ptr_state_has_not_changed(kv_ptr, loc_entry->help_rmw)) {
      fill_help_loc_entry_with_acced_rmw(kv_ptr, help_loc_entry);
      bookkeep_kv_ptr_and_loc_entry_due_to_lower_accept_rep(kv_ptr, loc_entry);
      help = true;
      checks_attempt_to_help_locally_accepted(kv_ptr, loc_entry, t_id);
    }
  }
  unlock_kv_ptr(loc_entry->kv_ptr, t_id);
  return help;
}


static inline void attempt_to_help_a_locally_accepted_value(sess_stall_t *stall_info,
                                                            loc_entry_t *loc_entry,
                                                            mica_op_t *kv_ptr, uint16_t t_id)
{
  loc_entry->back_off_cntr = 0;
  if (will_help_locally_accepted_value(kv_ptr, loc_entry, t_id)) {
    bool helping_myself = loc_entry->help_loc_entry->rmw_id.id == loc_entry->rmw_id.id;
    set_up_a_proposed_but_not_locally_acked_entry(stall_info, kv_ptr, loc_entry, helping_myself, t_id);
  }
}


static inline void steal_the_stucked_proposed_rmw(loc_entry_t *loc_entry,
                                                  mica_op_t *kv_ptr,
                                                  uint32_t *new_version,
                                                  uint16_t t_id)
{
  check_the_proposed_log_no(kv_ptr, loc_entry, t_id);
  loc_entry->log_no = kv_ptr->last_committed_log_no + 1;
  *new_version = kv_ptr->prop_ts.version + 1;
  activate_kv_pair(PROPOSED, *new_version, kv_ptr, loc_entry->opcode,
                   (uint8_t) machine_id, NULL, loc_entry->rmw_id.id,
                   loc_entry->log_no, t_id,
                   ENABLE_ASSERTIONS ? "attempt_to_steal_a_proposed_kv_ptr" : NULL);
  loc_entry->base_ts = kv_ptr->ts;
}

static inline bool steal_if_invalid_or_state_has_not_changed(loc_entry_t *loc_entry,
                                                             mica_op_t *kv_ptr,
                                                             uint32_t *new_version,
                                                             uint16_t t_id)
{
  bool invalid_or_state_has_not_changed =
      kv_ptr->state == INVALID_RMW || kv_ptr_state_has_not_changed(kv_ptr, loc_entry->help_rmw);

  if (invalid_or_state_has_not_changed) {
    steal_the_stucked_proposed_rmw(loc_entry, kv_ptr, new_version, t_id);
    return true;
  }
  else if (kv_ptr_state_has_changed(kv_ptr, loc_entry->help_rmw)) {
    print_when_state_changed_steal_proposed(kv_ptr, loc_entry, t_id);
    save_the_info_of_the_kv_ptr_owner(kv_ptr, loc_entry);
  }
  else my_assert(false, "");
  return false;
}

static inline bool will_steal_a_proposed_kv_ptr(loc_entry_t *loc_entry,
                                                mica_op_t *kv_ptr,
                                                uint32_t *new_version,
                                                uint16_t t_id)
{
  bool kv_ptr_was_grabbed = false;
  lock_kv_ptr(loc_entry->kv_ptr, t_id);
  {
    if (!if_already_committed_bcast_commits(loc_entry, t_id)) {
      kv_ptr_was_grabbed =
          steal_if_invalid_or_state_has_not_changed(loc_entry, kv_ptr,
                                                    new_version, t_id);
    }
  }
  unlock_kv_ptr(loc_entry->kv_ptr, t_id);
  return kv_ptr_was_grabbed;
}

// After backing off waiting on a PROPOSED kv_ptr try to steal it
static inline void attempt_to_steal_a_proposed_kv_ptr(loc_entry_t *loc_entry,
                                                      mica_op_t *kv_ptr,
                                                      uint16_t sess_i, uint16_t t_id)
{
  uint32_t new_version = 0;

  loc_entry->back_off_cntr = 0;

  if (will_steal_a_proposed_kv_ptr(loc_entry, kv_ptr, &new_version, t_id)) {
    print_after_stealing_proposed(kv_ptr, loc_entry, t_id);
    fill_loc_rmw_entry_on_grabbing_kv_ptr(loc_entry, new_version,
                                          PROPOSED, sess_i, t_id);
  }
}



static inline void clean_up_if_proposed_after_needs_kv_ptr(cp_core_ctx_t *cp_core_ctx,
                                                           loc_entry_t *loc_entry)
{
  if (loc_entry->state == PROPOSED) {
    loc_entry->back_off_cntr = 0;
    cp_prop_insert(cp_core_ctx->netw_ctx, loc_entry);
  }
}



// local_entry->state = NEEDS_KV_PTR
inline void handle_needs_kv_ptr_state(cp_core_ctx_t *cp_core_ctx,
                                      loc_entry_t *loc_entry,
                                      uint16_t sess_i,
                                      uint16_t t_id)
{
  mica_op_t *kv_ptr = loc_entry->kv_ptr;

  if (!attempt_to_grab_kv_ptr_after_waiting(cp_core_ctx->stall_info, kv_ptr, loc_entry,
                                            sess_i, t_id)) {
    check_handle_needs_kv_ptr_state(cp_core_ctx, sess_i);
    loc_entry->back_off_cntr++;
    if (loc_entry->back_off_cntr == RMW_BACK_OFF_TIMEOUT) {
      print_needs_kv_ptr_timeout_expires(loc_entry, sess_i, t_id);

      if (loc_entry->help_rmw->state == ACCEPTED)
        attempt_to_help_a_locally_accepted_value(cp_core_ctx->stall_info, loc_entry, kv_ptr, t_id);
      else  if (loc_entry->help_rmw->state == PROPOSED) {
        attempt_to_steal_a_proposed_kv_ptr(loc_entry, kv_ptr, sess_i, t_id);
      }
    }
  }
  clean_up_if_proposed_after_needs_kv_ptr(cp_core_ctx, loc_entry);

  check_end_handle_needs_kv_ptr_state(loc_entry);
}
