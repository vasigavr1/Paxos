//
// Created by vasilis on 07/07/2021.
//

#include <cp_core_util.h>


// Potentially useful (for performance only) when a propose receives already_committed
// responses and still is holding the kv_ptr
static inline void free_kv_ptr_if_rmw_failed(loc_entry_t *loc_entry,
                                             uint8_t state, uint16_t t_id)
{
  mica_op_t *kv_ptr = loc_entry->kv_ptr;
  bool kv_ptr_is_still_proposed_for_same_rmw =
      kv_ptr->state == state &&
      kv_ptr->log_no == loc_entry->log_no &&
      rmw_ids_are_equal(&kv_ptr->rmw_id, &loc_entry->rmw_id) &&
      compare_ts(&kv_ptr->prop_ts, &loc_entry->new_ts) == EQUAL;
  if (kv_ptr_is_still_proposed_for_same_rmw) {
    assert(false);

    print_free_kv_ptr_if_rmw_failed(loc_entry, state, t_id);

    lock_kv_ptr(loc_entry->kv_ptr, t_id);
    if (kv_ptr->state == state &&
        kv_ptr->log_no == loc_entry->log_no &&
        rmw_ids_are_equal(&kv_ptr->rmw_id, &loc_entry->rmw_id)) {
      if (state == PROPOSED && compare_ts(&kv_ptr->prop_ts, &loc_entry->new_ts) == EQUAL) {
        printf("Free_kv_ptr_if_prop_already_committed\n");
        print_rmw_rep_info(loc_entry, t_id);
        //assert(false);
        kv_ptr->state = INVALID_RMW;
      }
      else if (state == ACCEPTED && compare_ts(&kv_ptr->accepted_ts, &loc_entry->new_ts) == EQUAL)
        if (ENABLE_ASSERTIONS) assert(false);
    }
    check_log_nos_of_kv_ptr(kv_ptr, "free_kv_ptr_if_prop_failed", t_id);
    unlock_kv_ptr(loc_entry->kv_ptr, t_id);
  }
}


static inline void clean_up_after_inspecting_props(loc_entry_t *loc_entry,
                                                   bool zero_out_log_too_high_cntr,
                                                   uint16_t t_id)
{
  checks_before_resetting_prop(loc_entry);
  if (loc_entry->helping_flag == PROPOSE_NOT_LOCALLY_ACKED ||
      loc_entry->helping_flag == PROPOSE_LOCALLY_ACCEPTED)
    loc_entry->helping_flag = NOT_HELPING;

  if (loc_entry->rmw_reps.ready_to_inspect)
    zero_out_the_rmw_reply_loc_entry_metadata(loc_entry);
  else if (ENABLE_ASSERTIONS) assert(loc_entry->rmw_reps.tot_replies == 1);

  if (zero_out_log_too_high_cntr) loc_entry->log_too_high_cntr = 0;

  set_kilalble_flag(loc_entry);
  check_after_inspecting_prop(loc_entry);
}

static inline void prop_handle_ack_quorum(cp_core_ctx_t *cp_core_ctx,
                                          loc_entry_t *loc_entry)
{
  // Quorum of prop acks gathered: send an accept
  act_on_quorum_of_prop_acks(cp_core_ctx, loc_entry, cp_core_ctx->t_id);
  check_state_with_allowed_flags(4, (int) loc_entry->state,
                                 ACCEPTED, NEEDS_KV_PTR, MUST_BCAST_COMMITS);
}

static inline void prop_handle_log_too_small(loc_entry_t *loc_entry,
                                             uint16_t t_id)
{
  //It is impossible for this RMW to still hold the kv_ptr
  loc_entry->state = NEEDS_KV_PTR;
}

static inline void prop_handle_seen_higher_prop(loc_entry_t *loc_entry,
                                                uint16_t t_id)
{
  rmw_rep_info_t *rep_info = &loc_entry->rmw_reps;
  // retry by incrementing the highest base_ts seen
  loc_entry->state = RETRY_WITH_BIGGER_TS;
  loc_entry->new_ts.version = rep_info->seen_higher_prop_version;
}

static inline void prop_handle_log_too_high(loc_entry_t *loc_entry,
                                            bool *zero_out_log_too_high_cntr,
                                            uint16_t t_id)
{
  react_on_log_too_high_for_prop(loc_entry, t_id);
  loc_entry->new_ts.version = loc_entry->new_ts.version;
  *zero_out_log_too_high_cntr = false;
}

static inline bool prop_has_received_ack_quorum(loc_entry_t *loc_entry)
{
  uint8_t remote_quorum = QUORUM_NUM;
  rmw_rep_info_t *rep_info = &loc_entry->rmw_reps;
  return rep_info->acks >= remote_quorum &&
         loc_entry->helping_flag != PROPOSE_NOT_LOCALLY_ACKED;
}

static inline void prop_handle_rmw_commited(cp_core_ctx_t *cp_core_ctx,
                                            loc_entry_t *loc_entry)
{
  bool may_have_gotten_kv_in_propped_state = loc_entry->accepted_log_no != loc_entry->log_no;
  if (may_have_gotten_kv_in_propped_state)
    free_kv_ptr_if_rmw_failed(loc_entry, PROPOSED, cp_core_ctx->t_id);


  handle_already_committed_rmw(cp_core_ctx, loc_entry);
  check_state_with_allowed_flags(3, (int) loc_entry->state, INVALID_RMW,
                                 MUST_BCAST_COMMITS);
}

static inline void prop_handle_already_accepted(cp_core_ctx_t *cp_core_ctx,
                                                loc_entry_t *loc_entry)
{
  if (loc_entry->helping_flag == PROPOSE_LOCALLY_ACCEPTED)
    act_on_quorum_of_prop_acks(cp_core_ctx, loc_entry, cp_core_ctx->t_id);
  else act_on_receiving_already_accepted_rep_to_prop(cp_core_ctx, loc_entry, cp_core_ctx->t_id);
  check_state_with_allowed_flags(4, (int) loc_entry->state, ACCEPTED,
                                 NEEDS_KV_PTR, MUST_BCAST_COMMITS);
}

static inline bool handle_quorum_of_acc_reps(cp_core_ctx_t *cp_core_ctx,
                                             loc_entry_t *loc_entry,
                                             bool *zero_out_log_too_high_cntr)
{

  rmw_rep_info_t *rep_info = &loc_entry->rmw_reps;

  if (rep_info->rmw_id_commited > 0)
    prop_handle_rmw_commited(cp_core_ctx, loc_entry);
  else if (rep_info->log_too_small > 0)
    prop_handle_log_too_small(loc_entry, cp_core_ctx->t_id);
  else if (rep_info->seen_higher_prop_acc > 0)
    prop_handle_seen_higher_prop(loc_entry, cp_core_ctx->t_id);
  else if (prop_has_received_ack_quorum(loc_entry))
    prop_handle_ack_quorum(cp_core_ctx, loc_entry);
  else if (rep_info->already_accepted > 0)
    prop_handle_already_accepted(cp_core_ctx, loc_entry);
  else if (rep_info->log_too_high > 0)
    prop_handle_log_too_high(loc_entry, zero_out_log_too_high_cntr, cp_core_ctx->t_id);
  else my_assert(false, "Inspecting proposes: Could not find any reps to inspect");
}


// Inspect each propose that has gathered a quorum of replies
static inline void inspect_proposes(cp_core_ctx_t *cp_core_ctx,
                                    loc_entry_t *loc_entry,
                                    uint16_t t_id)
{
  loc_entry->stalled_reason = NO_REASON;
  loc_entry->rmw_reps.inspected = true;
  advance_loc_entry_l_id(loc_entry, t_id);

  bool zero_out_log_too_high_cntr = true;
  handle_quorum_of_acc_reps(cp_core_ctx, loc_entry, &zero_out_log_too_high_cntr);
  clean_up_after_inspecting_props(loc_entry, zero_out_log_too_high_cntr, t_id);
}





inline void inspect_props_if_ready_to_inspect(cp_core_ctx_t *cp_core_ctx,
                                              loc_entry_t *loc_entry)
{
  if (loc_entry->rmw_reps.ready_to_inspect)
    inspect_proposes(cp_core_ctx, loc_entry, cp_core_ctx->t_id);
}