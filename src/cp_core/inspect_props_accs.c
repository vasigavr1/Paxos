//
// Created by vasilis on 07/07/2021.
//

#include <cp_core_util.h>

static inline bool set_up_broadcast_already_committed_if_needed(loc_entry_t *loc_entry)
{
  bool will_broadcast = !loc_entry->rmw_reps.no_need_to_bcast &&
                          (loc_entry->rmw_reps.rmw_id_commited < REMOTE_QUORUM);

  if (will_broadcast) {
    loc_entry->log_no = loc_entry->accepted_log_no;
    loc_entry->state = MUST_BCAST_COMMITS;
    check_bcasting_after_rmw_already_committed();
  }

  return will_broadcast;
}

static inline void prop_acc_handle_already_committed(cp_core_ctx_t *cp_core_ctx,
                                                     loc_entry_t *loc_entry)
{
  check_if_accepted_cannot_be_helping(loc_entry);

 bool will_broadcast = set_up_broadcast_already_committed_if_needed(loc_entry);

  if (!will_broadcast) {
    loc_entry->state = INVALID_RMW;
    free_session_from_rmw(loc_entry, cp_core_ctx->stall_info,
                          true, cp_core_ctx->t_id);
  }
  check_state_with_allowed_flags(3, (int) loc_entry->state,
                                 INVALID_RMW,
                                 MUST_BCAST_COMMITS);
}


static inline void prop_acc_handle_log_too_small(loc_entry_t *loc_entry)
{
  check_if_accepted_cannot_be_helping(loc_entry);
  //It is impossible for this RMW to still hold the kv_ptr
  loc_entry->state = NEEDS_KV_PTR;

}


static inline void prop_acc_handle_seen_higher_prop(loc_entry_t *loc_entry)
{
  check_if_accepted_cannot_be_helping(loc_entry);
  loc_entry->state = RETRY_WITH_BIGGER_TS;
  loc_entry->new_ts.version = loc_entry->rmw_reps.seen_higher_prop_version;

}


/*
 * ----ACCEPTS----
 **/

static inline void clean_up_after_inspecting_accept(loc_entry_t *loc_entry,
                                                    uint16_t t_id)
{
  checks_before_resetting_accept(loc_entry);
  advance_loc_entry_l_id(loc_entry, t_id);
  zero_out_the_rmw_reply_loc_entry_metadata(loc_entry);
  reset_all_aboard_accept(loc_entry, t_id);
  check_after_inspecting_accept(loc_entry);
}

static inline bool acc_help_is_nacked(loc_entry_t *loc_entry)
{
  rmw_rep_info_t *rep_info = &loc_entry->rmw_reps;
  bool is_helping = loc_entry->helping_flag != NOT_HELPING;
  bool received_a_nack = rep_info->rmw_id_commited + rep_info->log_too_small +
                         rep_info->already_accepted + rep_info->seen_higher_prop_acc +
                         rep_info->log_too_high > 0;
  check_that_a_nack_is_received(received_a_nack, rep_info);
  check_that_if_nack_and_helping_flag_is_helping(is_helping, received_a_nack, loc_entry);

  return is_helping && received_a_nack;
}

static inline void avoid_values_in_commits_if_possible(loc_entry_t *loc_entry)
{
  if (!ENABLE_COMMITS_WITH_NO_VAL) return;

  if (loc_entry->rmw_reps.acks == MACHINE_NUM) {
    if (loc_entry->helping_flag == HELPING)
      loc_entry->help_loc_entry->avoid_val_in_com = true;
    else  loc_entry->avoid_val_in_com = true;
  }
}

static inline void acc_handle_ack_quorum(loc_entry_t *loc_entry)
{
  check_after_gathering_acc_acks(loc_entry);
  loc_entry->state = (uint8_t) (loc_entry->helping_flag == HELPING ?
                                MUST_BCAST_COMMITS_FROM_HELP : MUST_BCAST_COMMITS);
  avoid_values_in_commits_if_possible(loc_entry);
}


static inline void acc_handle_log_too_high(loc_entry_t *loc_entry)
{
  //on an accept we do not try to commit the previous RMW
  loc_entry->state = RETRY_WITH_BIGGER_TS;
}

static inline bool acc_handle_all_aboard(loc_entry_t *loc_entry,
                                         uint16_t t_id)
{
  check_handle_all_aboard(loc_entry);
  loc_entry->all_aboard_time_out++;
  if (loc_entry->all_aboard_time_out < ALL_ABOARD_TIMEOUT_CNT)
    return true;
  else {
    print_all_aboard_time_out(loc_entry, t_id);
    loc_entry->state = RETRY_WITH_BIGGER_TS;
    loc_entry->all_aboard_time_out = 0;
    loc_entry->new_ts.version = PAXOS_TS;
    return false;
  }
}

static inline bool acc_has_received_ack_quorum(loc_entry_t *loc_entry)
{
  rmw_rep_info_t *rep_info = &loc_entry->rmw_reps;
  uint8_t remote_quorum = (uint8_t) (loc_entry->all_aboard ?
                                     MACHINE_NUM : QUORUM_NUM);
  return rep_info->acks >= remote_quorum;
}

static inline bool handle_quorum_of_acc_reps(cp_core_ctx_t *cp_core_ctx,
                                             loc_entry_t *loc_entry)
{

  rmw_rep_info_t *rep_info = &loc_entry->rmw_reps;

  if (acc_help_is_nacked(loc_entry))
    reinstate_loc_entry_after_helping(loc_entry, cp_core_ctx->t_id);
  else if (rep_info->rmw_id_commited > 0)
    prop_acc_handle_already_committed(cp_core_ctx, loc_entry);
  else if (rep_info->log_too_small > 0)
    prop_acc_handle_log_too_small(loc_entry);
  else if (acc_has_received_ack_quorum(loc_entry))
    acc_handle_ack_quorum(loc_entry);
  else if (rep_info->seen_higher_prop_acc > 0)
    prop_acc_handle_seen_higher_prop(loc_entry);
  else if (rep_info->log_too_high > 0)
    acc_handle_log_too_high(loc_entry);
  else return false;

  return true;
}

static inline void inspect_accepts(cp_core_ctx_t *cp_core_ctx,
                                   loc_entry_t *loc_entry)
{
  check_inspect_accepts(loc_entry);
  loc_entry->rmw_reps.inspected = true;

  bool was_quorum_of_answers_sufficient =
      handle_quorum_of_acc_reps(cp_core_ctx, loc_entry);

  bool need_to_wait_for_more_reps = false;
  if (!was_quorum_of_answers_sufficient)
    need_to_wait_for_more_reps = acc_handle_all_aboard(loc_entry, cp_core_ctx->t_id);

  if (!need_to_wait_for_more_reps)
    clean_up_after_inspecting_accept(loc_entry, cp_core_ctx->t_id);

}

inline void inspect_accepts_if_ready_to_inspect(cp_core_ctx_t *cp_core_ctx,
                                                loc_entry_t *loc_entry)
{
  if (loc_entry->rmw_reps.ready_to_inspect)
    inspect_accepts(cp_core_ctx, loc_entry);
}


/*
 * ----PROPOSES----
 **/

static inline void zero_out_the_rmw_reply_if_not_gone_accepted(loc_entry_t *loc_entry)
{
  bool reps_have_not_been_zeroed = loc_entry->rmw_reps.ready_to_inspect;
  if (reps_have_not_been_zeroed)
    zero_out_the_rmw_reply_loc_entry_metadata(loc_entry);
  else check_when_reps_have_been_zeroes_on_prop(loc_entry);
}

static inline void reset_helping_flag(loc_entry_t *loc_entry)
{
  if (loc_entry->helping_flag == PROPOSE_NOT_LOCALLY_ACKED ||
      loc_entry->helping_flag == PROPOSE_LOCALLY_ACCEPTED)
    loc_entry->helping_flag = NOT_HELPING;
}

static inline void clean_up_after_inspecting_props(loc_entry_t *loc_entry,
                                                   bool zero_out_log_too_high_cntr)
{
  checks_before_resetting_prop(loc_entry);
  reset_helping_flag(loc_entry);
  zero_out_the_rmw_reply_if_not_gone_accepted(loc_entry);
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


static inline void prop_handle_already_accepted(cp_core_ctx_t *cp_core_ctx,
                                                loc_entry_t *loc_entry)
{
  if (loc_entry->helping_flag == PROPOSE_LOCALLY_ACCEPTED)
    act_on_quorum_of_prop_acks(cp_core_ctx, loc_entry, cp_core_ctx->t_id);
  else act_on_receiving_already_accepted_rep_to_prop(cp_core_ctx, loc_entry, cp_core_ctx->t_id);
  check_state_with_allowed_flags(4, (int) loc_entry->state, ACCEPTED,
                                 NEEDS_KV_PTR, MUST_BCAST_COMMITS);
}

static inline bool handle_quorum_of_prop_reps(cp_core_ctx_t *cp_core_ctx,
                                              loc_entry_t *loc_entry,
                                              bool *zero_out_log_too_high_cntr)
{

  rmw_rep_info_t *rep_info = &loc_entry->rmw_reps;

  if (rep_info->rmw_id_commited > 0)
    prop_acc_handle_already_committed(cp_core_ctx, loc_entry);
  else if (rep_info->log_too_small > 0)
    prop_acc_handle_log_too_small(loc_entry);
  else if (rep_info->seen_higher_prop_acc > 0)
    prop_acc_handle_seen_higher_prop(loc_entry);
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
  handle_quorum_of_prop_reps(cp_core_ctx, loc_entry, &zero_out_log_too_high_cntr);
  clean_up_after_inspecting_props(loc_entry, zero_out_log_too_high_cntr);
}


inline void inspect_props_if_ready_to_inspect(cp_core_ctx_t *cp_core_ctx,
                                              loc_entry_t *loc_entry)
{
  if (loc_entry->rmw_reps.ready_to_inspect)
    inspect_proposes(cp_core_ctx, loc_entry, cp_core_ctx->t_id);
}