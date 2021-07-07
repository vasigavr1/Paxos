//
// Created by vasilis on 07/07/2021.
//

#include <cp_core_util.h>

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

static inline void acc_handle_ack_quorum(loc_entry_t *loc_entry,
                                         uint16_t t_id)
{
  check_after_gathering_acc_acks(loc_entry);
  loc_entry->state = (uint8_t) (loc_entry->helping_flag == HELPING ?
                                MUST_BCAST_COMMITS_FROM_HELP : MUST_BCAST_COMMITS);
  avoid_values_in_commits_if_possible(loc_entry);
}

static inline void acc_handle_log_too_small(loc_entry_t *loc_entry)
{
  //It is impossible for this RMW to still hold the kv_ptr
  loc_entry->state = NEEDS_KV_PTR;
  check_loc_entry_is_not_helping(loc_entry);
}

static inline void acc_handle_seen_higher_prop(loc_entry_t *loc_entry)
{
  // retry by incrementing the highest base_ts seen
  loc_entry->state = RETRY_WITH_BIGGER_TS;
  loc_entry->new_ts.version = loc_entry->rmw_reps.seen_higher_prop_version;
  check_loc_entry_is_not_helping(loc_entry);
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
    handle_already_committed_rmw(cp_core_ctx, loc_entry);
  else if (rep_info->log_too_small > 0)
    acc_handle_log_too_small(loc_entry);
  else if (acc_has_received_ack_quorum(loc_entry))
    acc_handle_ack_quorum(loc_entry, cp_core_ctx->t_id);
  else if (rep_info->seen_higher_prop_acc > 0)
    acc_handle_seen_higher_prop(loc_entry);
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