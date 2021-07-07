//
// Created by vasilis on 06/07/2021.
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

static inline bool help_is_nacked(loc_entry_t *loc_entry)
{
  rmw_rep_info_t *rep_info = &loc_entry->rmw_reps;
  return loc_entry->helping_flag != NOT_HELPING && (
         rep_info->rmw_id_commited  + rep_info->log_too_small +
         rep_info->already_accepted + rep_info->seen_higher_prop_acc +
         rep_info->log_too_high > 0);
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

static inline void go_to_bcast_state_after_gathering_accept_acks(loc_entry_t *loc_entry,
                                                                 uint16_t t_id)
{
  check_after_gathering_acc_acks(loc_entry);
  loc_entry->state = (uint8_t) (loc_entry->helping_flag == HELPING ?
                                MUST_BCAST_COMMITS_FROM_HELP : MUST_BCAST_COMMITS);
  avoid_values_in_commits_if_possible(loc_entry);
}


static inline void handle_log_too_small(loc_entry_t *loc_entry)
{
  //It is impossible for this RMW to still hold the kv_ptr
  loc_entry->state = NEEDS_KV_PTR;
  check_loc_entry_is_not_helping(loc_entry);
}

static inline void handle_quorum_of_acc_reps(cp_core_ctx_t *cp_core_ctx,
                                             loc_entry_t *loc_entry,
                                             uint16_t t_id)
{
  rmw_rep_info_t *rep_info = &loc_entry->rmw_reps;
  uint8_t remote_quorum = (uint8_t) (loc_entry->all_aboard ?
                                     MACHINE_NUM : QUORUM_NUM);
  if (help_is_nacked(loc_entry))
    reinstate_loc_entry_after_helping(loc_entry, t_id);
  else if (rep_info->rmw_id_commited > 0)
    handle_already_committed_rmw(cp_core_ctx, loc_entry);
  else if (rep_info->log_too_small > 0)
    handle_log_too_small(loc_entry);
  else if (rep_info->acks >= remote_quorum)
    go_to_bcast_state_after_gathering_accept_acks(loc_entry, t_id);

    // SEEN HIGHER-TS PROPOSE
  else if (rep_info->seen_higher_prop_acc > 0) {
    // retry by incrementing the highest base_ts seen
    loc_entry->state = RETRY_WITH_BIGGER_TS;
    loc_entry->new_ts.version = rep_info->seen_higher_prop_version;
    check_loc_entry_is_not_helping(loc_entry);
  }
    // LOG TOO HIGH
  else if (rep_info->log_too_high > 0) {
    loc_entry->state = RETRY_WITH_BIGGER_TS;
  }
    // if a quorum of messages have been received but
    // we are waiting for more, then we are doing all aboard
  else if (ENABLE_ALL_ABOARD) {
    if (ENABLE_ASSERTIONS) assert(loc_entry->all_aboard);
    loc_entry->all_aboard_time_out++;
    if (ENABLE_ASSERTIONS) assert(loc_entry->new_ts.version == ALL_ABOARD_TS);
    if (loc_entry->all_aboard_time_out > ALL_ABOARD_TIMEOUT_CNT) {
      // printf("Wrkr %u, Timing out on key %u \n", t_id, loc_entry->key.bkt);
      loc_entry->state = RETRY_WITH_BIGGER_TS;
      loc_entry->all_aboard_time_out = 0;
      loc_entry->new_ts.version = PAXOS_TS;
    }
    else return; // avoid zeroing out the responses
  }
  else if (ENABLE_ASSERTIONS) assert(false);

}

static inline void inspect_accepts(cp_core_ctx_t *cp_core_ctx,
                                   loc_entry_t *loc_entry,
                                   uint16_t t_id)
{
  check_sum_of_reps(loc_entry);
  if (!loc_entry->rmw_reps.ready_to_inspect) return;
  loc_entry->rmw_reps.inspected = true;


  handle_quorum_of_acc_reps(cp_core_ctx, loc_entry, cp_core_ctx->t_id);

  clean_up_after_inspecting_accept(loc_entry, cp_core_ctx->t_id);

}

static inline void inspect_commits(cp_core_ctx_t *cp_core_ctx,
                                   loc_entry_t* loc_entry)
{

  loc_entry_t *entry_to_commit =
      loc_entry->state == MUST_BCAST_COMMITS ? loc_entry : loc_entry->help_loc_entry;
  bool inserted_commit = cp_com_insert(cp_core_ctx->netw_ctx, entry_to_commit, loc_entry->state);
  if (inserted_commit) {
    print_commit_latest_committed(loc_entry, cp_core_ctx->t_id);
    loc_entry->state = COMMITTED;
  }
}


static inline void handle_retry_state(cp_core_ctx_t *cp_core_ctx,
                                      loc_entry_t* loc_entry)
{
  take_kv_ptr_with_higher_TS(cp_core_ctx->stall_info, loc_entry, false, cp_core_ctx->t_id);
  check_state_with_allowed_flags(5, (int) loc_entry->state,
                                 INVALID_RMW,
                                 PROPOSED,
                                 NEEDS_KV_PTR,
                                 MUST_BCAST_COMMITS);
  if (loc_entry->state == PROPOSED)
    cp_prop_insert(cp_core_ctx->netw_ctx, loc_entry);


}

static inline void first_fsm_proped_acced_needs_kv(cp_core_ctx_t *cp_core_ctx,
                                                   loc_entry_t* loc_entry)
{
  switch (loc_entry->state) {
    case INVALID_RMW:
      return;
    case ACCEPTED:
      inspect_accepts(cp_core_ctx, loc_entry, cp_core_ctx->t_id);
      break;
    case PROPOSED:
      inspect_proposes(cp_core_ctx, loc_entry, cp_core_ctx->t_id);
      break;
    case NEEDS_KV_PTR:
      handle_needs_kv_ptr_state(cp_core_ctx, loc_entry, loc_entry->sess_id, cp_core_ctx->t_id);
      break;
    default:
      break;
  }
}

static inline void sec_fsm_bcast_and_retry(cp_core_ctx_t *cp_core_ctx,
                                           loc_entry_t* loc_entry)
{
  switch (loc_entry->state) {
    case RETRY_WITH_BIGGER_TS:
      handle_retry_state(cp_core_ctx, loc_entry);
      break;
    case MUST_BCAST_COMMITS:
    case MUST_BCAST_COMMITS_FROM_HELP:
      inspect_commits(cp_core_ctx, loc_entry);
      break;
    default: break;
  }
}



static inline void rmw_fsms(cp_core_ctx_t *cp_core_ctx,
                            loc_entry_t* loc_entry)
{
  first_fsm_proped_acced_needs_kv(cp_core_ctx, loc_entry);
  sec_fsm_bcast_and_retry(cp_core_ctx, loc_entry);
}

inline void cp_core_inspect_rmws(cp_core_ctx_t *cp_core_ctx)
{
  for (uint16_t sess_i = 0; sess_i < SESSIONS_PER_THREAD; sess_i++) {
    loc_entry_t* loc_entry = &cp_core_ctx->rmw_entries[sess_i];
    check_when_inspecting_rmw(loc_entry, cp_core_ctx->stall_info, sess_i);
    rmw_fsms(cp_core_ctx, loc_entry);
  }
}