//
// Created by vasilis on 06/07/2021.
//

#include <cp_core_util.h>

// Check if it's a commit without a value -- if it cannot be committed
// then do not attempt to overwrite the value and timestamp, because the commit's
// value and ts are stored in the kv_ptr->accepted_value/ts and may have been lost
static inline void handle_commit_with_no_val (mica_op_t *kv_ptr,
                                              commit_info_t *com_info,
                                              uint16_t t_id)
{
  if (com_info->no_value) {
    if (!can_process_com_no_value(kv_ptr, com_info, t_id)) {
      com_info->overwrite_kv = false;
    }
  }
}

static inline void clear_kv_state_advance_log_no (mica_op_t *kv_ptr,
                                                  commit_info_t *com_info)
{
  if (kv_ptr->log_no <= com_info->log_no) {
    kv_ptr->log_no = com_info->log_no;
    kv_ptr->state = INVALID_RMW;
  }
}

static inline void apply_val_if_carts_bigger(mica_op_t *kv_ptr,
                                             commit_info_t *com_info,
                                             uint16_t t_id)
{
  if (com_info->overwrite_kv) {
    compare_t cart_comp = compare_carts(&com_info->base_ts, com_info->log_no,
                                        &kv_ptr->ts, kv_ptr->last_committed_log_no);
    check_on_overwriting_commit_algorithm(kv_ptr, com_info, cart_comp, t_id);
    if (cart_comp == GREATER) {
      write_kv_ptr_val(kv_ptr, com_info->value, (size_t) VALUE_SIZE, com_info->flag);
      kv_ptr->ts = com_info->base_ts;
    }
  }
}

static inline void advance_last_comm_log_no_and_rmw_id(mica_op_t *kv_ptr,
                                                       commit_info_t *com_info,
                                                       uint16_t t_id)
{
  check_on_updating_rmw_meta_commit_algorithm(kv_ptr, com_info, t_id);
  if (kv_ptr->last_committed_log_no < com_info->log_no) {
    kv_ptr->last_committed_log_no = com_info->log_no;
    kv_ptr->last_committed_rmw_id = com_info->rmw_id;
  }
}

static inline void register_commit(mica_op_t *kv_ptr,
                                   commit_info_t *com_info,
                                   uint16_t t_id)
{
  register_committed_rmw_id(com_info->rmw_id.id, t_id);

  check_registered_against_kv_ptr_last_committed(kv_ptr, com_info->rmw_id.id,
                                                 com_info->message, t_id);
}

static inline void commit_algorithm(mica_op_t *kv_ptr,
                                    commit_info_t *com_info,
                                    uint16_t t_id)
{
  check_inputs_commit_algorithm(kv_ptr, com_info, t_id);

  lock_kv_ptr(kv_ptr, t_id); {
    check_state_before_commit_algorithm(kv_ptr, com_info, t_id);
    handle_commit_with_no_val(kv_ptr, com_info, t_id);
    clear_kv_state_advance_log_no(kv_ptr, com_info);
    apply_val_if_carts_bigger(kv_ptr, com_info, t_id);
    advance_last_comm_log_no_and_rmw_id(kv_ptr, com_info, t_id);
    register_commit(kv_ptr, com_info, t_id);
  }
  unlock_kv_ptr(kv_ptr, t_id);
}

static inline void fil_commit_info_based_on_flag(void* rmw,
                                                 loc_entry_t *loc_entry,
                                                 commit_info_t *com_info,
                                                 uint8_t flag)
{
  switch (flag) {
    case FROM_LOG_TOO_LOW_REP:
      fill_commit_info_from_rep(com_info, rmw, flag);
      break;
    case FROM_ALREADY_COMM_REP:
    case FROM_LOCAL:
      fill_commit_info_from_local(com_info, loc_entry, flag);
      break;
    case FROM_ALREADY_COMM_REP_HELP:
    case FROM_LOCAL_HELP:
      fill_commit_info_from_loc_help(com_info, loc_entry->help_loc_entry, flag);
      break;
    case FROM_REMOTE_COMMIT:
      fill_commit_info_from_rem_commit(com_info, rmw, flag);
      break;
    case FROM_REMOTE_COMMIT_NO_VAL:
      fill_commit_info_from_rem_commit_no_val(com_info, rmw, flag);
      break;
    default: my_assert(false, "");
  }
}

inline void commit_rmw(mica_op_t *kv_ptr,
                       void* rmw,
                       loc_entry_t *loc_entry,
                       uint8_t flag,
                       uint16_t t_id)
{

  process_commit_flags(rmw, loc_entry, &flag);
  commit_info_t com_info;
  fil_commit_info_based_on_flag(rmw, loc_entry, &com_info, flag);
  commit_algorithm(kv_ptr, &com_info, t_id);
}


static inline void free_session_and_reinstate_loc_entry(sess_stall_t *stall_info,
                                                        loc_entry_t *loc_entry,
                                                        uint16_t t_id)
{
  switch(loc_entry->helping_flag)
  {
    case NOT_HELPING:
    case PROPOSE_NOT_LOCALLY_ACKED:
    case PROPOSE_LOCALLY_ACCEPTED:
      loc_entry->state = INVALID_RMW;
      free_session_from_rmw(loc_entry, stall_info, true, t_id);
      break;
    case HELPING:
      reinstate_loc_entry_after_helping(loc_entry, t_id);
      break;
    case HELP_PREV_COMMITTED_LOG_TOO_HIGH:
      loc_entry->state = RETRY_WITH_BIGGER_TS;
      loc_entry->helping_flag = NOT_HELPING;
      break;
    default: my_assert(false, "");
  }
}

// On gathering quorum of acks for commit, commit locally and signal that the session must be freed if not helping
inline void act_on_quorum_of_commit_acks(cp_core_ctx_t *cp_core_ctx,
                                         uint16_t sess_id)
{
  loc_entry_t *loc_entry = &cp_core_ctx->rmw_entries[sess_id];
  check_act_on_quorum_of_commit_acks(loc_entry);

  if (loc_entry->helping_flag != HELP_PREV_COMMITTED_LOG_TOO_HIGH)
    commit_rmw(loc_entry->kv_ptr, NULL, loc_entry, FROM_LOCAL, cp_core_ctx->t_id);

  free_session_and_reinstate_loc_entry(cp_core_ctx->stall_info, loc_entry,cp_core_ctx->t_id);
}