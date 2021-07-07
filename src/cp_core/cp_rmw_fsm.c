//
// Created by vasilis on 06/07/2021.
//

#include <cp_core_util.h>




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
    case ACCEPTED:
      inspect_accepts_if_ready_to_inspect(cp_core_ctx, loc_entry);
      break;
    case PROPOSED:
      inspect_props_if_ready_to_inspect(cp_core_ctx, loc_entry);
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