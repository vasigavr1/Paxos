//
// Created by vasilis on 06/07/2021.
//

#include <cp_core_util.h>

//inline void inspect_rmws(context_t *ctx, uint16_t t_id)
//{
//  cp_ctx_t *cp_ctx = (cp_ctx_t *) ctx->appl_ctx;
//  for (uint16_t sess_i = 0; sess_i < SESSIONS_PER_THREAD; sess_i++) {
//    loc_entry_t* loc_entry = &cp_ctx->rmw_entries[sess_i];
//    uint8_t state = loc_entry->state;
//    if (state == INVALID_RMW) continue;
//    check_when_inspecting_rmw(loc_entry, &cp_ctx->stall_info, sess_i);
//
//    if (state == ACCEPTED)
//      inspect_accepts(&cp_ctx->stall_info, loc_entry, t_id);
//    if (state == PROPOSED)
//      inspect_proposes(ctx, &cp_ctx->stall_info, loc_entry, t_id);
//
//
//
//    /* =============== RETRY ======================== */
//    if (loc_entry->state == RETRY_WITH_BIGGER_TS) {
//      take_kv_ptr_with_higher_TS(&cp_ctx->stall_info, loc_entry, false, t_id);
//      check_state_with_allowed_flags(5, (int) loc_entry->state, INVALID_RMW, PROPOSED,
//                                     NEEDS_KV_PTR, MUST_BCAST_COMMITS);
//      if (loc_entry->state == PROPOSED) {
//        cp_prop_insert(ctx, loc_entry);
//      }
//    }
//
//    if (loc_entry->state == MUST_BCAST_COMMITS ||
//        loc_entry->state == MUST_BCAST_COMMITS_FROM_HELP) {
//      if (inspect_commits(ctx, loc_entry, cp_ctx->com_rob->capacity))
//        continue;
//    }
//
//    /* =============== NEEDS_KV_PTR ======================== */
//    if (state == NEEDS_KV_PTR) {
//      handle_needs_kv_ptr_state(ctx, &cp_ctx->stall_info, loc_entry, sess_i, t_id);
//    }
//  }
//}