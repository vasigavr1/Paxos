#ifndef CP_PAXOS_DEBUG_UTIL_H
#define CP_PAXOS_DEBUG_UTIL_H

#include <cp_config.h>
#include "cp_main.h"
#include "cp_generic_util.h"
#include "od_debug_util.h"
#include "od_network_context.h"


static inline void checks_before_resetting_accept(loc_entry_t *loc_entry)
{
  check_state_with_allowed_flags(6, (int) loc_entry->state,
                                 INVALID_RMW,          // already committed -- no broadcasts
                                 RETRY_WITH_BIGGER_TS, //log-too-high
                                 NEEDS_KV_PTR,         // log-too-small
                                 MUST_BCAST_COMMITS_FROM_HELP, // ack-quorum for help
                                 MUST_BCAST_COMMITS            // ack-quorum or already committed
  );

  check_state_with_allowed_flags(4, (int) loc_entry->helping_flag,
                                 NOT_HELPING,
                                 HELPING);
}

static inline void checks_before_resetting_prop(loc_entry_t *loc_entry)
{
  check_state_with_allowed_flags(7, (int) loc_entry->state,
                                 INVALID_RMW,          // Already-committed, no need to send commits
                                 RETRY_WITH_BIGGER_TS, // Log-too-high
                                 ACCEPTED,             // Acks or prop_locally_accepted or helping
                                 NEEDS_KV_PTR,         // log-too-small, failed to accept or failed to help because kv_ptr is taken
                                 MUST_BCAST_COMMITS,   //  already-committed, accept attempt found it's already committd
                                 MUST_BCAST_COMMITS_FROM_HELP // log-too-hig timeout
  );
  check_state_with_allowed_flags(7, (int) loc_entry->helping_flag,
                                 NOT_HELPING,
                                 HELPING,
                                 PROPOSE_NOT_LOCALLY_ACKED,
                                 PROPOSE_LOCALLY_ACCEPTED,
                                 HELP_PREV_COMMITTED_LOG_TOO_HIGH,
                                 HELPING_MYSELF);
}
static inline void check_after_inspecting_accept(loc_entry_t *loc_entry)
{

  check_state_with_allowed_flags(7, (int) loc_entry->state, ACCEPTED, INVALID_RMW, RETRY_WITH_BIGGER_TS,
                                 NEEDS_KV_PTR, MUST_BCAST_COMMITS, MUST_BCAST_COMMITS_FROM_HELP);
  if (ENABLE_ASSERTIONS && loc_entry->rmw_reps.ready_to_inspect)
    assert(loc_entry->state == ACCEPTED && loc_entry->all_aboard);
}


static inline void check_after_inspecting_prop(loc_entry_t *loc_entry)
{
  check_state_with_allowed_flags(7, (int) loc_entry->state, INVALID_RMW, RETRY_WITH_BIGGER_TS,
                                 NEEDS_KV_PTR, ACCEPTED, MUST_BCAST_COMMITS, MUST_BCAST_COMMITS_FROM_HELP);
  if (ENABLE_ASSERTIONS) assert(!loc_entry->rmw_reps.ready_to_inspect);
  if (loc_entry->state != ACCEPTED) assert(loc_entry->rmw_reps.tot_replies == 0);
  else
    assert(loc_entry->rmw_reps.tot_replies == 1);
}

// Check that the counter for propose replies add up (SAME FOR ACCEPTS AND PROPS)
static inline void check_sum_of_reps(loc_entry_t *loc_entry)
{
  if (ENABLE_ASSERTIONS) {
    assert(loc_entry->rmw_reps.tot_replies == sum_of_reps(&loc_entry->rmw_reps));
    assert(loc_entry->rmw_reps.tot_replies <= MACHINE_NUM);
  }
}

static inline void check_when_inspecting_rmw(loc_entry_t* loc_entry,
                                             sess_stall_t *stall_info,
                                             uint16_t sess_i)
{
  if (ENABLE_ASSERTIONS) {
    assert(loc_entry->sess_id == sess_i);
    assert(stall_info->stalled[sess_i]);
  }
}

static inline void print_commit_latest_committed(loc_entry_t* loc_entry,
                                                 uint16_t t_id)
{
  if (loc_entry->state == MUST_BCAST_COMMITS_FROM_HELP &&
      loc_entry->helping_flag == PROPOSE_NOT_LOCALLY_ACKED) {
    my_printf(green, "Wrkr %u sess %u will bcast commits for the latest committed RMW,"
                     " after learning its proposed RMW has already been committed \n",
              t_id, loc_entry->sess_id);
  }
}

#endif