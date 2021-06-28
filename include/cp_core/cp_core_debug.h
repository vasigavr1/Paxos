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

static inline void check_when_filling_loc_entry(loc_entry_t *loc_entry,
                                                uint16_t sess_i,
                                                uint32_t version,
                                                uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    check_global_sess_id((uint8_t) machine_id, t_id,
                         (uint16_t) sess_i, loc_entry->rmw_id.id);
    check_version(version, "fill_loc_rmw_entry_on_grabbing_global");
    assert(!loc_entry->rmw_reps.ready_to_inspect);
    assert(loc_entry->rmw_reps.tot_replies == 0);
  }
}

static inline void check_when_init_loc_entry(loc_entry_t* loc_entry,
                                             trace_op_t *op)
{
  if (ENABLE_ASSERTIONS) {
    assert(op->real_val_len <= RMW_VALUE_SIZE);
    assert(!loc_entry->rmw_reps.ready_to_inspect);
    assert(loc_entry->rmw_reps.tot_replies == 0);
    assert(loc_entry->state == INVALID_RMW);
  }
}

static inline void check_loc_entry_init_rmw_id(loc_entry_t* loc_entry,
                                               uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    assert(loc_entry->rmw_id.id % GLOBAL_SESSION_NUM == loc_entry->glob_sess_id);
    assert(glob_ses_id_to_t_id((uint32_t) (loc_entry->rmw_id.id % GLOBAL_SESSION_NUM)) == t_id &&
           glob_ses_id_to_m_id((uint32_t) (loc_entry->rmw_id.id % GLOBAL_SESSION_NUM)) == machine_id);
  }
}

static inline void check_loc_entry_if_already_committed(loc_entry_t* loc_entry)
{
  if (ENABLE_ASSERTIONS) {
    assert(loc_entry != NULL);
    assert(loc_entry->state != INVALID_RMW);
    assert(loc_entry->helping_flag != HELPING);
  }
}

static inline void check_act_on_quorum_of_commit_acks(loc_entry_t* loc_entry)
{
  if (ENABLE_ASSERTIONS) {
    assert(loc_entry != NULL);
    assert(loc_entry->state == COMMITTED);
  }
}


static inline void check_free_session_from_rmw(loc_entry_t* loc_entry,
                                               sess_stall_t *stall_info,
                                               uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    assert(loc_entry->sess_id < SESSIONS_PER_THREAD);
    assert(loc_entry->state == INVALID_RMW);
    if(stall_info->stalled[loc_entry->sess_id]) {
      my_printf(red, "Wrkr %u sess %u should be stalled \n", t_id, loc_entry->sess_id);
      assert(false);
    }
  }
}

static inline void print_clean_up_after_retrying(mica_op_t *kv_ptr,
                                                 loc_entry_t *loc_entry,
                                                 uint16_t t_id)
{
  if (DEBUG_RMW)
    my_printf(cyan, "Wrkr %u: session %u gets/regains the kv_ptr log %u to do its propose \n",
              t_id, loc_entry->sess_id, kv_ptr->log_no);
}


static inline void check_clean_up_after_retrying(mica_op_t *kv_ptr,
                                                 loc_entry_t *loc_entry,
                                                 bool help_locally_acced,
                                                 uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    assert(loc_entry->accepted_log_no == 0);
    assert(loc_entry->killable);
    assert(!help_locally_acced);
    assert(ENABLE_CAS_CANCELLING);
    //printf("Cancelling on needing kv_ptr Wrkr%u, sess %u, entry %u rmw_failing \n",
    //     t_id, loc_entry->sess_id, loc_entry->index_to_rmw);
  }
}


static inline void checks_acting_on_quorum_of_prop_ack(loc_entry_t *loc_entry, uint16_t t_id)
{
  check_state_with_allowed_flags(4, loc_entry->state, ACCEPTED, NEEDS_KV_PTR, MUST_BCAST_COMMITS);
  if (ENABLE_ASSERTIONS) {
    if (loc_entry->helping_flag == PROPOSE_LOCALLY_ACCEPTED) {
      assert(loc_entry->rmw_reps.tot_replies >= QUORUM_NUM);
      assert(loc_entry->rmw_reps.already_accepted >= 0);
      assert(loc_entry->rmw_reps.seen_higher_prop_acc == 0);
      assert(glob_ses_id_to_t_id((uint32_t) (loc_entry->rmw_id.id % GLOBAL_SESSION_NUM)) == t_id &&
             glob_ses_id_to_m_id((uint32_t) (loc_entry->rmw_id.id % GLOBAL_SESSION_NUM)) == machine_id);

    }
  }
}


static inline void
checks_and_prints_proposed_but_not_locally_acked(sess_stall_t *stall_info,
                                                 mica_op_t *kv_ptr,
                                                 loc_entry_t * loc_entry,
                                                 uint16_t t_id)
{
  if (DEBUG_RMW)
    my_printf(cyan, "Wrkr %u, session %u helps RMW id %u with version %u, m_id %u,"
                    " kv_ptr log/help log %u/%u kv_ptr committed log %u , "
                    " stashed rmw_id: %u state %u \n",
              t_id, loc_entry->sess_id, loc_entry->rmw_id.id,
              loc_entry->new_ts.version, loc_entry->new_ts.m_id,
              kv_ptr->log_no, loc_entry->log_no, kv_ptr->last_committed_log_no,
              loc_entry->help_rmw->rmw_id.id, loc_entry->help_rmw->state);

  if (ENABLE_ASSERTIONS) {
    assert(stall_info->stalled);
    assert(loc_entry->rmw_reps.tot_replies == 0);
  }
}

static inline void check_the_rmw_has_committed(uint64_t glob_sess_id)
{
  if (ENABLE_ASSERTIONS) assert(glob_sess_id < GLOBAL_SESSION_NUM);
}

static inline void print_is_log_too_high(uint32_t log_no,
                                         mica_op_t *kv_ptr,
                                         uint16_t t_id)
{
  if (DEBUG_RMW)
    my_printf(yellow, "Wkrk %u Log number is too high %u/%u entry state %u \n",
              t_id, log_no, kv_ptr->last_committed_log_no,
              kv_ptr->state);
}

static inline void check_is_log_too_high(uint32_t log_no, mica_op_t *kv_ptr)
{
  if (ENABLE_ASSERTIONS) {
    assert(log_no == kv_ptr->log_no);
    if (log_no != kv_ptr->accepted_log_no)
      printf("log_no %u, kv_ptr accepted_log_no %u, kv_ptr log no %u, kv_ptr->state %u \n",
             log_no, kv_ptr->accepted_log_no, kv_ptr->log_no, kv_ptr->state);
    //assert(log_no == kv_ptr->accepted_log_no);
    //assert(kv_ptr->state == ACCEPTED);
  }
}

static inline void check_search_prop_entries_with_l_id(uint16_t entry)
{
  if (ENABLE_ASSERTIONS) assert(entry < LOCAL_PROP_NUM);
}

static inline void check_activate_kv_pair(uint8_t state, mica_op_t *kv_ptr,
                                          uint32_t log_no, const char *message)
{
  if (ENABLE_ASSERTIONS) {
    if (kv_ptr->log_no == log_no && kv_ptr->state == ACCEPTED && state != ACCEPTED) {
      printf("%s \n", message);
      assert(false);
    }
    assert(kv_ptr->log_no <= log_no);
  }
}

static inline void check_activate_kv_pair_accepted(mica_op_t *kv_ptr,
                                                   uint32_t new_version,
                                                   uint8_t new_ts_m_id)
{
  if (ENABLE_ASSERTIONS) {
    assert(kv_ptr->prop_ts.version == new_version);
    assert(kv_ptr->prop_ts.m_id == new_ts_m_id);
    kv_ptr->accepted_rmw_id = kv_ptr->rmw_id;
  }
}

static inline void check_after_activate_kv_pair(mica_op_t *kv_ptr,
                                                const char *message,
                                                uint8_t state,
                                                uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    if (committed_glob_sess_rmw_id[kv_ptr->rmw_id.id % GLOBAL_SESSION_NUM] >= kv_ptr->rmw_id.id) {
      //my_printf(red, "Wrkr %u, attempts to activate with already committed RMW id %u/%u glob_sess id %u, state %u: %s \n",
      //           t_id, kv_ptr->rmw_id.id, committed_glob_sess_rmw_id[kv_ptr->rmw_id.id % GLOBAL_SESSION_NUM],
      //           kv_ptr->rmw_id.id % GLOBAL_SESSION_NUM, state, message);
    }
    assert(state == PROPOSED || state == ACCEPTED);
    assert(kv_ptr->last_committed_log_no < kv_ptr->log_no);
  }
}


#endif