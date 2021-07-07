#ifndef CP_PAXOS_DEBUG_UTIL_H
#define CP_PAXOS_DEBUG_UTIL_H

#include <cp_config.h>
#include "cp_main.h"
#include "cp_generic_util.h"
#include "od_debug_util.h"
#include "od_network_context.h"

static inline void check_rmw_ids_of_kv_ptr(mica_op_t *kv_ptr, uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    if (kv_ptr->state != INVALID_RMW) {
      if (kv_ptr->last_committed_rmw_id.id == kv_ptr->rmw_id.id) {
        my_printf(red, "Wrkr %u Last committed rmw id is equal to current, kv_ptr state %u, com log/log %u/%u "
                       "rmw id %u/%u,  \n",
                  t_id, kv_ptr->state, kv_ptr->last_committed_log_no, kv_ptr->log_no,
                  kv_ptr->last_committed_rmw_id.id, kv_ptr->rmw_id.id);
        assert(false);
      }
    }
  }
}

static inline void check_log_nos_of_kv_ptr(mica_op_t *kv_ptr, const char *message, uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    bool equal_plus_one = kv_ptr->last_committed_log_no + 1 == kv_ptr->log_no;
    bool equal = kv_ptr->last_committed_log_no == kv_ptr->log_no;
    assert((equal_plus_one) ||
           (equal && kv_ptr->state == INVALID_RMW));

    if (kv_ptr->state != INVALID_RMW) {
            if (kv_ptr->last_committed_log_no >= kv_ptr->log_no) {
        my_printf(red, "Wrkr %u t_id, kv_ptr state %u, com log/log %u/%u : %s \n",
                  t_id, kv_ptr->state, kv_ptr->last_committed_log_no, kv_ptr->log_no, message);
        assert(false);
      }
    }
  }
}

static inline void check_kv_ptr_invariants(mica_op_t *kv_ptr, uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    check_log_nos_of_kv_ptr(kv_ptr, "check_kv_ptr_invariants", t_id);
    check_rmw_ids_of_kv_ptr(kv_ptr, t_id);
  }
}

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
  check_state_with_allowed_flags(6, (int) loc_entry->helping_flag,
                                 NOT_HELPING,
                                 HELPING,
                                 PROPOSE_NOT_LOCALLY_ACKED,
                                 PROPOSE_LOCALLY_ACCEPTED,
                                 HELP_PREV_COMMITTED_LOG_TOO_HIGH);
}

static inline void check_loc_entry_help_flag(loc_entry_t *loc_entry,
                                             uint8_t state,
                                             bool expected_to_be)
{
  if (ENABLE_ASSERTIONS) {
    bool state_is_correct = expected_to_be ?
                 loc_entry->helping_flag == state :
                 loc_entry->helping_flag != state;
    if (!state_is_correct){
      my_printf(red,"Expecting helping flag to %s be %s, flag is %s \n",
                expected_to_be ? "" : "not",
                help_state_to_str(state),
                help_state_to_str(loc_entry->helping_flag));
      assert(false);
    }
  }
}

static inline void check_loc_entry_help_flag_is(loc_entry_t *loc_entry,
                                                uint8_t expected_state)
{
  check_loc_entry_help_flag(loc_entry, expected_state, true);
}

static inline void check_loc_entry_help_flag_is_not(loc_entry_t *loc_entry,
                                                    uint8_t expected_state)
{
  check_loc_entry_help_flag(loc_entry, expected_state, false);
}

static inline void check_loc_entry_is_not_helping(loc_entry_t *loc_entry)
{
  check_loc_entry_help_flag_is(loc_entry, NOT_HELPING);
}

static inline void check_loc_entry_is_helping(loc_entry_t *loc_entry)
{
  check_loc_entry_help_flag_is(loc_entry, HELPING);
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
    if (loc_entry->state != INVALID_RMW)
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
    if (loc_entry->helping_flag == HELPING &&
        rmw_ids_are_equal(&loc_entry->help_loc_entry->rmw_id, &loc_entry->rmw_id)) {
      my_printf(red, "Helping myself, but should not\n");
    }
  }
}


static inline void check_free_session_from_rmw(loc_entry_t* loc_entry,
                                               sess_stall_t *stall_info,
                                               uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    assert(loc_entry->sess_id < SESSIONS_PER_THREAD);
    assert(loc_entry->state == INVALID_RMW);
    if(!stall_info->stalled[loc_entry->sess_id]) {
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

static inline void print_log_too_small(uint32_t log_no,
                                       mica_op_t *kv_ptr,
                                       uint64_t rmw_l_id,
                                       uint16_t t_id)
{
  if (DEBUG_RMW)
    my_printf(yellow, "Wkrk %u Log number is too small %u/%u entry state %u, propose/accept with rmw_lid %u,"
                      " \n", t_id, log_no, kv_ptr->last_committed_log_no,
              kv_ptr->state, rmw_l_id);

}

static inline void print_if_log_is_higher_than_local(uint32_t log_no,
                                                     mica_op_t *kv_ptr,
                                                     uint64_t rmw_l_id,
                                                     uint16_t t_id)
{
  if (DEBUG_RMW) { // remote log is higher than the locally stored!
    if (kv_ptr->log_no < log_no && log_no > 1)
      my_printf(yellow, "Wkrk %u Log number is higher than expected %u/%u, entry state %u, "
                        "propose/accept with rmw_lid %u\n",
                t_id, log_no, kv_ptr->log_no,
                kv_ptr->state, rmw_l_id);
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

static inline void checks_after_local_accept(mica_op_t *kv_ptr,
                                             loc_entry_t *loc_entry,
                                             uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    assert(loc_entry->accepted_log_no == loc_entry->log_no);
    assert(loc_entry->log_no == kv_ptr->last_committed_log_no + 1);
    assert(compare_ts(&kv_ptr->prop_ts, &kv_ptr->accepted_ts) != SMALLER);
    kv_ptr->accepted_rmw_id = kv_ptr->rmw_id;
  }
  if (ENABLE_DEBUG_RMW_KV_PTR) {
    //kv_ptr->dbg->proposed_ts = loc_entry->new_ts;
    //kv_ptr->dbg->proposed_log_no = loc_entry->log_no;
    //kv_ptr->dbg->proposed_rmw_id = loc_entry->rmw_id;
  }
  check_log_nos_of_kv_ptr(kv_ptr, "attempt_local_accept and succeed", t_id);
}


static inline void checks_after_failure_to_locally_accept(mica_op_t *kv_ptr,
                                                          loc_entry_t *loc_entry,
                                                          uint16_t t_id)
{
  if (DEBUG_RMW)
    my_printf(green, "Wrkr %u failed to get rmw id %u, accepted locally "
                     "kv_ptr rmw id %u, state %u \n",
              t_id, loc_entry->rmw_id.id,
              kv_ptr->rmw_id.id, kv_ptr->state);
  // --CHECKS--
  if (ENABLE_ASSERTIONS) {
    if (kv_ptr->state == PROPOSED || kv_ptr->state == ACCEPTED) {
      if(!(compare_ts(&kv_ptr->prop_ts, &loc_entry->new_ts) == GREATER ||
           kv_ptr->log_no > loc_entry->log_no)) {
        my_printf(red, "State: %s,  loc-entry-helping %d, Kv prop/base_ts %u/%u -- loc-entry base_ts %u/%u, "
                       "kv-log/loc-log %u/%u kv-rmw_id/loc-rmw-id %u/%u\n",
                  kv_ptr->state == ACCEPTED ? "ACCEPTED" : "PROPOSED",
                  loc_entry->helping_flag,
                  kv_ptr->prop_ts.version, kv_ptr->prop_ts.m_id,
                  loc_entry->new_ts.version, loc_entry->new_ts.m_id,
                  kv_ptr->log_no, loc_entry->log_no,
                  kv_ptr->rmw_id.id, loc_entry->rmw_id.id);
        assert(false);
      }
    }
    else if (kv_ptr->state == INVALID_RMW) // some other rmw committed
      // with cancelling it is possible for some other RMW to stole and then cancelled itself
      if (!ENABLE_CAS_CANCELLING) assert(kv_ptr->last_committed_log_no >= loc_entry->log_no);
  }


  check_log_nos_of_kv_ptr(kv_ptr, "attempt_local_accept and fail", t_id);
}

static inline void checks_after_local_accept_help(mica_op_t *kv_ptr,
                                                  loc_entry_t *loc_entry,
                                                  uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    assert(compare_ts(&kv_ptr->prop_ts, &kv_ptr->accepted_ts) != SMALLER);
    kv_ptr->accepted_rmw_id = kv_ptr->rmw_id;
    check_log_nos_of_kv_ptr(kv_ptr, "attempt_local_accept_to_help and succeed", t_id);
  }
}


static inline void checks_after_failure_to_locally_accept_help(mica_op_t *kv_ptr,
                                                               loc_entry_t *loc_entry,
                                                               uint16_t t_id)
{
  if (DEBUG_RMW)
    my_printf(green, "Wrkr %u sess %u failed to get rmw id %u, accepted locally "
                     "kv_ptr rmw id %u, state %u \n",
              t_id, loc_entry->sess_id, loc_entry->rmw_id.id,
              kv_ptr->rmw_id.id, kv_ptr->state);


  check_log_nos_of_kv_ptr(kv_ptr, "attempt_local_accept_to_help and fail", t_id);
}

static inline void check_state_before_commit_algorithm(mica_op_t *kv_ptr,
                                                       commit_info_t *com_info,
                                                       uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    if (com_info->flag == FROM_LOCAL || com_info->flag == FROM_LOCAL_HELP) {
      // make sure that if we are on the same log
      if (kv_ptr->log_no == com_info->log_no) {
        if (!rmw_ids_are_equal(&com_info->rmw_id, &kv_ptr->rmw_id)) {
          my_printf(red, "kv_ptr is on same log as what is about to be committed but on different rmw-id \n");
          print_commit_info(com_info, yellow, t_id);
          print_kv_ptr(kv_ptr, cyan, t_id);
          // this is a hard error
          assert(false);
        }
        if (kv_ptr->state != INVALID_RMW) {
          if (kv_ptr->state != ACCEPTED) {
            my_printf(red, "Committing: Logs are equal, rmw-ids are equal "
                           "but state is not accepted \n");
            print_commit_info(com_info, yellow, t_id);
            print_kv_ptr(kv_ptr, cyan, t_id);
            assert(false);
          }
        }
      }
      else {
        // if the log has moved on then the RMW has been helped,
        // it has been committed in the other machines so there is no need to change its state
        check_log_nos_of_kv_ptr(kv_ptr, "commit_helped_or_local_from_loc_entry", t_id);
        if (ENABLE_ASSERTIONS) {
          if (kv_ptr->state != INVALID_RMW)
            assert(!rmw_ids_are_equal(&kv_ptr->rmw_id, &com_info->rmw_id));
        }
      }
    }
    else if (com_info->flag == FROM_REMOTE_COMMIT_NO_VAL) {
      if (kv_ptr->last_committed_log_no < com_info->log_no) {
        if (ENABLE_ASSERTIONS) {
          assert(kv_ptr->state == ACCEPTED);
          assert(kv_ptr->log_no == com_info->log_no);
          assert(kv_ptr->accepted_rmw_id.id == com_info->rmw_id.id);
        }
      }
    }
  }
}



static inline void check_propose_snoops_entry(cp_prop_t *prop,
                                              mica_op_t *kv_ptr)
{
  if (ENABLE_ASSERTIONS)  {
    assert(prop->opcode == PROPOSE_OP);
    assert(prop->log_no > kv_ptr->last_committed_log_no);
    assert(prop->log_no == kv_ptr->log_no);
    assert(check_entry_validity_with_key(&prop->key, kv_ptr));
  }
}



static inline void check_accept_snoops_entry(cp_acc_t *acc,
                                             mica_op_t *kv_ptr)
{
  if (ENABLE_ASSERTIONS)  {
    assert(acc->opcode == ACCEPT_OP);
    assert(acc->log_no > kv_ptr->last_committed_log_no);
    assert(acc->log_no == kv_ptr->log_no);
    assert(check_entry_validity_with_key(&acc->key, kv_ptr));
  }
}

static inline void print_accept_snoops_entry(cp_acc_t *acc,
                                             mica_op_t *kv_ptr,
                                             compare_t ts_comp,
                                             uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    if (DEBUG_RMW && ts_comp == EQUAL && kv_ptr->state == ACCEPTED)
      my_printf(red, "Wrkr %u Received Accept for the same TS as already accepted, "
                     "version %u/%u m_id %u/%u, rmw_id %u/%u\n",
                t_id, acc->ts.version, kv_ptr->prop_ts.version,
                acc->ts.m_id,
                kv_ptr->prop_ts.m_id, acc->t_rmw_id,
                kv_ptr->rmw_id.id);
  }
}

static inline void print_check_after_accept_snoops_entry(cp_acc_t *acc,
                                                         mica_op_t *kv_ptr,
                                                         cp_rmw_rep_t *rep,
                                                         uint8_t return_flag,
                                                         uint16_t t_id)
{
  if (DEBUG_RMW)
    my_printf(yellow, "Wrkr %u: %s Accept with rmw_id %u, "
                      "log_no: %u, base_ts.version: %u, ts_m_id %u,"
                      "locally stored state: %u, "
                      "locally stored base_ts: version %u, m_id %u \n",
              t_id, return_flag == RMW_ACK ? "Acks" : "Nacks",
              acc->t_rmw_id, acc->log_no,
              acc->ts.version, acc->ts.m_id, kv_ptr->state,
              kv_ptr->prop_ts.version,
              kv_ptr->prop_ts.m_id);

  if (ENABLE_ASSERTIONS)
    assert(return_flag == RMW_ACK || rep->ts.version > 0);
}


static inline void check_log_no_on_ack_remote_prop_acc(mica_op_t *kv_ptr,
                                                       uint32_t log_no)
{
  if (ENABLE_ASSERTIONS) {
    assert(log_no == kv_ptr->last_committed_log_no + 1);
    assert(kv_ptr->log_no == kv_ptr->last_committed_log_no);
  }
}

static inline void check_create_prop_rep(cp_prop_t *prop,
                                         mica_op_t *kv_ptr)
{
  if (ENABLE_ASSERTIONS) {
    assert(kv_ptr->prop_ts.version >= prop->ts.version);
    check_keys_with_one_trace_op(&prop->key, kv_ptr);
  }
}

static inline uint64_t dbg_kv_ptr_create_acc_prop_rep(mica_op_t *kv_ptr,
                                                      uint64_t *number_of_reqs)
{
  if (ENABLE_DEBUG_RMW_KV_PTR) {
    // kv_ptr->dbg->prop_acc_num++;
    // number_of_reqs = kv_ptr->dbg->prop_acc_num;
  }
}


// After registering, make sure the registered is bigger/equal to what is saved as registered
static inline void check_registered_against_kv_ptr_last_committed(mica_op_t *kv_ptr,
                                                                  uint64_t committed_id,
                                                                  const char *message, uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    uint32_t committed_glob_ses_id = (uint32_t)(committed_id % GLOBAL_SESSION_NUM);
    uint32_t glob_sess_id = (uint32_t)(kv_ptr->last_committed_rmw_id.id % GLOBAL_SESSION_NUM);
    uint64_t id = kv_ptr->last_committed_rmw_id.id;
    assert(glob_sess_id < GLOBAL_SESSION_NUM);
    if (committed_glob_sess_rmw_id[glob_sess_id] < id) {
      my_printf(yellow, "Committing %s rmw_id: %u glob_sess_id: %u \n", message, committed_id, committed_glob_ses_id);
      my_printf(red, "Wrkr %u: %s rmw_id: kv_ptr last committed %lu, "
                     "glob_sess_id :kv_ptr last committed %u,"
                     "committed_glob_sess_rmw_id %lu,   \n", t_id, message,
                kv_ptr->last_committed_rmw_id.id,
                glob_sess_id,
                committed_glob_sess_rmw_id[glob_sess_id]);
      //assert(false);
    }
  }
}

static inline void check_fill_com_info(uint32_t log_no)
{
  if (ENABLE_ASSERTIONS) assert(log_no > 0);
}



static inline void comment_on_why_we_dont_check_if_rmw_committed()
{
  // We don't need to check if the RMW is already registered (committed) in
  // (attempt_local_accept_to_help)-- it's not wrong to do so--
  // but if the RMW has been committed, it will be in the present log_no
  // and we will not be able to accept locally anyway.
}

static inline void check_store_rmw_rep_to_help_loc_entry(loc_entry_t* loc_entry,
                                                         cp_rmw_rep_t* prop_rep,
                                                         compare_t ts_comp)
{
  if (ENABLE_ASSERTIONS) {
    loc_entry_t *help_loc_entry = loc_entry->help_loc_entry;
    if (loc_entry->helping_flag == PROPOSE_LOCALLY_ACCEPTED) {
      assert(help_loc_entry->new_ts.version > 0);
      assert(help_loc_entry->state == ACCEPTED);
      assert(ts_comp != EQUAL); // It would have been an SAME_ACC_ACK
    }
    assert(help_loc_entry->state == INVALID_RMW || help_loc_entry->state == ACCEPTED);
  }
}

static inline void check_handle_prop_or_acc_rep_ack(cp_rmw_rep_mes_t *rep_mes,
                                                    rmw_rep_info_t *rep_info,
                                                    bool is_accept,
                                                    uint16_t t_id)
{
  if (ENABLE_ASSERTIONS)
    assert(rep_mes->m_id < MACHINE_NUM && rep_mes->m_id != machine_id);
  if (DEBUG_RMW)
    my_printf(green, "Wrkr %u, the received rep is an %s ack, "
                     "total acks %u \n", t_id, is_accept ? "acc" : "prop",
              rep_info->acks);
}

static inline void check_handle_rmw_rep_seen_lower_acc(loc_entry_t* loc_entry,
                                                       cp_rmw_rep_t *rep,
                                                       bool is_accept)
{
  if (ENABLE_ASSERTIONS) {
    assert(compare_netw_ts_with_ts(&rep->ts, &loc_entry->new_ts) == SMALLER);
    assert(!is_accept);
  }
}


static inline void print_handle_rmw_rep_seen_higher(cp_rmw_rep_t *rep,
                                                    rmw_rep_info_t *rep_info,
                                                    bool is_accept,
                                                    uint16_t t_id)
{
  if (DEBUG_RMW)
    my_printf(yellow, "Wrkr %u: the %s rep is %u, %u sum of all other reps %u \n", t_id,
              is_accept ? "acc" : "prop",rep->opcode,
              rep_info->seen_higher_prop_acc,
              rep_info->rmw_id_commited + rep_info->log_too_small +
              rep_info->already_accepted);

}


static inline void print_handle_rmw_rep_higher_ts(rmw_rep_info_t *rep_info,
                                                  uint16_t t_id)
{
  if (DEBUG_RMW)
    my_printf(yellow, "Wrkr %u: overwriting the TS version %u \n",
              t_id, rep_info->seen_higher_prop_version);

}


static inline void check_handle_rmw_rep_end(loc_entry_t* loc_entry,
                                            bool is_accept)
{
  if (ENABLE_ASSERTIONS) {
    if (is_accept) assert(loc_entry->state == ACCEPTED);
    if (!is_accept) assert(loc_entry->state == PROPOSED);
    check_sum_of_reps(loc_entry);
  }

}

static inline void check_find_local_and_handle_rmw_rep(loc_entry_t *loc_entry_array,
                                                       cp_rmw_rep_t *rep,
                                                       cp_rmw_rep_mes_t *rep_mes,
                                                       uint16_t byte_ptr,
                                                       bool is_accept,
                                                       uint16_t r_rep_i,
                                                       uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    assert(loc_entry_array != NULL);
    if (!opcode_is_rmw_rep(rep->opcode)) {
      printf("Rep_i %u, current opcode %u first opcode: %u, byte_ptr %u \n",
             r_rep_i, rep->opcode, rep_mes->rmw_rep[0].opcode, byte_ptr);
    }
    assert(opcode_is_rmw_rep(rep->opcode));
    //    if (prop_info->l_id <= rep->l_id)
    //      my_printf(red, "Wrkr %u, rep_i %u, opcode %u, is_accept %d, incoming rep l_id %u, max prop lid %u \n",
    //                t_id, r_rep_i, rep->opcode, is_accept, rep->l_id, prop_info->l_id);
    //
    //    assert(prop_info->l_id > rep->l_id);
  }
  //my_printf(cyan, "RMW rep opcode %u, l_id %u \n", rep->opcode, rep->l_id);

}


static inline void check_zero_out_the_rmw_reply(loc_entry_t* loc_entry)
{
  if (ENABLE_ASSERTIONS) { // make sure the loc_entry is correctly set-up
    if (loc_entry->help_loc_entry == NULL) {
      my_printf(red, "When Zeroing: The help_loc_ptr is NULL. The reason is typically that "
                     "help_loc_entry was passed to the function "
                     "instead of loc entry to check \n");
      assert(false);
    }
    assert(loc_entry->rmw_reps.ready_to_inspect);
    assert(loc_entry->rmw_reps.inspected);
  }
}

static inline void check_after_zeroing_out_rmw_reply(loc_entry_t* loc_entry)
{
  if (ENABLE_ASSERTIONS) assert(!loc_entry->rmw_reps.ready_to_inspect);
}

static inline void check_reinstate_loc_entry_after_helping(loc_entry_t* loc_entry)
{
  check_loc_entry_is_helping(loc_entry);
}

static inline void check_after_reinstate_loc_entry_after_helping(loc_entry_t* loc_entry,
                                                                 uint16_t t_id)
{
  if (DEBUG_RMW)
    my_printf(yellow, "Wrkr %u, sess %u reinstates its RMW id %u after helping \n",
              t_id, loc_entry->sess_id, loc_entry->rmw_id.id);
  if (ENABLE_ASSERTIONS)
    assert(glob_ses_id_to_m_id((uint32_t) (loc_entry->rmw_id.id % GLOBAL_SESSION_NUM)) == (uint8_t) machine_id);

}



static inline void check_after_gathering_acc_acks(loc_entry_t* loc_entry)
{
  if (ENABLE_ASSERTIONS) {
    assert(loc_entry->state != COMMITTED);
    if (loc_entry->helping_flag == HELPING) assert(!loc_entry->all_aboard);
    assert(!loc_entry->avoid_val_in_com);
    assert(!loc_entry->avoid_val_in_com);
    assert(!loc_entry->help_loc_entry->avoid_val_in_com);
  }
}

static inline void check_that_a_nack_is_received(bool received_nack,
                                                 rmw_rep_info_t * rep_info)
{
  if (ENABLE_ASSERTIONS) {
    if (received_nack)
      assert(rep_info->rmw_id_commited > 0 || rep_info->log_too_small > 0 ||
             rep_info->already_accepted > 0 || rep_info->seen_higher_prop_acc > 0 ||
             rep_info->log_too_high > 0);
    else assert(rep_info->rmw_id_commited == 0 && rep_info->log_too_small == 0 &&
                rep_info->already_accepted == 0 && rep_info->seen_higher_prop_acc == 0 &&
                rep_info->log_too_high == 0);
  }
}


static inline void check_that_if_nack_and_helping_flag_is_helping(bool is_helping,
                                                                  bool received_a_nack,
                                                                  loc_entry_t *loc_entry)
{
  if (ENABLE_ASSERTIONS) {
    if (is_helping && received_a_nack)
      check_loc_entry_help_flag_is(loc_entry, HELPING);
  }
}
//
static inline void check_handle_all_aboard(loc_entry_t *loc_entry)
{
  if (ENABLE_ASSERTIONS) {
    assert(ENABLE_ALL_ABOARD);
    assert(loc_entry->all_aboard);
    assert(loc_entry->new_ts.version == ALL_ABOARD_TS);
  }
}

static inline void print_all_aboard_time_out(loc_entry_t *loc_entry,
                                             uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
     //my_printf(green, "Wrkr %u, Timing out on key %u \n",
     // t_id, loc_entry->key.bkt);
  }
}


static inline void check_inspect_accepts(loc_entry_t *loc_entry)
{
  if (ENABLE_ASSERTIONS) {
    check_sum_of_reps(loc_entry);
    check_loc_entry_help_flag_is_not(loc_entry, PROPOSE_LOCALLY_ACCEPTED);
    check_loc_entry_help_flag_is_not(loc_entry, PROPOSE_NOT_LOCALLY_ACKED);
    check_state_with_allowed_flags(3, loc_entry->helping_flag, NOT_HELPING, HELPING);
  }
}


static inline void check_if_accepted_cannot_be_helping(loc_entry_t *loc_entry)
{
  if (ENABLE_ASSERTIONS) {
    if (loc_entry->state == ACCEPTED)
      check_loc_entry_is_not_helping(loc_entry);
  }
}


static inline void check_bcasting_after_rmw_already_committed()
{
  if (ENABLE_ASSERTIONS) {
    assert(MACHINE_NUM > 3);
  }
}

static inline void check_when_reps_have_been_zeroes_on_prop(loc_entry_t *loc_entry)
{
  if (ENABLE_ASSERTIONS) {
    assert(loc_entry->rmw_reps.tot_replies == 1);
    assert(loc_entry->state == ACCEPTED);
  }
}

//static inline void check_()
//{
//  if (ENABLE_ASSERTIONS) {
//  }
//}
//
//static inline void check_()
//{
//  if (ENABLE_ASSERTIONS) {
//  }
//}

#endif