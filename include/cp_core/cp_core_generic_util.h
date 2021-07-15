//
// Created by vasilis on 28/06/2021.
//

#ifndef CP_CORE_GENERIC_UTIL_H
#define CP_CORE_GENERIC_UTIL_H


#include <cp_config.h>
#include "cp_main.h"
#include "cp_debug_util.h"
#include "cp_core_debug.h"
#include "od_wrkr_side_calls.h"


void take_kv_ptr_with_higher_TS(sess_stall_t *stall_info,
                                loc_entry_t *loc_entry,
                                bool from_propose,
                                uint16_t t_id);

void handle_needs_kv_ptr_state(cp_core_ctx_t *cp_core_ctx,
                               loc_entry_t *loc_entry,
                               uint16_t sess_i,
                               uint16_t t_id);

void inspect_props_if_ready_to_inspect(cp_core_ctx_t *cp_core_ctx,
                                       loc_entry_t *loc_entry);

void inspect_accepts_if_ready_to_inspect(cp_core_ctx_t *cp_core_ctx,
                                         loc_entry_t *loc_entry);

void commit_rmw(mica_op_t *kv_ptr,
                void* rmw,
                loc_entry_t *loc_entry,
                uint8_t flag,
                uint16_t t_id);

void attempt_local_accept_to_help(loc_entry_t *loc_entry,
                                  uint16_t t_id);
void attempt_local_accept(loc_entry_t *loc_entry,
                          uint16_t t_id);



static inline void lock_kv_ptr(mica_op_t *kv_ptr, uint16_t t_id)
{
  lock_seqlock(&kv_ptr->seqlock);
  check_kv_ptr_invariants(kv_ptr, t_id);
}

static inline void unlock_kv_ptr(mica_op_t *kv_ptr, uint16_t t_id)
{
  check_kv_ptr_invariants(kv_ptr, t_id);
  unlock_seqlock(&kv_ptr->seqlock);
}


/* ---------------------------------------------------------------------------
//------------------------------ GENERIC UTILITY------------------------------------------
//---------------------------------------------------------------------------*/



// After having helped another RMW, bring your own RMW back into the local entry
static inline void reinstate_loc_entry_after_helping(loc_entry_t *loc_entry, uint16_t t_id)
{
  check_loc_entry_is_helping(loc_entry);
  loc_entry->state = NEEDS_KV_PTR;
  loc_entry->helping_flag = NOT_HELPING;
  check_after_reinstate_loc_entry_after_helping(loc_entry, t_id);
}


// Perform the operation of the RMW and store the result in the local entry, call on locally accepting
static inline void perform_the_rmw_on_the_loc_entry(mica_op_t *kv_ptr,
                                                    loc_entry_t *loc_entry,
                                                    uint16_t t_id)
{
  loc_entry->rmw_is_successful = true;
  loc_entry->base_ts = kv_ptr->ts;
  loc_entry->accepted_log_no = kv_ptr->log_no;

  switch (loc_entry->opcode) {
    case RMW_PLAIN_WRITE:
      break;
    case FETCH_AND_ADD:
      memcpy(loc_entry->value_to_read, kv_ptr->value, loc_entry->rmw_val_len);
      *(uint64_t *)loc_entry->value_to_write = (*(uint64_t *)loc_entry->value_to_read) + (*(uint64_t *)loc_entry->compare_val);
      if (ENABLE_ASSERTIONS && !ENABLE_CLIENTS && RMW_RATIO >= 1000)
        assert((*(uint64_t *)loc_entry->compare_val == 1));
      //printf("%u %lu \n", loc_entry->log_no, *(uint64_t *)loc_entry->value_to_write);
      break;
    case COMPARE_AND_SWAP_WEAK:
    case COMPARE_AND_SWAP_STRONG:
      // if are equal
      loc_entry->rmw_is_successful = memcmp(loc_entry->compare_val,
                                            kv_ptr->value,
                                            loc_entry->rmw_val_len) == 0;
      if (!loc_entry->rmw_is_successful) {
        memcpy(loc_entry->value_to_read, kv_ptr->value, loc_entry->rmw_val_len);
      }
      break;
    default:
      if (ENABLE_ASSERTIONS) assert(false);
  }
  // we need to remember the last accepted value
  if (loc_entry->rmw_is_successful) {
    write_kv_ptr_acc_val(kv_ptr, loc_entry->value_to_write, (size_t) RMW_VALUE_SIZE);
  }
  else {
    write_kv_ptr_acc_val(kv_ptr, loc_entry->value_to_read, (size_t) RMW_VALUE_SIZE);
  }

}


// free a session held by an RMW
static inline void free_session_from_rmw(loc_entry_t *loc_entry,
                                         sess_stall_t *stall_info,
                                         bool allow_paxos_log,
                                         uint16_t t_id)
{
  check_free_session_from_rmw(loc_entry, stall_info, t_id);
  fill_req_array_when_after_rmw(loc_entry->sess_id, loc_entry->index_to_req_array, loc_entry->opcode,
                                loc_entry->value_to_read, loc_entry->rmw_is_successful, t_id);
  if (VERIFY_PAXOS && allow_paxos_log) verify_paxos(loc_entry, t_id);
  // my_printf(cyan, "Session %u completing \n", loc_entry->glob_sess_id);
  signal_completion_to_client(loc_entry->sess_id, loc_entry->index_to_req_array, t_id);
  stall_info->stalled[loc_entry->sess_id] = false;
  stall_info->all_stalled = false;
}


static inline void local_rmw_ack(loc_entry_t *loc_entry)
{
  loc_entry->rmw_reps.tot_replies = 1;
  loc_entry->rmw_reps.acks = 1;
}


/*--------------------------------------------------------------------------
 * --------------------CAS EARLY FAILURE--------------------------
 * --------------------------------------------------------------------------*/

// Returns true if the CAS has to be cut short
static inline bool rmw_compare_fails(uint8_t opcode, uint8_t *compare_val,
                                     uint8_t *kv_ptr_value, uint32_t val_len, uint16_t t_id)
{
  if (!opcode_is_compare_rmw(opcode) || (!ENABLE_CAS_CANCELLING)) return false; // there is nothing to fail
  if (ENABLE_ASSERTIONS) {
    assert(compare_val != NULL);
    assert(kv_ptr_value != NULL);
  }
  // memcmp() returns 0 if regions are equal. Thus the CAS fails if the result is not zero
  bool rmw_fails = memcmp(compare_val, kv_ptr_value, val_len) != 0;
  if (ENABLE_STAT_COUNTING && rmw_fails) {
    t_stats[t_id].cancelled_rmws++;
  }
  return rmw_fails;

}

// returns true if the RMW can be failed before allocating a local entry
static inline bool does_rmw_fail_early(trace_op_t *op, mica_op_t *kv_ptr,
                                       uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) assert(op->real_val_len <= RMW_VALUE_SIZE);
  if (op->opcode == COMPARE_AND_SWAP_WEAK &&
      rmw_compare_fails(op->opcode, op->value_to_read,
                        kv_ptr->value, op->real_val_len, t_id)) {
    //my_printf(red, "CAS fails returns val %u/%u \n", kv_ptr->value[RMW_BYTE_OFFSET], op->value_to_read[0]);

    fill_req_array_on_rmw_early_fail(op->session_id, kv_ptr->value,
                                     op->index_to_req_array, t_id);
    return true;
  }
  else return  false;
}

//
static inline bool rmw_fails_with_loc_entry(loc_entry_t *loc_entry, mica_op_t *kv_ptr,
                                            bool *rmw_fails, uint16_t t_id)
{
  if (ENABLE_CAS_CANCELLING) {
    if (loc_entry->killable) {
      if (rmw_compare_fails(loc_entry->opcode, loc_entry->compare_val,
                            kv_ptr->value, loc_entry->rmw_val_len, t_id)) {
        (*rmw_fails) = true;
        if (ENABLE_ASSERTIONS) {
          assert(!loc_entry->rmw_is_successful);
          assert(loc_entry->rmw_val_len <= RMW_VALUE_SIZE);
          assert(loc_entry->helping_flag != HELPING);
        }
        memcpy(loc_entry->value_to_read, kv_ptr->value,
               loc_entry->rmw_val_len);
        return true;
      }
    }
  }
  return false;
}

/*--------------------------------------------------------------------------
 * --------------------BACK-OFF UTILITY-------------------------------------
 * --------------------------------------------------------------------------*/

static inline void save_the_info_of_the_kv_ptr_owner(mica_op_t *kv_ptr,
                                                     loc_entry_t *loc_entry)
{
  loc_entry->help_rmw->state = kv_ptr->state;
  assign_second_rmw_id_to_first(&loc_entry->help_rmw->rmw_id, &kv_ptr->rmw_id);
  loc_entry->help_rmw->ts = kv_ptr->prop_ts;
  loc_entry->help_rmw->log_no = kv_ptr->log_no;
}



static inline bool if_already_committed_bcast_commits(loc_entry_t *loc_entry,
                                                      uint16_t t_id)
{
  check_loc_entry_if_already_committed(loc_entry);
  if (loc_entry->rmw_id.id <= committed_glob_sess_rmw_id[loc_entry->glob_sess_id]) {
    //my_printf(yellow, "Wrkr %u, sess: %u Bcast rmws %u \n", t_id, loc_entry->sess_id);
    loc_entry->log_no = loc_entry->accepted_log_no;
    loc_entry->state = MUST_BCAST_COMMITS;
    return true;
  }
  return false;
}

/*--------------------------------------------------------------------------
 * -------------------SENDING MESSAGES-----------------------------------
 * --------------------------------------------------------------------------*/

//fill the reply entry with last_committed RMW-id, TS, value and log number
static inline void fill_reply_entry_with_committed_RMW (mica_op_t *kv_ptr,
                                                        struct rmw_rep_last_committed *rep,
                                                        uint16_t t_id)
{
  rep->ts.m_id = kv_ptr->ts.m_id; // Here we reply with the base TS
  rep->ts.version = kv_ptr->ts.version;
  memcpy(rep->value, kv_ptr->value, (size_t) RMW_VALUE_SIZE);
  rep->log_no_or_base_version = kv_ptr->last_committed_log_no;
  rep->rmw_id = kv_ptr->last_committed_rmw_id.id;
  //if (rep->base_ts.version == 0)
  //  my_printf(yellow, "Wrkr %u replies with flag %u Log_no %u, rmw_id %lu glob_sess id %u\n",
  //         t_id, rep->opcode, rep->log_no, rep->rmw_id, rep->glob_sess_id);
}

// If a local RMW managed to grab a kv_ptr, then it sets up its local entry
static inline void fill_loc_rmw_entry_on_grabbing_kv_ptr(loc_entry_t *loc_entry,
                                                         uint32_t version, uint8_t state,
                                                         uint16_t sess_i, uint16_t t_id)
{
  check_when_filling_loc_entry(loc_entry, sess_i, version, t_id);
  local_rmw_ack(loc_entry);
  loc_entry->state = state;
  loc_entry->new_ts.version = version;
  loc_entry->new_ts.m_id = (uint8_t) machine_id;
}



// Activate the entry that belongs to a given key to initiate an RMW (either a local or a remote)
static inline void activate_kv_pair(uint8_t state, uint32_t new_version, mica_op_t *kv_ptr,
                                    uint8_t opcode, uint8_t new_ts_m_id, loc_entry_t *loc_entry,
                                    uint64_t rmw_id,
                                    uint32_t log_no, uint16_t t_id, const char *message)
{
  check_activate_kv_pair(state, kv_ptr, log_no, message);

  kv_ptr->opcode = opcode;
  kv_ptr->prop_ts.m_id = new_ts_m_id;
  kv_ptr->prop_ts.version = new_version;
  kv_ptr->rmw_id.id = rmw_id;
  kv_ptr->state = state;
  kv_ptr->log_no = log_no;

  if (state == ACCEPTED) {
    check_activate_kv_pair_accepted(kv_ptr, new_version, new_ts_m_id);
    kv_ptr->accepted_ts = kv_ptr->prop_ts;
    kv_ptr->accepted_log_no = log_no;
    if (loc_entry != NULL && loc_entry->all_aboard) {
      perform_the_rmw_on_the_loc_entry(kv_ptr, loc_entry, t_id);
    }
  }
  check_after_activate_kv_pair(kv_ptr, message, state, t_id);
}

// When a propose/accept has inspected the responses (after they have reached at least a quorum),
// advance the entry's l_id such that previous responses are disregarded
static inline void advance_loc_entry_l_id(loc_entry_t *loc_entry,
                                          uint16_t t_id)
{
  loc_entry->l_id += SESSIONS_PER_THREAD;
  loc_entry->help_loc_entry->l_id = loc_entry->l_id;
  if (ENABLE_ASSERTIONS) assert(loc_entry->l_id % SESSIONS_PER_THREAD == loc_entry->sess_id);
}

 // Initialize a local  RMW entry on the first time it gets allocated
static inline void init_loc_entry(trace_op_t *op,
                                  uint16_t t_id,
                                  loc_entry_t* loc_entry)
{
  check_when_init_loc_entry(loc_entry, op);
  loc_entry->opcode = op->opcode;
  if (opcode_is_compare_rmw(op->opcode) || op->opcode == RMW_PLAIN_WRITE)
    memcpy(loc_entry->value_to_write, op->value_to_write, op->real_val_len);
  loc_entry->killable = op->opcode == COMPARE_AND_SWAP_WEAK;
  if (opcode_is_compare_rmw(op->opcode))
    loc_entry->compare_val = op->value_to_read; //expected value
  else if (op->opcode == FETCH_AND_ADD) {
    loc_entry->compare_val = op->value_to_write; // value to be added
  }
  loc_entry->fp_detected = false;
  loc_entry->rmw_val_len = op->real_val_len;
  loc_entry->rmw_is_successful = false;
  loc_entry->all_aboard = ENABLE_ALL_ABOARD && op->attempt_all_aboard;
  loc_entry->avoid_val_in_com = false;
  loc_entry->base_ts_found = false;
  loc_entry->all_aboard_time_out = 0;
  memcpy(&loc_entry->key, &op->key, KEY_SIZE);
  memset(&loc_entry->rmw_reps, 0, sizeof(struct rmw_rep_info));
  loc_entry->index_to_req_array = op->index_to_req_array;

  loc_entry->back_off_cntr = 0;
  loc_entry->log_too_high_cntr = 0;
  loc_entry->helping_flag = NOT_HELPING;
  loc_entry->rmw_id.id+= GLOBAL_SESSION_NUM;
  advance_loc_entry_l_id(loc_entry, t_id);
  loc_entry->accepted_log_no = 0;
  loc_entry->help_loc_entry->state = INVALID_RMW;
  check_loc_entry_init_rmw_id(loc_entry, t_id);
}

// When time-out-ing on a stuck Accepted value, and try to help it, you need to first propose your own
static inline void set_up_a_proposed_but_not_locally_acked_entry(sess_stall_t *stall_info,
                                                                 mica_op_t  *kv_ptr,
                                                                 loc_entry_t *loc_entry,
                                                                 bool helping_myself,
                                                                 uint16_t t_id)
{
  checks_and_prints_proposed_but_not_locally_acked(stall_info, kv_ptr, loc_entry, t_id);
  loc_entry_t *help_loc_entry = loc_entry->help_loc_entry;
  loc_entry->state = PROPOSED;
  help_loc_entry->state = ACCEPTED;
  if (helping_myself) {
    loc_entry->helping_flag = PROPOSE_LOCALLY_ACCEPTED;
  }
  else {
    loc_entry->helping_flag = PROPOSE_NOT_LOCALLY_ACKED;
    help_loc_entry->log_no = loc_entry->log_no;
    help_loc_entry->key = loc_entry->key;
  }
  loc_entry->rmw_reps.tot_replies = 1;
  loc_entry->rmw_reps.already_accepted = 1;
  logging_proposed_but_not_locally_acked(kv_ptr, loc_entry, help_loc_entry, t_id);
}

#endif //CP_CORE_GENERIC_UTIL_H
