//
// Created by vasilis on 28/06/2021.
//

#ifndef CP_CORE_UTIL_H
#define CP_CORE_UTIL_H

#include <cp_core_generic_util.h>


/*--------------------------------------------------------------------------
 * --------------------RMW-INIT---------------------------------------------
 * --------------------------------------------------------------------------*/

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

// Initialize a local  RMW entry on the first time it gets allocated
static inline void init_loc_entry(trace_op_t *op,
                                  uint16_t t_id, loc_entry_t* loc_entry)
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


// Activate the entry that belongs to a given key to initiate an RMW (either a local or a remote)
static inline void activate_kv_pair(uint8_t state, uint32_t new_version, mica_op_t *kv_ptr,
                                    uint8_t opcode, uint8_t new_ts_m_id, loc_entry_t *loc_entry,
                                    uint64_t rmw_id,
                                    uint32_t log_no, uint16_t t_id, const char *message)
{
  check_activate_kv_pair(state, kv_ptr, log_no, message);
  // pass the new base_ts!
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




void create_prop_rep(cp_prop_t *prop,
                     cp_prop_mes_t *prop_mes,
                     cp_rmw_rep_t *prop_rep,
                     mica_op_t *kv_ptr,
                     uint16_t t_id);


void create_acc_rep(cp_acc_t *acc,
                    cp_acc_mes_t *acc_mes,
                    cp_rmw_rep_t *acc_rep,
                    mica_op_t *kv_ptr,
                    uint16_t t_id);

/*--------------------------------------------------------------------------
 * ---------------------------------------------------------
 * --------------------------------------------------------------------------*/


static inline void take_kv_ptr_to_acc_state(mica_op_t *kv_ptr,
                                            loc_entry_t *loc_entry,
                                            uint16_t t_id)
{
  checks_before_local_accept(kv_ptr, loc_entry, t_id);

  kv_ptr->state = ACCEPTED;
  // calculate the new value depending on the type of RMW
  perform_the_rmw_on_the_loc_entry(kv_ptr, loc_entry, t_id);
  //when last_accepted_value is update also update the acc_base_ts
  kv_ptr->base_acc_ts = kv_ptr->ts;
  kv_ptr->accepted_ts = loc_entry->new_ts;
  kv_ptr->accepted_log_no = kv_ptr->log_no;
  checks_after_local_accept(kv_ptr, loc_entry, t_id);
}

// After gathering a quorum of proposal acks, check if you can accept locally-- THIS IS STRICTLY LOCAL RMWS -- no helps
// Every RMW that gets committed must pass through this function successfully (at least one time)
static inline void attempt_local_accept(loc_entry_t *loc_entry,
                                        uint16_t t_id)
{
  mica_op_t *kv_ptr = loc_entry->kv_ptr;
  checks_preliminary_local_accept(kv_ptr, loc_entry, t_id);

  lock_kv_ptr(loc_entry->kv_ptr, t_id);

  if (if_already_committed_bcast_commits(loc_entry, t_id)) {
    unlock_kv_ptr(loc_entry->kv_ptr, t_id);
    return;
  }

  if (same_rmw_id_same_ts_and_invalid(kv_ptr, loc_entry)) {
    take_kv_ptr_to_acc_state(kv_ptr, loc_entry, t_id);
    unlock_kv_ptr(loc_entry->kv_ptr, t_id);
    loc_entry->state = ACCEPTED;
  }
  else { // the entry stores a different rmw_id and thus our proposal has been won by another
    // Some other RMW has won the kv_ptr we are trying to get accepted
    checks_after_failure_to_locally_accept(kv_ptr, loc_entry, t_id);
    unlock_kv_ptr(loc_entry->kv_ptr, t_id);
    loc_entry->state = NEEDS_KV_PTR;
  }
}

static inline void take_local_kv_ptr_to_acc_state_when_helping(mica_op_t *kv_ptr,
                                                               loc_entry_t *loc_entry,
                                                               loc_entry_t* help_loc_entry,
                                                               uint16_t t_id)
{
  kv_ptr->state = ACCEPTED;
  kv_ptr->rmw_id = help_loc_entry->rmw_id;
  kv_ptr->accepted_ts = help_loc_entry->new_ts;
  kv_ptr->accepted_log_no = kv_ptr->log_no;
  write_kv_ptr_acc_val(kv_ptr, help_loc_entry->value_to_write, (size_t) RMW_VALUE_SIZE);
  kv_ptr->base_acc_ts = help_loc_entry->base_ts;// the base_ts of the RMW we are helping
  checks_after_local_accept_help(kv_ptr, loc_entry, t_id);
  unlock_kv_ptr(loc_entry->kv_ptr, t_id);
  loc_entry->state = ACCEPTED;
}

static inline void clean_up_if_cannot_accept_locally(mica_op_t *kv_ptr,
                                                     loc_entry_t *loc_entry,
                                                     uint16_t t_id)
{
  checks_after_failure_to_locally_accept_help(kv_ptr, loc_entry, t_id);
  unlock_kv_ptr(loc_entry->kv_ptr, t_id);
  loc_entry->state = NEEDS_KV_PTR;
  loc_entry->help_loc_entry->state = INVALID_RMW;
}


// After gathering a quorum of proposal reps, one of them was a lower TS accept, try and help it
static inline void attempt_local_accept_to_help(loc_entry_t *loc_entry,
                                                uint16_t t_id)
{
  mica_op_t *kv_ptr = loc_entry->kv_ptr;
  loc_entry_t* help_loc_entry = loc_entry->help_loc_entry;

  help_loc_entry->new_ts = loc_entry->new_ts;
  checks_preliminary_local_accept_help(kv_ptr, loc_entry, help_loc_entry);

  lock_kv_ptr(loc_entry->kv_ptr, t_id);
  comment_on_why_we_dont_check_if_rmw_committed();

  bool can_accept_locally = find_out_if_can_accept_help_locally(kv_ptr, loc_entry,
                                                                help_loc_entry, t_id);
  if (can_accept_locally)
    take_local_kv_ptr_to_acc_state_when_helping(kv_ptr, loc_entry,
                                                help_loc_entry, t_id);
  else clean_up_if_cannot_accept_locally(kv_ptr, loc_entry, t_id);

}


/*--------------------------------------------------------------------------
 * --------------------COMMITING-------------------------------------
 * --------------------------------------------------------------------------*/

void commit_rmw(mica_op_t *kv_ptr,
                void* rmw,
                loc_entry_t *loc_entry,
                uint8_t flag,
                uint16_t t_id);


// On gathering quorum of acks for commit, commit locally and
// signal that the session must be freed if not helping
void act_on_quorum_of_commit_acks(cp_core_ctx_t *cp_core_ctx,
                                  uint16_t sess_id);



/*--------------------------------------------------------------------------
 * --------------------HANDLE REPLIES-------------------------------------
 * --------------------------------------------------------------------------*/


// Handle read replies that refer to RMWs (either replies to accepts or proposes)
void handle_rmw_rep_replies(cp_core_ctx_t *cp_core_ctx,
                            cp_rmw_rep_mes_t *r_rep_mes,
                            bool is_accept);




/*--------------------------------------------------------------------------
 * -----------------------------RMW-FSM-------------------------------------
 * --------------------------------------------------------------------------*/
void inspect_props_if_ready_to_inspect(cp_core_ctx_t *cp_core_ctx,
                                       loc_entry_t *loc_entry);

void inspect_accepts_if_ready_to_inspect(cp_core_ctx_t *cp_core_ctx,
                                         loc_entry_t *loc_entry);


//------------------------------HELP STUCK RMW------------------------------------------
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



// local_entry->state = NEEDS_KV_PTR
void handle_needs_kv_ptr_state(cp_core_ctx_t *cp_core_ctx,
                               loc_entry_t *loc_entry,
                               uint16_t sess_i,
                               uint16_t t_id);



//------------------------------CLEAN-UP------------------------------------------

typedef struct retry_flags{
  bool from_propose;
  bool kv_ptr_was_grabbed ;
  bool rmw_fails;
  bool help_locally_acced;
  bool is_still_accepted;
} retry_flags_t;


static inline void clean_up_after_retrying(sess_stall_t *stall_info,
                                           mica_op_t *kv_ptr,
                                           loc_entry_t *loc_entry,
                                           retry_flags_t flags,
                                           uint16_t t_id)
{
  if (flags.kv_ptr_was_grabbed) {
    print_clean_up_after_retrying(kv_ptr, loc_entry, t_id);
    loc_entry->state = PROPOSED;
    if (flags.help_locally_acced)
      set_up_a_proposed_but_not_locally_acked_entry(stall_info, kv_ptr,
                                                    loc_entry, true, t_id);
    else local_rmw_ack(loc_entry);
  }
  else if (flags.rmw_fails) {
    check_clean_up_after_retrying(kv_ptr, loc_entry,
                                  flags.help_locally_acced, t_id);
    loc_entry->state = INVALID_RMW;
    free_session_from_rmw(loc_entry, stall_info,
                          false, t_id);
  }
  else loc_entry->state = NEEDS_KV_PTR;
}



static inline bool can_kv_ptr_be_taken_with_higher_TS(mica_op_t *kv_ptr,
                                                      loc_entry_t *loc_entry,
                                                      retry_flags_t *flags)
{
  bool is_still_proposed = rmw_ids_are_equal(&kv_ptr->rmw_id, &loc_entry->rmw_id) &&
                           kv_ptr->state == PROPOSED;

  flags->is_still_accepted = rmw_ids_are_equal(&kv_ptr->rmw_id, &loc_entry->rmw_id) &&
                             kv_ptr->state == ACCEPTED &&
                             compare_ts(&kv_ptr->accepted_ts, &loc_entry->new_ts) == EQUAL;

  return kv_ptr->state == INVALID_RMW || is_still_proposed || flags->is_still_accepted;
}


static inline void update_loc_entry_when_taking_kv_ptr_with_higher_TS(mica_op_t *kv_ptr,
                                                                      loc_entry_t *loc_entry)
{
  loc_entry->log_no = kv_ptr->last_committed_log_no + 1;
  loc_entry->new_ts.version = MAX(loc_entry->new_ts.version, kv_ptr->prop_ts.version) + 1;
  loc_entry->base_ts = kv_ptr->ts; // Minimize the possibility for RMW_ACK_BASE_TS_STALE
  loc_entry->new_ts.m_id = (uint8_t) machine_id;
}

static inline void update_kv_ptr_when_taking_kv_ptr_with_higher_TS(mica_op_t *kv_ptr,
                                                                   loc_entry_t *loc_entry)
{
  if (kv_ptr->state == INVALID_RMW) {
    kv_ptr->log_no = kv_ptr->last_committed_log_no + 1;
    kv_ptr->opcode = loc_entry->opcode;
    assign_second_rmw_id_to_first(&kv_ptr->rmw_id, &loc_entry->rmw_id);
  }
  kv_ptr->prop_ts = loc_entry->new_ts;
}

static inline void if_accepted_help_else_steal(mica_op_t *kv_ptr,
                                               loc_entry_t *loc_entry,
                                               retry_flags_t *flags)
{
  if (!flags->is_still_accepted) {
    check_kv_ptr_state_is_not_acced(kv_ptr);
    kv_ptr->state = PROPOSED;
  }
  else {
    flags->help_locally_acced = true;
    loc_entry->help_loc_entry->new_ts = kv_ptr->accepted_ts;
  }
}

static inline void rmw_fails_or_steals_kv_ptr_or_helps_kv_ptr(mica_op_t *kv_ptr,
                                                              loc_entry_t *loc_entry,
                                                              retry_flags_t *flags,
                                                              uint16_t t_id)
{
  check_when_retrying_with_higher_TS(kv_ptr, loc_entry, flags->from_propose);
  if (rmw_fails_with_loc_entry(loc_entry, kv_ptr, &flags->rmw_fails, t_id)) {
    check_kv_ptr_state_is_not_acced(kv_ptr);
    kv_ptr->state = INVALID_RMW;
  }
  else {
    update_loc_entry_when_taking_kv_ptr_with_higher_TS(kv_ptr, loc_entry);
    update_kv_ptr_when_taking_kv_ptr_with_higher_TS(kv_ptr, loc_entry);
    if_accepted_help_else_steal(kv_ptr, loc_entry, flags);
    flags->kv_ptr_was_grabbed = true;
  }
}

// local_entry->state == RETRY_WITH_BIGGER_TS
static inline void take_kv_ptr_with_higher_TS(sess_stall_t *stall_info,
                                              loc_entry_t *loc_entry,
                                              bool from_propose,
                                              uint16_t t_id)
{
  retry_flags_t flags;
  memset(&flags, 0, sizeof(retry_flags_t));
  flags.from_propose = from_propose;

  mica_op_t *kv_ptr = loc_entry->kv_ptr;
  lock_kv_ptr(kv_ptr, t_id);
  {
    if (if_already_committed_bcast_commits(loc_entry, t_id)) {
      unlock_kv_ptr(loc_entry->kv_ptr, t_id);
      return;
    }
    if (can_kv_ptr_be_taken_with_higher_TS(kv_ptr, loc_entry, &flags)) {
      rmw_fails_or_steals_kv_ptr_or_helps_kv_ptr(kv_ptr, loc_entry, &flags, t_id);
    }
    else print_when_retrying_fails(kv_ptr, loc_entry, t_id);
  }
  unlock_kv_ptr(loc_entry->kv_ptr, t_id);
  clean_up_after_retrying(stall_info, kv_ptr, loc_entry, flags, t_id);
}


// Worker inspects its local RMW entries
void cp_core_inspect_rmws(cp_core_ctx_t *cp_core_ctx);
/*--------------------------------------------------------------------------
 * --------------------INIT RMW-------------------------------------
 * --------------------------------------------------------------------------*/


static inline void loc_entry_was_successful_first_time(loc_entry_t *loc_entry,
                                                       cp_core_ctx_t *cp_core_ctx,
                                                       trace_op_t *op,
                                                       uint16_t t_id)
{
  fill_loc_rmw_entry_on_grabbing_kv_ptr(loc_entry, op->ts.version,
                                        loc_entry->state, op->session_id, t_id);

  bool doing_all_aboard = loc_entry->state == ACCEPTED;
  check_op_version(op, doing_all_aboard);

  if (doing_all_aboard) {
    loc_entry->accepted_log_no = loc_entry->log_no;
    cp_acc_insert(cp_core_ctx->netw_ctx, loc_entry, false);
    loc_entry->killable = false;
    loc_entry->all_aboard = true;
    loc_entry->all_aboard_time_out = 0;
  }
  else
    cp_prop_insert(cp_core_ctx->netw_ctx, loc_entry);
}

static inline void handle_loc_entry_cas_failed_first_time(loc_entry_t *loc_entry,
                                                          cp_core_ctx_t *cp_core_ctx,
                                                          trace_op_t *op,
                                                          uint16_t t_id)
{
 if (ENABLE_CAS_CANCELLING) {
   if (loc_entry->state == CAS_FAILED) {
     signal_completion_to_client(op->session_id, op->index_to_req_array, t_id);
     cp_core_ctx->stall_info->stalled[op->session_id] = false;
     cp_core_ctx->stall_info->all_stalled = false;
     loc_entry->state = INVALID_RMW;
   }
 }
 else my_assert(false, "Wrong loc_entry in RMW");
}

// Insert an RMW in the local RMW structs
static inline void insert_rmw(cp_core_ctx_t *cp_core_ctx,
                              trace_op_t *op,
                              uint16_t t_id)
{
  uint16_t session_id = op->session_id;
  check_session_id(session_id);
  loc_entry_t *loc_entry = &cp_core_ctx->rmw_entries[session_id];
  uint8_t success_state = (uint8_t) (ENABLE_ALL_ABOARD && op->attempt_all_aboard ? ACCEPTED : PROPOSED);
  bool kv_ptr_taken = loc_entry->state == success_state;

  if (kv_ptr_taken)  loc_entry_was_successful_first_time(loc_entry, cp_core_ctx, op, t_id);
  else if (loc_entry->state == NEEDS_KV_PTR) {
    if (ENABLE_ALL_ABOARD) loc_entry->all_aboard = false;
  }
  else handle_loc_entry_cas_failed_first_time(loc_entry, cp_core_ctx, op, t_id);
}





#endif //CP_CORE_UTIL_H
