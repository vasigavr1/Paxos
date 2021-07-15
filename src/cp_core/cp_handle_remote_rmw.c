//
// Created by vasilis on 06/07/2021.
//

#include <cp_core_interface.h>

// Check the global RMW-id structure, to see if an RMW has already been committed
static inline bool the_rmw_has_committed(mica_op_t *kv_ptr,
                                         uint64_t rmw_id,
                                         uint32_t log_no, uint16_t t_id,
                                         struct rmw_rep_last_committed *rep)
{
  uint64_t glob_sess_id = rmw_id % GLOBAL_SESSION_NUM;
  check_the_rmw_has_committed(glob_sess_id);

  if (committed_glob_sess_rmw_id[glob_sess_id] >= rmw_id) {
    bool same_log = kv_ptr->last_committed_log_no == log_no;
    rep->opcode = (uint8_t) (same_log ? RMW_ID_COMMITTED_SAME_LOG : RMW_ID_COMMITTED);
    check_when_rmw_has_committed(kv_ptr, rep, glob_sess_id, log_no, rmw_id, t_id);
    return true;
  }
  else return false;
}

static inline void log_no_too_low(uint32_t log_no, mica_op_t *kv_ptr,
                                  uint64_t rmw_l_id,
                                  uint16_t t_id,
                                  cp_rmw_rep_t *rep)
{
  print_log_too_small(log_no, kv_ptr, rmw_l_id, t_id);
  rep->opcode = LOG_TOO_SMALL;
  fill_reply_entry_with_committed_RMW (kv_ptr, rep, t_id);
}

static inline void log_no_too_high(uint32_t log_no, mica_op_t *kv_ptr,
                                   uint16_t t_id,
                                   cp_rmw_rep_t *rep)
{
  print_is_log_too_high(log_no, kv_ptr, t_id);
  rep->opcode = LOG_TOO_HIGH;
}

static inline bool is_log_too_low_or_too_high(uint32_t log_no, mica_op_t *kv_ptr,
                                              uint64_t rmw_l_id,
                                              uint16_t t_id,
                                              cp_rmw_rep_t *rep)
{
  uint32_t expected_log_no = kv_ptr->last_committed_log_no + 1;
  if (log_no == expected_log_no)
    return false;
  else if (log_no < expected_log_no)
    log_no_too_low(log_no, kv_ptr, rmw_l_id, t_id, rep);
  else
    log_no_too_high(log_no, kv_ptr, t_id, rep);

  return true;
}


static inline bool is_log_lower_higher_or_has_rmw_committed(uint32_t log_no, mica_op_t *kv_ptr,
                                                            uint64_t rmw_l_id,
                                                            uint16_t t_id,
                                                            cp_rmw_rep_t *rep)
{
  check_log_nos_of_kv_ptr(kv_ptr, "is_log_smaller_or_has_rmw_committed", t_id);

  if (the_rmw_has_committed(kv_ptr, rmw_l_id, log_no, t_id, rep))
    return true;
  return is_log_too_low_or_too_high(log_no, kv_ptr, rmw_l_id, t_id, rep);
}


static inline uint8_t is_base_ts_too_small(mica_op_t *kv_ptr,
                                           struct propose *prop,
                                           struct rmw_rep_last_committed *rep,
                                           uint16_t t_id)
{
  if (prop->base_ts.version == DO_NOT_CHECK_BASE_TS) return RMW_ACK;

  compare_t  comp_ts = compare_ts(&kv_ptr->ts, &prop->base_ts);
  if (comp_ts == GREATER) {
    rep->ts.version = kv_ptr->ts.version;
    rep->ts.m_id = kv_ptr->ts.m_id;
    memcpy(rep->value, kv_ptr->value, (size_t) RMW_VALUE_SIZE);
    return RMW_ACK_BASE_TS_STALE;
  }
  else return RMW_ACK;
}



static inline uint8_t tell_prop_to_help_lower_acc(cp_prop_t *prop,
                                                  mica_op_t *kv_ptr,
                                                  cp_rmw_rep_t *rep)
{
  assign_ts_to_netw_ts(&rep->ts, &kv_ptr->accepted_ts);
  rep->rmw_id = kv_ptr->rmw_id.id;
  memcpy(rep->value, kv_ptr->last_accepted_value, (size_t) RMW_VALUE_SIZE);
  rep->log_no_or_base_version = kv_ptr->base_acc_ts.version;
  rep->base_m_id = kv_ptr->base_acc_ts.m_id;
  return  SEEN_LOWER_ACC;
}


static inline uint8_t compare_kv_ptr_acc_ts_with_prop_ts(cp_prop_t *prop,
                                                         mica_op_t *kv_ptr,
                                                         cp_rmw_rep_t *rep)
{
  compare_t acc_ts_comp = compare_netw_ts_with_ts(&prop->ts, &kv_ptr->accepted_ts);
  if (kv_ptr->state == ACCEPTED && acc_ts_comp == GREATER) {
    return kv_ptr->rmw_id.id == prop->t_rmw_id ?
           RMW_ACK_ACC_SAME_RMW :
           tell_prop_to_help_lower_acc(prop, kv_ptr, rep);
  }
  else  return RMW_ACK;
}

// Look at the kv_ptr to answer to a propose message-- kv pair lock is held when calling this
static inline uint8_t propose_snoops_entry(cp_prop_t *prop,
                                           mica_op_t *kv_ptr,
                                           uint8_t m_id,
                                           uint16_t t_id,
                                           cp_rmw_rep_t *rep)
{
  check_propose_snoops_entry(prop, kv_ptr);
  uint8_t return_flag;
  compare_t prop_ts_comp = compare_netw_ts_with_ts(&prop->ts, &kv_ptr->prop_ts);

  if (prop_ts_comp == GREATER) {
    assign_netw_ts_to_ts(&kv_ptr->prop_ts, &prop->ts);
    return_flag = compare_kv_ptr_acc_ts_with_prop_ts(prop, kv_ptr, rep);
  }
  else {
    assign_ts_to_netw_ts(&rep->ts, &kv_ptr->prop_ts);
    return_flag = SEEN_HIGHER_PROP;
  }

  check_state_with_allowed_flags(5, return_flag, RMW_ACK, RMW_ACK_ACC_SAME_RMW,
                                 SEEN_HIGHER_PROP, SEEN_LOWER_ACC);
  return return_flag;
}

static inline uint8_t ack_acc_if_ts_equal_greater(cp_acc_t *acc,
                                                  mica_op_t *kv_ptr,
                                                  compare_t ts_comp,
                                                  uint16_t t_id)
{
  print_accept_snoops_entry(acc, kv_ptr, ts_comp, t_id);
  return RMW_ACK;
}

static inline uint8_t nack_acc_if_ts_too_low(mica_op_t *kv_ptr,
                                             cp_rmw_rep_t *rep){
  check_state_with_allowed_flags(3, kv_ptr->state,
                                 PROPOSED, ACCEPTED);
  assign_ts_to_netw_ts(&rep->ts, &kv_ptr->prop_ts);
  return kv_ptr->state == PROPOSED ?
         SEEN_HIGHER_PROP : SEEN_HIGHER_ACC;

}

static uint8_t acc_inspects_kv_ts(cp_acc_t *acc,
                                  mica_op_t *kv_ptr,
                                  cp_rmw_rep_t *rep,
                                  uint16_t t_id)
{
  // Higher Ts  = Success,  Lower Ts  = Failure
  compare_t ts_comp = compare_netw_ts_with_ts(&acc->ts, &kv_ptr->prop_ts);
  // Higher Ts  = Success
  if (ts_comp == EQUAL || ts_comp == GREATER)
    return ack_acc_if_ts_equal_greater(acc, kv_ptr, ts_comp, t_id);
  else if (ts_comp == SMALLER)
    return nack_acc_if_ts_too_low(kv_ptr, rep);
  else my_assert(false, "");
}

// Look at an RMW entry to answer to an accept message-- kv pair lock is held when calling this
static inline uint8_t accept_snoops_entry(cp_acc_t *acc, mica_op_t *kv_ptr, uint8_t sender_m_id,
                                          uint16_t t_id, cp_rmw_rep_t *rep)
{
  check_accept_snoops_entry(acc, kv_ptr);
  uint8_t return_flag =  kv_ptr->state == INVALID_RMW ?
                         RMW_ACK : acc_inspects_kv_ts(acc, kv_ptr, rep, t_id);

  print_check_after_accept_snoops_entry(acc, kv_ptr, rep, return_flag, t_id);
  return return_flag;
}

//Handle a remote propose/accept whose log number is big enough
static inline uint8_t handle_remote_prop_or_acc_in_kvs(mica_op_t *kv_ptr, void *prop_or_acc,
                                                       uint8_t sender_m_id, uint16_t t_id,
                                                       struct rmw_rep_last_committed *rep, uint32_t log_no,
                                                       bool is_prop)
{
  // if the log number is higher than expected blindly ack
  if (log_no > kv_ptr->log_no) {
    check_log_no_on_ack_remote_prop_acc(kv_ptr, log_no);
    return RMW_ACK;
  }
  else
    return is_prop ? propose_snoops_entry((cp_prop_t *) prop_or_acc, kv_ptr, sender_m_id, t_id, rep) :
           accept_snoops_entry((cp_acc_t *) prop_or_acc, kv_ptr, sender_m_id, t_id, rep);
}

static inline void bookkeeping_if_creating_prop_ack(cp_prop_t *prop,
                                                    cp_rmw_rep_t *prop_rep,
                                                    mica_op_t *kv_ptr,
                                                    uint16_t t_id)
{
  // if the propose is going to be acked record its information in the kv_ptr
  if (prop_rep->opcode == RMW_ACK) {
    if (ENABLE_ASSERTIONS) assert(prop->log_no >= kv_ptr->log_no);
    activate_kv_pair(PROPOSED, prop->ts.version, kv_ptr, prop->opcode,
                     prop->ts.m_id, NULL, prop->t_rmw_id, prop->log_no, t_id,
                     ENABLE_ASSERTIONS ? "received propose" : NULL);
  }
  if (prop_rep->opcode == RMW_ACK || prop_rep->opcode == RMW_ACK_ACC_SAME_RMW) {
    prop_rep->opcode = is_base_ts_too_small(kv_ptr, prop, prop_rep, t_id);
  }
  check_create_prop_rep(prop, kv_ptr);
}

static inline void create_prop_rep_after_locking_kv_ptr(cp_prop_t *prop,
                                                        cp_prop_mes_t *prop_mes,
                                                        cp_rmw_rep_t *prop_rep,
                                                        mica_op_t *kv_ptr,
                                                        uint64_t *number_of_reqs,
                                                        uint16_t t_id)
{
  if (!is_log_lower_higher_or_has_rmw_committed(prop->log_no, kv_ptr,
                                                prop->t_rmw_id,
                                                t_id, prop_rep)) {
    prop_rep->opcode = handle_remote_prop_or_acc_in_kvs(kv_ptr, (void *) prop, prop_mes->m_id, t_id,
                                                        prop_rep, prop->log_no, true);
    bookkeeping_if_creating_prop_ack(prop, prop_rep, kv_ptr, t_id);
  }
  dbg_kv_ptr_create_acc_prop_rep(kv_ptr, number_of_reqs);
}

inline void create_prop_rep(cp_prop_t *prop,
                            cp_prop_mes_t *prop_mes,
                            cp_rmw_rep_t *prop_rep,
                            mica_op_t *kv_ptr,
                            uint16_t t_id)
{
  uint64_t number_of_reqs = 0;

  prop_rep->l_id = prop->l_id;
  lock_kv_ptr(kv_ptr, t_id);
  {
    create_prop_rep_after_locking_kv_ptr(prop, prop_mes, prop_rep, kv_ptr, &number_of_reqs, t_id);
  }
  unlock_kv_ptr(kv_ptr, t_id);
  print_log_on_rmw_recv(prop->t_rmw_id, prop_mes->m_id, prop->log_no, prop_rep,
                        prop->ts, kv_ptr, number_of_reqs, false, t_id);
}

static inline void create_acc_rep_after_locking_kv_ptr(cp_acc_t *acc,
                                                       cp_acc_mes_t *acc_mes,
                                                       cp_rmw_rep_t *acc_rep,
                                                       mica_op_t *kv_ptr,
                                                       uint64_t *number_of_reqs,
                                                       uint16_t t_id)
{
  uint64_t rmw_l_id = acc->t_rmw_id;
  //my_printf(cyan, "Received accept with rmw_id %u, glob_sess %u \n", rmw_l_id, glob_sess_id);
  uint32_t log_no = acc->log_no;
  uint8_t acc_m_id = acc_mes->m_id;
  if (!is_log_lower_higher_or_has_rmw_committed(log_no, kv_ptr, rmw_l_id, t_id, acc_rep)) {
    acc_rep->opcode = handle_remote_prop_or_acc_in_kvs(kv_ptr, (void *) acc, acc_m_id, t_id, acc_rep, log_no, false);
    if (acc_rep->opcode == RMW_ACK) {
      activate_kv_pair(ACCEPTED, acc->ts.version, kv_ptr, acc->opcode,
                       acc->ts.m_id, NULL, rmw_l_id, log_no, t_id,
                       ENABLE_ASSERTIONS ? "received accept" : NULL);
      memcpy(kv_ptr->last_accepted_value, acc->value, (size_t) RMW_VALUE_SIZE);
      kv_ptr->base_acc_ts = acc->base_ts;
    }
  }
  dbg_kv_ptr_create_acc_prop_rep(kv_ptr, number_of_reqs);
}

inline void create_acc_rep(cp_acc_t *acc,
                           cp_acc_mes_t *acc_mes,
                           cp_rmw_rep_t *acc_rep,
                           mica_op_t *kv_ptr,
                           uint16_t t_id)
{
  uint64_t number_of_reqs = 0;
  acc_rep->l_id = acc->l_id;
  lock_kv_ptr(kv_ptr, t_id);
  {
    create_acc_rep_after_locking_kv_ptr(acc, acc_mes, acc_rep, kv_ptr, &number_of_reqs, t_id);
  }
  unlock_kv_ptr(kv_ptr, t_id);
  print_log_on_rmw_recv(acc->t_rmw_id, acc_mes->m_id, acc->log_no, acc_rep,
                        acc->ts, kv_ptr, number_of_reqs, true, t_id);
}