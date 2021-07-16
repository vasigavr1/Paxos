//
// Created by vasilis on 28/06/2021.
//

#ifndef CP_CORE_GENERIC_UTIL_H
#define CP_CORE_GENERIC_UTIL_H


#include <cp_config.h>
#include <cp_main.h>
#include <od_wrkr_side_calls.h>
#include <od_generic_inline_util.h>


/* ---------------------------------------------------------------------------
//------------------------------ GENERIC UTILITY------------------------------------------
//---------------------------------------------------------------------------*/

static inline const char* committing_flag_to_str(uint8_t state)
{
  switch (state)
  {
    case FROM_LOG_TOO_LOW_REP:
      return "FROM_LOG_TOO_LOW_REP";
    case FROM_ALREADY_COMM_REP:
      return "FROM_ALREADY_COMM_REP";
    case FROM_LOCAL:
      return "FROM_LOCAL";
    case FROM_ALREADY_COMM_REP_HELP:
      return "FROM_ALREADY_COMM_REP_HELP";
    case FROM_LOCAL_HELP:
      return "FROM_LOCAL_HELP";
    case FROM_REMOTE_COMMIT:
      return "FROM_REMOTE_COMMIT";
    case FROM_REMOTE_COMMIT_NO_VAL:
      return "FROM_REMOTE_COMMIT_NO_VAL";
    case FROM_LOCAL_ACQUIRE:
      return "FROM_LOCAL_ACQUIRE";
    case FROM_OOE_READ:
      return "FROM_OOE_READ";
    case FROM_TRACE_WRITE:
      return "FROM_TRACE_WRITE";
    case FROM_BASE_TS_STALE:
      return "FROM_BASE_TS_STALE";
    case FROM_ISOLATED_OP:
      return "FROM_ISOLATED_OP";
    case FROM_REMOTE_WRITE_RELEASE:
      return "FROM_REMOTE_WRITE_RELEASE";
    case FROM_OOE_LOCAL_WRITE:
      return "FROM_OOE_LOCAL_WRITE";
    default: return "Unknown";
  }
}

static inline const char* help_state_to_str(uint8_t state)
{
  switch (state)
  {
    case NOT_HELPING:
      return "NOT_HELPING";
    case PROPOSE_NOT_LOCALLY_ACKED:
      return "PROPOSE_NOT_LOCALLY_ACKED";
    case HELPING:
      return "HELPING";
    case PROPOSE_LOCALLY_ACCEPTED:
      return "PROPOSE_LOCALLY_ACCEPTED";
    case HELP_PREV_COMMITTED_LOG_TOO_HIGH:
      return "HELP_PREV_COMMITTED_LOG_TOO_HIGH";
    case IS_HELPER:
      return "IS_HELPER";
    default: return "Unknown";
  }
}


static inline const char* state_to_str(uint8_t state)
{
  if (ENABLE_ASSERTIONS) {
    switch (state) {
      case INVALID_RMW:
        return "INVALID_RMW";
      case PROPOSED:
        return "PROPOSED";
      case ACCEPTED:
        return "ACCEPTED";
      case NEEDS_KV_PTR:
        return "NEEDS_KV_PTR";
      case RETRY_WITH_BIGGER_TS:
        return "RETRY_WITH_BIGGER_TS";
      case MUST_BCAST_COMMITS:
        return "MUST_BCAST_COMMITS";
      case MUST_BCAST_COMMITS_FROM_HELP:
        return "MUST_BCAST_COMMITS_FROM_HELP";
      case COMMITTED:
        return "COMMITTED";
      case CAS_FAILED:
        return "CAS_FAILED";
      default:
        return "Unknown";
    }
  }
}

static inline void print_loc_entry(loc_entry_t *loc_entry, color_t color, uint16_t t_id)
{
  my_printf(color, "WORKER %u -------%s-Local Entry------------ \n", t_id,
            loc_entry->help_loc_entry == NULL ? "HELP" : "-");
  my_printf(color, "Key : %u \n", loc_entry->key.bkt);
  my_printf(color, "Session %u/%u \n", loc_entry->sess_id, loc_entry->glob_sess_id);
  my_printf(color, "State %s \n", state_to_str(loc_entry->state));
  my_printf(color, "Log no %u\n", loc_entry->log_no);
  my_printf(color, "Rmw %u\n", loc_entry->rmw_id.id);
  print_ts(loc_entry->base_ts, "Base base_ts:", color);
  print_ts(loc_entry->new_ts, "Propose base_ts:", color);
  my_printf(color, "Helping state %s \n", help_state_to_str(loc_entry->helping_flag));
}

static inline void print_kv_ptr(mica_op_t *kv_ptr, color_t color, uint16_t t_id)
{
  my_printf(color, "WORKER %u-------KV_ptr----------- \n", t_id);
  my_printf(color, "Key : %u \n", kv_ptr->key.bkt);
  my_printf(color, "*****Committed RMW***** \n");
  my_printf(color, "Last committed log %u\n", kv_ptr->last_committed_log_no);
  my_printf(color, "Last committed rmw %u\n", kv_ptr->last_committed_rmw_id.id);
  print_ts(kv_ptr->base_acc_ts, "Base base_ts:", color);

  my_printf(color, "*****Active RMW*****\n");
  my_printf(color, "State %s \n", state_to_str(kv_ptr->state));
  my_printf(color, "Log %u\n", kv_ptr->log_no);
  my_printf(color, "RMW-id %u \n", kv_ptr->rmw_id.id);
  print_ts(kv_ptr->prop_ts, "Proposed base_ts:", color);
  print_ts(kv_ptr->accepted_ts, "Accepted base_ts:", color);
}

static inline uint8_t sum_of_reps(struct rmw_rep_info* rmw_reps)
{
  return rmw_reps->acks + rmw_reps->rmw_id_commited +
         rmw_reps->log_too_small + rmw_reps->already_accepted +
         rmw_reps->seen_higher_prop_acc + rmw_reps->log_too_high;
}

static inline bool opcode_is_rmw_rep(uint8_t opcode)
{
  return (opcode >= RMW_ACK && opcode <= NO_OP_PROP_REP);
}

static inline bool rmw_ids_are_equal(struct rmw_id *id1, struct rmw_id *id2)
{
  return id1->id == id2->id;
}

static inline bool opcode_is_compare_rmw(uint8_t opcode)
{
  return opcode == COMPARE_AND_SWAP_WEAK || opcode == COMPARE_AND_SWAP_STRONG;
}

static inline void write_kv_ptr_val(mica_op_t *kv_ptr, uint8_t *new_val,
                                    size_t val_size, uint8_t flag)
{
  memcpy(kv_ptr->value, new_val, val_size);
}

static inline void assign_second_rmw_id_to_first(struct rmw_id* rmw_id1, struct rmw_id* rmw_id2)
{
  rmw_id1->id = rmw_id2->id;
}


static inline void swap_rmw_ids(struct rmw_id* rmw_id1, struct rmw_id* rmw_id2)
{
  struct rmw_id  tmp = *rmw_id1;
  assign_second_rmw_id_to_first(rmw_id1, rmw_id2);
  assign_second_rmw_id_to_first(rmw_id2, &tmp);
}


static inline void write_kv_ptr_acc_val(mica_op_t *kv_ptr, uint8_t *new_val, size_t val_size)
{
  memcpy(kv_ptr->last_accepted_value, new_val, val_size);
}

static inline void write_kv_if_conditional_on_ts(mica_op_t *kv_ptr, uint8_t *new_val,
                                                 size_t val_size,
                                                 uint8_t flag, ts_tuple_t base_ts)
{
  lock_seqlock(&kv_ptr->seqlock);
  if (compare_ts(&base_ts, &kv_ptr->ts) == GREATER) {
    write_kv_ptr_val(kv_ptr, new_val, (size_t) VALUE_SIZE, flag);
    kv_ptr->ts = base_ts;
  }
  unlock_seqlock(&kv_ptr->seqlock);
}

static inline void write_kv_if_conditional_on_netw_ts(mica_op_t *kv_ptr, uint8_t *new_val,
                                                      size_t val_size, uint8_t flag,
                                                      struct network_ts_tuple netw_base_ts)
{
  ts_tuple_t base_ts = {netw_base_ts.m_id, netw_base_ts.version};
  write_kv_if_conditional_on_ts(kv_ptr, new_val, val_size, flag, base_ts);
}

static inline bool same_rmw_id_same_log_same_ts(mica_op_t *kv_ptr, loc_entry_t *loc_entry)
{
  return rmw_ids_are_equal(&loc_entry->rmw_id, &kv_ptr->rmw_id) &&
         loc_entry->log_no == kv_ptr->log_no &&
         compare_ts(&loc_entry->new_ts, &kv_ptr->prop_ts) == EQUAL;
}

static inline bool same_rmw_id_same_log(mica_op_t *kv_ptr, loc_entry_t *loc_entry)
{
  return rmw_ids_are_equal(&loc_entry->rmw_id, &kv_ptr->rmw_id) &&
         loc_entry->log_no == kv_ptr->log_no;
}


#endif //CP_CORE_GENERIC_UTIL_H
