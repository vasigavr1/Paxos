//
// Created by vasilis on 16/07/2021.
//

#ifndef ODYSSEY_CP_CORE_STRUCTS_H
#define ODYSSEY_CP_CORE_STRUCTS_H

#include <od_top.h>

typedef struct commit_info {
  bool overwrite_kv;
  bool no_value;
  uint8_t flag;
  uint32_t log_no;
  ts_tuple_t base_ts;
  rmw_id_t rmw_id;
  uint8_t *value;
  const char* message;
} commit_info_t;

struct rmw_help_entry{
  ts_tuple_t ts;
  uint8_t opcode;
  uint8_t value[RMW_VALUE_SIZE];
  struct rmw_id rmw_id;
  uint32_t log_no;
  // RMW that has not grabbed a global entry uses this to
  // implement back-of by polling on the global entry
  uint8_t state;
};


typedef struct rmw_rep_info {
  uint8_t tot_replies;
  uint8_t acks;
  uint8_t rmw_id_commited;
  uint8_t log_too_small;
  uint8_t already_accepted;
  //  uint8_t ts_stale;
  uint8_t seen_higher_prop_acc; // Seen a higher prop or accept
  uint8_t log_too_high;
  uint8_t nacks;
  bool no_need_to_bcast; // raised when an already-committed reply does not trigger commit bcasts, because it refers to a later log
  bool ready_to_inspect;
  bool inspected;
  // used to know whether to help after a prop-- if you have seen a higher acc,
  // then you should not try to help a lower accept, and thus dont try at all
  uint32_t seen_higher_prop_version;

} rmw_rep_info_t;

// Entry that keep pending thread-local RMWs, the entries are accessed with session id
typedef struct rmw_local_entry {
  ts_tuple_t new_ts;
  struct key key;
  uint8_t opcode;
  uint8_t state;
  uint8_t helping_flag;
  bool fp_detected;
  bool killable; // can the RMW (if CAS) be killed early
  bool must_release;
  bool rmw_is_successful; // was the RMW (if CAS) successful
  bool all_aboard;
  bool avoid_val_in_com;
  bool base_ts_found;
  uint8_t value_to_write[VALUE_SIZE];
  uint8_t value_to_read[VALUE_SIZE];
  ts_tuple_t base_ts;
  uint8_t *compare_val; //for CAS- add value for FAA
  uint32_t rmw_val_len;
  rmw_id_t rmw_id; // this is implicitly the l_id
  rmw_rep_info_t rmw_reps;
  uint64_t epoch_id;
  uint16_t sess_id;
  uint32_t glob_sess_id;
  uint32_t index_to_req_array;
  uint32_t back_off_cntr;
  uint16_t log_too_high_cntr;
  uint32_t all_aboard_time_out;
  uint32_t log_no;
  uint32_t accepted_log_no; // this is the log no that has been accepted locally and thus when committed is guaranteed to be the correct logno
  uint64_t l_id; // the unique l_id of the entry, it typically coincides with the rmw_id except from helping cases
  mica_op_t *kv_ptr;
  struct rmw_help_entry *help_rmw;
  struct rmw_local_entry* help_loc_entry;
  uint32_t stalled_reason;

} loc_entry_t;


typedef struct cp_core_ctx {
  loc_entry_t *rmw_entries;
  sess_stall_t *stall_info;
  void* appl_ctx;
  void* netw_ctx;
  uint16_t t_id;
} cp_core_ctx_t;

struct dbg_glob_entry {
  ts_tuple_t last_committed_ts;
  uint32_t last_committed_log_no;
  struct rmw_id last_committed_rmw_id;
  ts_tuple_t proposed_ts;
  uint32_t proposed_log_no;
  struct rmw_id proposed_rmw_id;
  uint8_t last_committed_flag;
  uint64_t prop_acc_num;
};




#endif //ODYSSEY_CP_CORE_STRUCTS_H
