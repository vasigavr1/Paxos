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
