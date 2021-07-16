//
// Created by vasilis on 16/07/2021.
//

#ifndef ODYSSEY_CP_NETW_STRUCTS_H
#define ODYSSEY_CP_NETW_STRUCTS_H

#include <cp_main.h>

typedef struct rmw_rep_flag {
  bool is_accept;
  uint8_t unused;
  uint16_t op_i;
} rmw_rep_flag_t;

typedef struct cp_com_rob {
  uint64_t l_id; // for debug
  uint16_t sess_id; // connection with loc entry
  uint8_t state;
  uint8_t acks_seen;
} cp_com_rob_t;


typedef struct cp_ptr_to_ops {
  void **ptr_to_ops;
  void **ptr_to_mes;
  bool *break_message;
  uint16_t polled_ops;
} cp_ptrs_to_ops_t;

struct l_ids {
  uint64_t inserted_prop_id;
  uint64_t inserted_acc_id;
  uint64_t inserted_com_id;
  uint64_t applied_com_id;
};

typedef struct cp_cp_ctx_debug {
  bool slept;
  uint64_t loop_counter;
  uint32_t sizes_dbg_cntr;
  uint64_t debug_lids;
  uint32_t release_rdy_dbg_cnt;
  struct session_dbg *ses_dbg;
} cp_debug_t;



typedef struct cp_ctx {
  fifo_t *com_rob;
  cp_ptrs_to_ops_t *ptrs_to_ops;
  trace_info_t trace_info;
  trace_op_t *ops;
  cp_core_ctx_t *cp_core_ctx;
  sess_stall_t stall_info;
  struct l_ids l_ids;
  cp_debug_t *debug_loop;
  mica_key_t *key_per_sess;
} cp_ctx_t;

// A helper to debug sessions by remembering which write holds a given session
struct session_dbg {
  uint32_t dbg_cnt[SESSIONS_PER_THREAD];
  //uint8_t is_release[SESSIONS_PER_THREAD];
  //uint32_t request_id[SESSIONS_PER_THREAD];
};


#endif //ODYSSEY_CP_NETW_STRUCTS_H
