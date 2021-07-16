#ifndef CP_MAIN_H
#define CP_MAIN_H

#include <stdint.h>
#include <pthread.h>
#include <stdint-gcc.h>
#include <od_fifo.h>
#include "od_city.h"
#include "cp_config.h"
#include "cp_messages.h"
#include "cp_buffer_sizes.h"
#include "od_stats.h"



#define LOCAL_PROP_NUM_ (SESSIONS_PER_THREAD)
#define LOCAL_PROP_NUM (ENABLE_RMWS == 1 ? LOCAL_PROP_NUM_ : 0)
#define PROP_FIFO_SIZE (LOCAL_PROP_NUM + 1)
#define ACC_FIFO_SIZE (LOCAL_PROP_NUM + 1)
#define COM_FIFO_SIZE (LOCAL_PROP_NUM + 1)
#define COM_ROB_SIZE (LOCAL_PROP_NUM + 1)



typedef struct cp_core_ctx cp_core_ctx_t;

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
  struct rmw_id rmw_id; // this is implicitly the l_id
  struct rmw_rep_info rmw_reps;
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


// Registering data structure
extern atomic_uint_fast64_t committed_glob_sess_rmw_id[GLOBAL_SESSION_NUM];

typedef struct rmw_rep_flag {
  bool is_accept;
  uint8_t unused;
  uint16_t op_i;
} rmw_rep_flag_t;


typedef struct trace_op {
  uint16_t session_id;
  bool attempt_all_aboard;
  ts_tuple_t ts;
  struct key key;
  uint8_t opcode;
  uint8_t val_len; // this represents the maximum value len
  uint8_t value[VALUE_SIZE];
  uint8_t *value_to_write;
  uint8_t *value_to_read; //compare value for CAS/  addition argument for F&A
  uint32_t index_to_req_array;
  uint32_t real_val_len; // this is the value length the client is interested in
} trace_op_t;

typedef struct thread_stats {
  uint64_t total_reqs;
  od_qp_stats_t qp_stats[QP_NUM];
  uint64_t cancelled_rmws;
  uint64_t all_aboard_rmws; // completed ones
} t_stats_t;
extern t_stats_t t_stats[WORKERS_PER_MACHINE];


struct mica_op;
extern FILE* rmw_verify_fp[WORKERS_PER_MACHINE];

#endif
