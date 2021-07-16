#ifndef CP_MAIN_H
#define CP_MAIN_H

#include <stdint.h>
#include <stdint-gcc.h>
#include "cp_config.h"
#include "cp_messages.h"
#include "cp_buffer_sizes.h"
//#include "od_stats.h"

typedef struct fifo fifo_t;

#define LOCAL_PROP_NUM_ (SESSIONS_PER_THREAD)
#define LOCAL_PROP_NUM (ENABLE_RMWS == 1 ? LOCAL_PROP_NUM_ : 0)
#define PROP_FIFO_SIZE (LOCAL_PROP_NUM + 1)
#define ACC_FIFO_SIZE (LOCAL_PROP_NUM + 1)
#define COM_FIFO_SIZE (LOCAL_PROP_NUM + 1)
#define COM_ROB_SIZE (LOCAL_PROP_NUM + 1)



typedef struct cp_core_ctx cp_core_ctx_t;


// Registering data structure
extern atomic_uint_fast64_t committed_glob_sess_rmw_id[GLOBAL_SESSION_NUM];


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
