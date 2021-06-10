//
// Created by vasilis on 27/04/20.
//

#ifndef CP_CONFIG_H
#define CP_CONFIG_H


#ifndef _GNU_SOURCE
# define _GNU_SOURCE
#endif


// Generic header files
#include "od_top.h"
#include "cp_opcodes.h"

void cp_stats(stats_ctx_t *ctx);


// CORE CONFIGURATION
#define PROP_CREDITS 3//
#define ACC_CREDITS PROP_CREDITS
#define PROP_COALESCE 10
#define ACC_COALESCE PROP_COALESCE
#define MAX_PROP_ACC_COALESCE (MAX(PROP_COALESCE, ACC_COALESCE))
#define COM_COALESCE 10
#define COM_CREDITS 5

#define MEASURE_SLOW_PATH 0
#define MAX_OP_BATCH SESSIONS_PER_THREAD



// Important Knobs

#define ENABLE_COMMITS_WITH_NO_VAL 1
#define ENABLE_CAS_CANCELLING 1
#define ENABLE_ALL_ABOARD 0


// TIMEOUTS
#define WRITE_FIFO_TIMEOUT M_1
#define RMW_BACK_OFF_TIMEOUT 1500 //K_32 //K_32// M_1
#define ALL_ABOARD_TIMEOUT_CNT K_16
#define LOG_TOO_HIGH_TIME_OUT 10



#define VERIFY_PAXOS 0
#define PRINT_LOGS 0
#define COMMIT_LOGS 0
#define DUMP_STATS_2_FILE 0


#define PROP_SEND_MCAST_QP 0
#define ACC_SEND_MCAST_QP 1
#define COM_SEND_MCAST_QP 2


//////////////////////////////////////////////////////
/////////////~~~~STRUCTS~~~~~~/////////////////////////
//////////////////////////////////////////////////////



// unique RMW id-- each machine must remember how many
// RMW each thread has committed, to avoid committing an RMW twice
typedef struct rmw_id {
  //uint32_t glob_sess_id; // global session id
  uint64_t id; // the local rmw id of the source
} rmw_id_t;


#define MICA_VALUE_SIZE (VALUE_SIZE + (FIND_PADDING_CUST_ALIGN(VALUE_SIZE, 32)))
#define MICA_OP_SIZE_  (100 + (2 * (MICA_VALUE_SIZE)))
#define MICA_OP_PADDING_SIZE  (FIND_PADDING(MICA_OP_SIZE_))

#define MICA_OP_SIZE  (MICA_OP_SIZE_ + MICA_OP_PADDING_SIZE)
typedef struct mica_op {
  // Cache-line -1
  uint8_t value[MICA_VALUE_SIZE];
  uint8_t last_accepted_value[MICA_VALUE_SIZE];


  // Cache-line -2
  struct key key;
  seqlock_t seqlock;

  uint8_t opcode; // what kind of RMW
  uint8_t state;
  uint8_t unused[2];

  // BYTES: 20 - 32
  uint32_t log_no; // keep track of the biggest log_no that has not been committed
  uint32_t accepted_log_no; // not really needed, but good for debug
  uint32_t last_committed_log_no;

  // BYTES: 32 - 64 -- each takes 8
  ts_tuple_t ts; // base base_ts
  ts_tuple_t prop_ts;
  ts_tuple_t accepted_ts;
  ts_tuple_t base_acc_ts;


  // Cache-line 3 -- each rmw_id takes up 8 bytes
  struct rmw_id rmw_id;
  //struct rmw_id last_registered_rmw_id; // i was using it to put in accepts, when accepts carried last-registered-rmw-id
  struct rmw_id last_committed_rmw_id;
  struct rmw_id accepted_rmw_id; // not really needed, but useful for debugging
  uint64_t epoch_id;
  uint32_t key_id; // strictly for debug

  uint8_t padding[MICA_OP_PADDING_SIZE];
} mica_op_t;






#endif //CP_CONFIG_H
