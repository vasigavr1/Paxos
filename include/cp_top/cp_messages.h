//
// Created by vasilis on 22/05/20.
//

#ifndef CP_MESSAGES_H
#define CP_MESSAGES_H

#include <od_top.h>
#include <cp_config.h>

// post some extra receives to avoid spurious out_of_buffer errors
#define RECV_WR_SAFETY_MARGIN 2


// IMPORTANT SIZES
#define TS_TUPLE_SIZE (5) // version and m_id consist the Timestamp tuple
#define LOG_NO_SIZE 4
#define RMW_VALUE_SIZE VALUE_SIZE //
#define SESSION_BYTES 2 // session ids must fit in 2 bytes i.e.
// in the first round of a release the first bytes of the value get overwritten
// before ovewritting them they get stored in astruct with capacity SEND_CONF_VEC_SIZE
#define SEND_CONF_VEC_SIZE 2


// PROPOSES
#define PROP_MES_HEADER (10) // local id + coalesce num + m_id
#define PROP_SIZE (42 + 2) // l_id 8, RMW_id- 8, ts 5, key 8, log_number 4, opcode 1 + basets 8
#define PROP_MES_SIZE (PROP_MES_HEADER + (PROP_SIZE * PROP_COALESCE))
#define PROP_RECV_SIZE (GRH_SIZE + PROP_MES_SIZE)

#define MAX_PROP_WRS (MESSAGES_IN_BCAST_BATCH)
#define MAX_RECV_PROP_WRS ((PROP_CREDITS * REM_MACH_NUM) + RECV_WR_SAFETY_MARGIN)
#define MAX_INCOMING_PROP (MAX_RECV_PROP_WRS * PROP_COALESCE)

//
#define MAX_PROP_REP_WRS (PROP_CREDITS * REM_MACH_NUM)
#define RMW_REP_MES_HEADER (11) //l_id 8 , coalesce_num 1, m_id 1, opcode 1 TODO remove opcode
#define RMW_REP_SMALL_SIZE 9 // lid and opcode
#define RMW_REP_ONLY_TS_SIZE (9 + TS_TUPLE_SIZE)
// PROPOSE REPLIES
#define PROP_REP_LOG_TOO_LOW_SIZE (26 + RMW_VALUE_SIZE)  //l_id- 8, RMW_id- 8, ts 5, log_no - 4,  RMW value, opcode 1
#define PROP_REP_BASE_TS_STALE_SIZE (9 + TS_TUPLE_SIZE + RMW_VALUE_SIZE)
#define PROP_REP_ACCEPTED_SIZE (RMW_REP_ONLY_TS_SIZE + 8 + RMW_VALUE_SIZE + TS_TUPLE_SIZE) //lid 8, opcode 1, ts 5, rmw-id 8 + base_ts 5
#define PROP_REP_SIZE PROP_REP_ACCEPTED_SIZE
#define PROP_REP_MES_SIZE (RMW_REP_MES_HEADER + (PROP_COALESCE * PROP_REP_ACCEPTED_SIZE)) //Message size of replies to proposes
#define MAX_RECV_PROP_REP_WRS ((REM_MACH_NUM * PROP_CREDITS))
#define PROP_REP_RECV_SIZE (GRH_SIZE + PROP_REP_MES_SIZE)
#define PROP_REP_FIFO_SIZE (MAX_INCOMING_PROP)


// ACCEPT REPLIES
#define MAX_ACC_REP_WRS (ACC_CREDITS * REM_MACH_NUM)
#define MAX_RECV_ACC_REP_WRS ((REM_MACH_NUM * ACC_CREDITS))
// TODO refactor the sizes, the biggest accept reply is smaller than the biggest prop-reply (for seen lower accept)
#define ACC_REP_SIZE PROP_REP_SIZE // (26 + RMW_VALUE_SIZE)  //l_id- 8, RMW_id- 10, ts 5, log_no - 4,  RMW value, opcode 1
#define ACC_REP_MES_SIZE (RMW_REP_MES_HEADER + (ACC_COALESCE * ACC_REP_SIZE)) //Message size of replies to accepts
#define ACC_REP_RECV_SIZE (GRH_SIZE + ACC_REP_MES_SIZE)

#define MAX_RMW_REP_WRS (MAX_PROP_REP_WRS + MAX_ACC_REP_WRS)
#define MAX_RECV_RMW_REP_WRS (MAX_RECV_PROP_REP_WRS + MAX_RECV_ACC_REP_WRS)
#define RMW_REP_RECV_SIZE MAX(PROP_REP_RECV_SIZE, ACC_REP_RECV_SIZE)
#define RMW_REP_MES_SIZE MAX(PROP_REP_MES_SIZE, ACC_REP_MES_SIZE)
#define RMW_REP_FIFO_SIZE (PROP_REP_FIFO_SIZE + ACC_REP_FIFO_SIZE)

// ACCEPTS -- ACCEPT coalescing is derived from max write capacity. ACC reps are derived from accept coalescing
#define ACC_MES_HEADER (10) //l_id 8 , coalesce_num 1
#define ACC_HEADER (35 + 5 + 4) //original l_id 8 key 8 rmw-id 10, last-committed rmw_id 10, ts 5 log_no 4 opcode 1, val_len 1
#define ACC_SIZE (ACC_HEADER + RMW_VALUE_SIZE)
#define ACC_MES_SIZE (ACC_MES_HEADER + (ACC_SIZE * ACC_COALESCE))
#define ACC_RECV_SIZE (GRH_SIZE + ACC_MES_SIZE)


#define MAX_ACC_WRS (MESSAGES_IN_BCAST_BATCH)
#define MAX_RECV_ACC_WRS ((ACC_CREDITS * REM_MACH_NUM) + RECV_WR_SAFETY_MARGIN)
#define MAX_INCOMING_ACC (MAX_RECV_ACC_WRS * ACC_COALESCE)
#define ACC_REP_FIFO_SIZE (MAX_INCOMING_ACC)

#define MAX_INCOMING_RMW MAX(MAX_INCOMING_ACC, MAX_INCOMING_PROP)


// COMMITS
#define MAX_COM_WRS (MESSAGES_IN_BCAST_BATCH)
#define MAX_RECV_COM_WRS ((COM_CREDITS * REM_MACH_NUM) + RECV_WR_SAFETY_MARGIN)
#define COM_MES_HEADER (12) // local id + m_id + com_num + opcode
#define COM_HEADER (27 + 1)
#define COM_SIZE (COM_HEADER + RMW_VALUE_SIZE)
#define COM_MES_SIZE MAX_COM_SIZE //(COM_MES_HEADER + (COM_SIZE * COM_COALESCE))
#define COM_RECV_SIZE (GRH_SIZE + COM_MES_SIZE)
#define MAX_COM_SIZE_NO_HDR (MAX_COM_SIZE - COM_MES_HEADER)
// COMMIT_NO_VAL
#define COMMIT_NO_VAL_SIZE (22 + 2)
#define COM_NO_VAL_MES_SIZE MAX_COM_SIZE

#define COM_COALESCE (MAX_COM_SIZE_NO_HDR / COM_SIZE)
#define COM_NO_VAL_COALESCE (MAX_COM_SIZE_NO_HDR / COMMIT_NO_VAL_SIZE)
#define MAX_COM_COALESCE COM_NO_VAL_COALESCE

#define MAX_INCOMING_COM (MAX_RECV_COM_WRS * MAX_COM_COALESCE)


/// PROPOSES
typedef struct propose {
  struct network_ts_tuple ts;
  uint8_t opcode;
  uint8_t unused[2];
  mica_key_t key;

  uint64_t t_rmw_id;
  uint32_t log_no;
  uint64_t l_id; // the l_id of the rmw local_entry
  ts_tuple_t base_ts;
} __attribute__((__packed__)) cp_prop_t;

typedef struct cp_prop_mes {
  uint64_t l_id;
  uint8_t coalesce_num;
  uint8_t m_id;
  cp_prop_t prop[PROP_COALESCE];
}__attribute__((__packed__)) cp_prop_mes_t;

typedef struct cp_prop_message_ud_req {
  uint8_t grh[GRH_SIZE];
  cp_prop_mes_t prop_mes;
} cp_prop_mes_ud_t;

/// RMW REPS
typedef struct rmw_rep_last_committed {
  uint8_t opcode;
  uint64_t l_id; // the l_id of the rmw local_entry
  struct network_ts_tuple ts; // This is the base for RMW-already-committed or Log-to-low, it's proposed/accepted ts for the rest
  uint8_t value[RMW_VALUE_SIZE];
  uint64_t rmw_id; //accepted  OR last committed
  uint32_t log_no_or_base_version; // log no for RMW-already-committed/Log-too-low, base_ts.version for proposed/accepted
  uint8_t base_m_id; // base_ts.m_id used in a LowerAcc reply to a propose
} __attribute__((__packed__)) cp_rmw_rep_t;


typedef struct rmw_rep_message {
  uint8_t coalesce_num;
  uint8_t m_id;
  uint8_t opcode;
  uint64_t l_id ;
  cp_rmw_rep_t rmw_rep[MAX_PROP_ACC_COALESCE];
}__attribute__((__packed__)) cp_rmw_rep_mes_t;

typedef struct cp_rmw_rep_message_ud_req {
  uint8_t grh[GRH_SIZE];
  cp_rmw_rep_mes_t rep_mes;
} cp_rmw_rep_mes_ud_t;

/// Accepts
typedef struct accept {
  struct network_ts_tuple ts;
  uint8_t opcode;
  uint8_t val_len;
  uint8_t unused;
  mica_key_t key;
  uint64_t t_rmw_id ; // the upper bits are overloaded to indicate that the accept is trying to flip a bit
  uint64_t l_id;
  uint32_t log_no;
  ts_tuple_t base_ts;
  //uint8_t unused_[3];
  uint8_t value[RMW_VALUE_SIZE];
} __attribute__((__packed__)) cp_acc_t;

typedef struct cp_acc_mes {
  uint64_t l_id;
  uint8_t coalesce_num;
  uint8_t m_id;
  cp_acc_t acc[ACC_COALESCE];

}__attribute__((__packed__)) cp_acc_mes_t;

typedef struct cp_acc_message_ud_req {
  uint8_t grh[GRH_SIZE];
  cp_acc_mes_t acc_mes;
} cp_acc_mes_ud_t;

/// Commits
typedef struct commit_no_val {
  uint32_t log_no;
  uint8_t unused;
  uint8_t opcode;
  uint8_t unused_[2];
  mica_key_t key;
  uint64_t t_rmw_id; //rmw lid to be committed
}__attribute__((__packed__)) cp_com_no_val_t;

typedef struct commit {
  struct network_ts_tuple base_ts;
  uint8_t opcode;
  uint8_t val_len;
  uint8_t unused;
  mica_key_t key;
  uint64_t t_rmw_id; //rmw lid to be committed
  uint32_t log_no;
  uint8_t value[VALUE_SIZE];
} __attribute__((__packed__)) cp_com_t;


typedef struct cp_com_mes {
  uint64_t l_id;
  uint8_t m_id;
  uint8_t coalesce_num;
  uint8_t opcode;
  uint8_t unused;
  cp_com_t com[COM_COALESCE];
}__attribute__((__packed__)) cp_com_mes_t;

typedef struct cp_com_message_ud_req {
  uint8_t grh[GRH_SIZE];
  cp_com_mes_t com_mes;
} cp_com_mes_ud_t;

// Give an opcode to get the capacity of the read rep messages
static inline uint16_t get_size_from_opcode(uint8_t opcode)
{
  if (opcode > CARTS_EQUAL) opcode -= FALSE_POSITIVE_OFFSET;
  switch(opcode) {
    case LOG_TOO_SMALL:
      return PROP_REP_LOG_TOO_LOW_SIZE;
    case SEEN_LOWER_ACC:
      return PROP_REP_ACCEPTED_SIZE;
    case SEEN_HIGHER_PROP:
    case SEEN_HIGHER_ACC:
      return RMW_REP_ONLY_TS_SIZE;
    case RMW_ACK_BASE_TS_STALE:
      return PROP_REP_BASE_TS_STALE_SIZE;
    case RMW_ID_COMMITTED:
    case RMW_ID_COMMITTED_SAME_LOG:
    case RMW_ACK:
    case LOG_TOO_HIGH:
    case NO_OP_PROP_REP:
      return RMW_REP_SMALL_SIZE;
    default: if (ENABLE_ASSERTIONS) {
        my_printf(red, "Opcode %u \n", opcode);
        assert(false);
      }
  }
}



#endif //CP_MESSAGES_H
