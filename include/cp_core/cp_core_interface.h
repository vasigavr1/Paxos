//
// Created by vasilis on 28/06/2021.
//

#ifndef CP_CORE_UTIL_H
#define CP_CORE_UTIL_H



#include <cp_config.h>
#include <cp_debug_util.h>
#include "cp_main.h"



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


// On gathering quorum of acks for commit, commit locally and
// signal that the session must be freed if not helping
void act_on_quorum_of_commit_acks(cp_core_ctx_t *cp_core_ctx,
                                  uint16_t sess_id);


// Handle read replies that refer to RMWs (either replies to accepts or proposes)
void handle_rmw_rep_replies(cp_core_ctx_t *cp_core_ctx,
                            cp_rmw_rep_mes_t *r_rep_mes,
                            bool is_accept);




// Worker inspects its local RMW entries
void cp_core_inspect_rmws(cp_core_ctx_t *cp_core_ctx);


void insert_rmw(cp_core_ctx_t *cp_core_ctx,
                trace_op_t *op,
                uint16_t t_id);


void rmw_tries_to_get_kv_ptr_first_time(trace_op_t *op,
                                        mica_op_t *kv_ptr,
                                        cp_core_ctx_t *cp_core_ctx,
                                        uint16_t op_i,
                                        uint16_t t_id);

void on_receiving_remote_commit(mica_op_t *kv_ptr,
                                cp_com_t *com,
                                cp_com_mes_t *com_mes,
                                uint16_t op_i,
                                uint16_t t_id);

// Fill a write message with a commit
static inline void fill_commit_message_from_l_entry(struct commit *com, loc_entry_t *loc_entry,
                                                    uint8_t broadcast_state, uint16_t t_id)
{
  check_loc_entry_when_filling_com(loc_entry, broadcast_state, t_id);

  memcpy(&com->key, &loc_entry->key, KEY_SIZE);
  com->t_rmw_id = loc_entry->rmw_id.id;
  com->base_ts.m_id = loc_entry->base_ts.m_id;
  if (loc_entry->avoid_val_in_com) {
    assert(ENABLE_COMMITS_WITH_NO_VAL);
    com->opcode = COMMIT_OP_NO_VAL;
    loc_entry->avoid_val_in_com = false;
    com->base_ts.version = loc_entry->log_no;
  }
  else {
    com->opcode = COMMIT_OP;
    com->log_no = loc_entry->log_no;
    com->base_ts.version = loc_entry->base_ts.version;
    if (broadcast_state == MUST_BCAST_COMMITS && !loc_entry->rmw_is_successful) {
      memcpy(com->value, loc_entry->value_to_read, (size_t) RMW_VALUE_SIZE);
    } else {
      memcpy(com->value, loc_entry->value_to_write, (size_t) RMW_VALUE_SIZE);
    }
    //print_treiber_top((struct top *) com->value, "Sending commit", cyan);
    if (ENABLE_ASSERTIONS) {
      assert(com->log_no > 0);
      assert(com->t_rmw_id > 0);
    }
  }
}







#endif //CP_CORE_UTIL_H
