//
// Created by vasilis on 28/06/2021.
//

#ifndef CP_CORE_UTIL_H
#define CP_CORE_UTIL_H



#include <cp_config.h>
#include <od_generic_inline_util.h>
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
void fill_commit_message_from_l_entry(cp_com_t *com,
                                      loc_entry_t *loc_entry,
                                      uint8_t broadcast_state,
                                      uint16_t t_id);

cp_core_ctx_t* cp_init_cp_core_ctx(void *cp_ctx,
                                   sess_stall_t *stall_info,
                                   void *ctx,
                                   uint16_t t_id);


#endif //CP_CORE_UTIL_H
