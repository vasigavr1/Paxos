//
// Created by vasilis on 28/06/2021.
//

#ifndef CP_CORE_UTIL_H
#define CP_CORE_UTIL_H

#include <cp_core_generic_util.h>



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



// After gathering a quorum of proposal reps, one of them was a lower TS accept, try and help it
void attempt_local_accept_to_help(loc_entry_t *loc_entry,
                                  uint16_t t_id);
void attempt_local_accept(loc_entry_t *loc_entry,
                          uint16_t t_id);

/*--------------------------------------------------------------------------
 * --------------------COMMITING-------------------------------------
 * --------------------------------------------------------------------------*/

void commit_rmw(mica_op_t *kv_ptr,
                void* rmw,
                loc_entry_t *loc_entry,
                uint8_t flag,
                uint16_t t_id);


// On gathering quorum of acks for commit, commit locally and
// signal that the session must be freed if not helping
void act_on_quorum_of_commit_acks(cp_core_ctx_t *cp_core_ctx,
                                  uint16_t sess_id);



/*--------------------------------------------------------------------------
 * --------------------HANDLE REPLIES-------------------------------------
 * --------------------------------------------------------------------------*/


// Handle read replies that refer to RMWs (either replies to accepts or proposes)
void handle_rmw_rep_replies(cp_core_ctx_t *cp_core_ctx,
                            cp_rmw_rep_mes_t *r_rep_mes,
                            bool is_accept);




/*--------------------------------------------------------------------------
 * -----------------------------RMW-FSM-------------------------------------
 * --------------------------------------------------------------------------*/
void inspect_props_if_ready_to_inspect(cp_core_ctx_t *cp_core_ctx,
                                       loc_entry_t *loc_entry);

void inspect_accepts_if_ready_to_inspect(cp_core_ctx_t *cp_core_ctx,
                                         loc_entry_t *loc_entry);


//------------------------------HELP STUCK RMW------------------------------------------




// local_entry->state = NEEDS_KV_PTR
void handle_needs_kv_ptr_state(cp_core_ctx_t *cp_core_ctx,
                               loc_entry_t *loc_entry,
                               uint16_t sess_i,
                               uint16_t t_id);

// local_entry->state == RETRY_WITH_BIGGER_TS
void take_kv_ptr_with_higher_TS(sess_stall_t *stall_info,
                                loc_entry_t *loc_entry,
                                bool from_propose,
                                uint16_t t_id);


// Worker inspects its local RMW entries
void cp_core_inspect_rmws(cp_core_ctx_t *cp_core_ctx);
/*--------------------------------------------------------------------------
 * --------------------INIT RMW-------------------------------------
 * --------------------------------------------------------------------------*/


void insert_rmw(cp_core_ctx_t *cp_core_ctx,
                trace_op_t *op,
                uint16_t t_id);




void rmw_tries_to_get_kv_ptr_first_time(trace_op_t *op,
                                        mica_op_t *kv_ptr,
                                        cp_core_ctx_t *cp_core_ctx,
                                        uint16_t op_i,
                                        uint16_t t_id);



#endif //CP_CORE_UTIL_H
