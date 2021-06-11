//
// Created by vasilis on 11/05/20.
//

#ifndef CP_DEBUG_UTIL_H
#define CP_DEBUG_UTIL_H

#include <cp_config.h>
#include "cp_main.h"
#include "cp_generic_util.h"
#include "od_debug_util.h"
#include "od_network_context.h"


/* ---------------------------------------------------------------------------
//------------------------------DEBUGGING-------------------------------------
//---------------------------------------------------------------------------*/

static inline void update_commit_logs(uint16_t t_id, uint32_t bkt, uint32_t log_no, uint8_t *old_value,
                                      uint8_t *value, const char* message, uint8_t flag)
{
  if (COMMIT_LOGS) { /*
    if (flag == LOG_COMS) {
      struct top *top = (struct top *) old_value;
      struct top *new_top = (struct top *) value;
      bool pushing = new_top->push_counter == top->push_counter + 1;
      bool popping = new_top->pop_counter == top->pop_counter + 1;
      fprintf(rmw_verify_fp[t_id], "Key: %u, log %u: %s: push/pop poitner %u/%u, "
                "key_ptr %u/%u/%u/%u %s - t = %lu\n",
              bkt, log_no, pushing ? "Pushing" : "Pulling",
              new_top->push_counter, new_top->pop_counter, new_top->key_id,
              new_top->sec_key_id, new_top->third_key_id, new_top->fourth_key_id, message,
              time_approx);
    }
    else if (flag == LOG_WS){
      struct node *node = (struct node *) old_value;
      struct node *new_node = (struct node *) value;
      fprintf(rmw_verify_fp[t_id], "Key: %u, %u/%u/%u/%u, "
                "old: %u/%u/%u/%u version %u -- %s - t = %lu\n",
              bkt, new_node->key_id,
              new_node->stack_id, new_node->push_counter, new_node->next_key_id,
              node->key_id,
              node->stack_id, node->push_counter, node->next_key_id, log_no, message,
              time_approx);
    }*/
  }
}

static inline void check_version(uint32_t version, const char *message) {
  if (ENABLE_ASSERTIONS) {


//    if (version == 0 || version % 2 != 0) {
//      my_printf(red, "Version %u %s\n", version, message);
//    }
    assert(version >= ALL_ABOARD_TS);
//    assert(version % 2 == 0);
  }
}


// Print the rep info received for a propose or an accept
static inline void print_rmw_rep_info(loc_entry_t *loc_entry, uint16_t t_id) {
  struct rmw_rep_info *rmw_rep = &loc_entry->rmw_reps;
  my_printf(yellow, "Wrkr %u Printing rmw_rep for sess %u state %u helping flag %u \n"
              "Tot_replies %u \n acks: %u \n rmw_id_committed: %u \n log_too_small %u\n"
              "already_accepted : %u\n seen_higher_prop : %u\n "
              "log_too_high: %u \n",
            t_id, loc_entry->sess_id, loc_entry->state, loc_entry->helping_flag,
            rmw_rep->tot_replies,
            rmw_rep->acks, rmw_rep->rmw_id_commited, rmw_rep->log_too_small,
            rmw_rep->already_accepted,
            rmw_rep->seen_higher_prop_acc, rmw_rep->log_too_high);
}


// When pulling a new req from the trace, check the req and the working session
static inline void check_trace_req(cp_ctx_t *cp_ctx, trace_t *trace, trace_op_t *op,
                                   int working_session, uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    assert(trace->opcode != NOP);
    check_state_with_allowed_flags(8, trace->opcode, OP_RELEASE, KVS_OP_PUT,
                                   OP_ACQUIRE, KVS_OP_GET, FETCH_AND_ADD, COMPARE_AND_SWAP_WEAK,
                                   COMPARE_AND_SWAP_STRONG);
    assert(op->opcode == trace->opcode);
    assert(!cp_ctx->stall_info.stalled[working_session]);
    if (ENABLE_RMWS && cp_ctx->prop_info->entry[working_session].state != INVALID_RMW) {
      my_printf(cyan, "wrk %u  Session %u has loc_entry state %u , helping flag %u\n", t_id,
                working_session, cp_ctx->prop_info->entry[working_session].state,
                cp_ctx->prop_info->entry[working_session].helping_flag);
      assert(false);
    }
  }
}


static inline void send_prop_checks(context_t *ctx)
{
  cp_ctx_t *cp_ctx = (cp_ctx_t *) ctx->appl_ctx;
  per_qp_meta_t *qp_meta = &ctx->qp_meta[PROP_QP_ID];
  fifo_t *send_fifo = qp_meta->send_fifo;

  // Create the broadcast messages
  cp_prop_mes_t *prop_buf = (cp_prop_mes_t *) qp_meta->send_fifo->fifo;
  cp_prop_mes_t *prop_mes = &prop_buf[send_fifo->pull_ptr];

  slot_meta_t *slot_meta = get_fifo_slot_meta_pull(send_fifo);
  uint8_t coalesce_num = (uint8_t) slot_meta->coalesce_num;
  if (ENABLE_ASSERTIONS) {
    assert(send_fifo->net_capacity >= coalesce_num);
    qp_meta->outstanding_messages += coalesce_num;
    assert(prop_mes->coalesce_num == (uint8_t) slot_meta->coalesce_num);
    assert(prop_mes->coalesce_num > 0);
    assert(prop_mes->m_id == (uint8_t) ctx->m_id);

    if (DEBUG_RMW) {
      struct propose *prop = &prop_mes->prop[0];
      my_printf(green, "Wrkr %u : I BROADCAST a propose message %u with %u props with mes_size %u, with credits: %d, lid: %u, "
                       "rmw_id %u, glob_sess id %u, log_no %u, version %u \n",
                ctx->t_id, prop->opcode, coalesce_num, slot_meta->byte_size,
                qp_meta->credits[(machine_id + 1) % MACHINE_NUM], prop_mes->l_id,
                prop->t_rmw_id, prop->t_rmw_id % GLOBAL_SESSION_NUM,
                prop->log_no, prop->ts.version);
    }

  }
  if (ENABLE_STAT_COUNTING) {
    t_stats[ctx->t_id].proposes_sent++;
  }
}


static inline void send_acc_checks(context_t *ctx)
{
  cp_ctx_t *cp_ctx = (cp_ctx_t *) ctx->appl_ctx;
  per_qp_meta_t *qp_meta = &ctx->qp_meta[ACC_QP_ID];
  fifo_t *send_fifo = qp_meta->send_fifo;

  // Create the broadcast messages
  cp_acc_mes_t *acc_buf = (cp_acc_mes_t *) qp_meta->send_fifo->fifo;
  cp_acc_mes_t *acc_mes = &acc_buf[send_fifo->pull_ptr];

  slot_meta_t *slot_meta = get_fifo_slot_meta_pull(send_fifo);
  uint8_t coalesce_num = (uint8_t) slot_meta->coalesce_num;
  if (ENABLE_ASSERTIONS) {
    assert(send_fifo->net_capacity >= coalesce_num);
    qp_meta->outstanding_messages += coalesce_num;
    assert(acc_mes->coalesce_num == (uint8_t) slot_meta->coalesce_num);
    assert(acc_mes->coalesce_num > 0);
    assert(acc_mes->m_id == (uint8_t) ctx->m_id);

    if (DEBUG_RMW) {
      cp_acc_t *acc = &acc_mes->acc[0];
      my_printf(green, "Wrkr %u : I BROADCAST an accept message %u with %u accs with mes_size %u, with credits: %d, lid: %u, "
                       "rmw_id %u, glob_sess id %u, log_no %u, version %u \n",
                ctx->t_id, acc->opcode, coalesce_num, slot_meta->byte_size,
                qp_meta->credits[(machine_id + 1) % MACHINE_NUM], acc_mes->l_id,
                acc->t_rmw_id, acc->t_rmw_id % GLOBAL_SESSION_NUM,
                acc->log_no, acc->ts.version);
    }

  }
  if (ENABLE_STAT_COUNTING) {
    t_stats[ctx->t_id].accepts_sent++;
  }
}


static inline void send_com_checks(context_t *ctx)
{
  cp_ctx_t *cp_ctx = (cp_ctx_t *) ctx->appl_ctx;
  per_qp_meta_t *qp_meta = &ctx->qp_meta[COM_QP_ID];
  fifo_t *send_fifo = qp_meta->send_fifo;

  // Create the broadcast messages
  cp_com_mes_t *com_buf = (cp_com_mes_t *) qp_meta->send_fifo->fifo;
  cp_com_mes_t *com_mes = &com_buf[send_fifo->pull_ptr];

  slot_meta_t *slot_meta = get_fifo_slot_meta_pull(send_fifo);
  uint8_t coalesce_num = (uint8_t) slot_meta->coalesce_num;
  if (ENABLE_ASSERTIONS) {
    assert(send_fifo->net_capacity >= coalesce_num);
    qp_meta->outstanding_messages += coalesce_num;
    assert(com_mes->coalesce_num == (uint8_t) slot_meta->coalesce_num);
    assert(com_mes->coalesce_num > 0);
    assert(com_mes->m_id == (uint8_t) ctx->m_id);

    for (uint8_t i = 0; i < coalesce_num; i++)
    {
      uint16_t backward_ptr = (slot_meta->backward_ptr + i) % COM_FIFO_SIZE;
      cp_com_rob_t *com_rob = (cp_com_rob_t *) get_fifo_slot(cp_ctx->com_rob, backward_ptr);
      assert(com_rob->state == VALID);
      assert(com_rob->acks_seen == 0);
      assert(com_rob->l_id == com_mes->l_id + i);
      com_rob->state = SENT_COMMIT;

    }

    if (DEBUG_RMW) {
      cp_com_t *com = &com_mes->com[0];
      my_printf(green, "Wrkr %u : I BROADCAST a commit message %u with %u coms with mes_size %u, with credits: %d, lid: %u, "
                       "rmw_id %u, glob_sess id %u, log_no %u, base version %u \n",
                ctx->t_id, com->opcode, coalesce_num, slot_meta->byte_size,
                qp_meta->credits[(machine_id + 1) % MACHINE_NUM], com_mes->l_id,
                com->t_rmw_id, com->t_rmw_id % GLOBAL_SESSION_NUM,
                com->log_no, com->base_ts.version);
    }

  }
  if (ENABLE_STAT_COUNTING) {
    t_stats[ctx->t_id].accepts_sent++;
  }
}


static inline void send_rmw_rep_checks(context_t *ctx, uint16_t qp_id)
{
  if (!ENABLE_ASSERTIONS) return;

  bool is_accept = qp_id == ACC_REP_QP_ID;
  cp_ctx_t *cp_ctx = (cp_ctx_t *) ctx->appl_ctx;
  per_qp_meta_t *qp_meta = &ctx->qp_meta[qp_id];
  fifo_t *send_fifo = qp_meta->send_fifo;

  cp_rmw_rep_mes_t *rep_buf = (cp_rmw_rep_mes_t *) qp_meta->send_fifo->fifo;
  cp_rmw_rep_mes_t *rep_mes = &rep_buf[send_fifo->pull_ptr];
  slot_meta_t *slot_meta = get_fifo_slot_meta_pull(send_fifo);
  if (DEBUG_RMW)
    my_printf(yellow, "Worker %u sending %s rep_mes l_id %lu, coalesce %u, to m_id %u, opcode %u\n",
              ctx->t_id,
              is_accept ? "accept" : "propose",
              rep_mes->l_id,
              rep_mes->coalesce_num,
              rep_mes->m_id,
              rep_mes->opcode);


}







// When forging a write (which the accept hijack)
static inline void checks_when_forging_an_accept(struct accept* acc, struct ibv_sge *send_sgl,
                                                 uint16_t br_i, uint8_t w_i, uint8_t coalesce_num, uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    assert(coalesce_num > 0);
    if (DEBUG_RMW)
      printf("Worker: %u, Accept in position %d, val-len %u, message w_size %d\n", t_id, w_i, acc->val_len,
             send_sgl[br_i].length);
    check_state_with_allowed_flags(3, acc->opcode, ACCEPT_OP, ACCEPT_OP_BIT_VECTOR);
    //assert(acc->val_len == VALUE_SIZE >> SHIFT_BITS);

  }
}

// When forging a write (which the accept hijack)
static inline void checks_when_forging_a_commit(struct commit *com, struct ibv_sge *send_sgl,
                                                uint16_t br_i, uint8_t w_i, uint8_t coalesce_num, uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    assert(coalesce_num > 0);
    if (DEBUG_RMW)
      printf("Worker: %u, Commit %d, val-len %u, message w_size %d\n", t_id, w_i, com->val_len,
             send_sgl[br_i].length);
    //assert(com->val_len == VALUE_SIZE >> SHIFT_BITS);
    assert(com->opcode == COMMIT_OP || com->opcode == RMW_ACQ_COMMIT_OP ||
           com->opcode == COMMIT_OP_NO_VAL);
  }
}


static inline void print_all_stalled_sessions(cp_ctx_t *cp_ctx, uint16_t t_id)
{
  uint32_t count = 0;
  for (uint16_t sess_i = 0; sess_i < SESSIONS_PER_THREAD; sess_i++) {
    uint32_t glob_sess_i = get_glob_sess_id((uint8_t) machine_id, t_id, sess_i);

    if (cp_ctx->stall_info.stalled) {
      loc_entry_t *loc_entry = &cp_ctx->prop_info->entry[sess_i];
      if (!count) {
        my_printf(yellow, "----------------------------------------\n");
        my_printf(yellow, "WORKER %u STALLED SESSIONS\n", t_id);
        my_printf(yellow, "----------------------------------------\n");
      }
      count++;
      my_printf(magenta, "Session %u state %u reason %u \n",
                glob_sess_i, loc_entry->state, loc_entry->stalled_reason);
      if (loc_entry->stalled_reason == STALLED_BECAUSE_NOT_ENOUGH_REPS) {
        rmw_rep_info_t *reps = &loc_entry->rmw_reps;
        my_printf(red, "Session %u, reps %u\n", glob_sess_i, reps->tot_replies);
      }
    }
  }
  if (count) {
    my_printf(yellow, "----------------------------------------\n");
    my_printf(yellow, "%u STALLED SESSIONS FOUND FOR WORKER %u \n", count, t_id);
    my_printf(yellow, "----------------------------------------\n");
  }
}






static inline void check_global_sess_id(uint8_t machine_id, uint16_t t_id,
                                        uint16_t session_id, uint64_t rmw_id)
{
  uint32_t glob_sess_id = (uint32_t) (rmw_id % GLOBAL_SESSION_NUM);
  assert(glob_ses_id_to_m_id(glob_sess_id) == machine_id);
  assert(glob_ses_id_to_t_id(glob_sess_id) == t_id);
  assert(glob_ses_id_to_sess_id(glob_sess_id) == session_id);
}


static inline void check_and_print_remote_acc(cp_acc_t *acc,
                                              cp_acc_mes_t *acc_mes,
                                              uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    assert(acc_mes->coalesce_num == 0); // the coalesce_num gets reset after polling a write
    assert(acc_mes->m_id < MACHINE_NUM);
    assert(acc->ts.version > 0);
  }
  uint64_t rmw_l_id = acc->t_rmw_id;
  uint8_t acc_m_id = acc_mes->m_id;
  uint32_t log_no = acc->log_no;
  if (DEBUG_RMW) my_printf(green, "Worker %u is handling a remote RMW accept from m_id %u "
                                  "l_id %u, rmw_l_id %u, glob_ses_id %u, log_no %u, version %u  \n",
                           t_id, acc_m_id, acc->l_id, rmw_l_id, (uint32_t) rmw_l_id % GLOBAL_SESSION_NUM, log_no, acc->ts.version);
}

static inline void print_log_on_rmw_recv(uint64_t rmw_l_id,
                                         uint8_t acc_m_id,
                                         uint32_t log_no,
                                         cp_rmw_rep_t *acc_rep,
                                         netw_ts_tuple_t ts,
                                         mica_op_t *kv_ptr,
                                         uint64_t number_of_reqs,
                                         bool is_accept,
                                         uint16_t t_id)
{
  if (PRINT_LOGS)
    fprintf(rmw_verify_fp[t_id], "Key: %u, log %u: Req %lu, %s: m_id:%u, "
                                 "rmw_id %lu, glob_sess id: %u, "
                                 "version %u, m_id: %u, resp: %u \n",
            kv_ptr->key.bkt, log_no, number_of_reqs,
            is_accept ? "Acc" : "Prop",
            acc_m_id, rmw_l_id,
            (uint32_t) rmw_l_id % GLOBAL_SESSION_NUM,
            ts.version, ts.m_id, acc_rep->opcode);



}




// Returns true if the incoming key and the entry key are equal
static inline bool check_entry_validity_with_key(struct key *incoming_key, mica_op_t * kv_ptr)
{
  if (ENABLE_ASSERTIONS) {
    struct key *entry_key = &kv_ptr->key;
    return keys_are_equal(incoming_key, entry_key);
  }
  return true;
}


// When polling acks: more precisely when inspecting each l_id acked
static inline void  cp_check_ack_and_print(context_t *ctx,
                                           cp_com_rob_t *com_rob,
                                           ctx_ack_mes_t *ack,
                                           uint16_t ack_i,
                                           uint32_t ack_ptr,
                                           uint16_t  ack_num)
{
  if (ENABLE_ASSERTIONS) {
    cp_ctx_t* cp_ctx = (cp_ctx_t*) ctx->appl_ctx;
    per_qp_meta_t *qp_meta = &ctx->qp_meta[ACK_QP_ID];
    uint64_t pull_lid = cp_ctx->l_ids.applied_com_id;
    if (ENABLE_ASSERTIONS && (ack_ptr == cp_ctx->com_rob->push_ptr)) {
      uint32_t origin_ack_ptr = (uint32_t) (ack_ptr - ack_i + COM_ROB_SIZE) % COM_ROB_SIZE;
      my_printf(red, "Origin ack_ptr %u/%u, acks %u/%u, pull_ptr %u, push_ptr % u, capacity %u \n",
                origin_ack_ptr, (cp_ctx->com_rob->pull_ptr + (ack->l_id - pull_lid)) % COM_ROB_SIZE,
                ack_i, ack_num, cp_ctx->com_rob->pull_ptr, cp_ctx->com_rob->push_ptr, cp_ctx->com_rob->capacity);
    }

    assert((com_rob->l_id % cp_ctx->com_rob->max_size) == ack_ptr);
    if (com_rob->acks_seen == REMOTE_QUORUM) {
      qp_meta->outstanding_messages--;
      assert(com_rob->state == SENT_COMMIT);
      if (DEBUG_ACKS)
        printf("Worker %d, sess %u: valid ack %u/%u com at ptr %d is ready \n",
               ctx->t_id, com_rob->sess_id, ack_i, ack_num,  ack_ptr);
    }
  }
}

// Check the key of the trace_op and the KVS
static inline void check_trace_op_key_vs_kv_ptr(trace_op_t* op, mica_op_t* kv_ptr)
{
  if (ENABLE_ASSERTIONS) {
    struct key *op_key = &op->key;
    struct key *kv_key = &kv_ptr->key;
    if (!keys_are_equal(kv_key, op_key)) {
      print_key(kv_key);
      print_key(op_key);
      assert(false);
    }
  }
}

// Check the key of the cache_op and  the KV
static inline void check_keys_with_one_trace_op(struct key *com_key, mica_op_t *kv_ptr)
{
  if (ENABLE_ASSERTIONS) {
    struct key *kv_key = &kv_ptr->key;
    if (!keys_are_equal(kv_key, com_key)) {
      print_key(kv_key);
      print_key(com_key);
      assert(false);
    }
  }
}



// Check that the counter for propose replies add up(SAME FOR ACCEPTS AND PROPS)
static inline void check_sum_of_reps(loc_entry_t *loc_entry)
{
  if (ENABLE_ASSERTIONS) {
    assert(loc_entry->rmw_reps.tot_replies == sum_of_reps(&loc_entry->rmw_reps));
    assert(loc_entry->rmw_reps.tot_replies <= MACHINE_NUM);
  }
}



static inline void check_loc_entry_metadata_is_reset(loc_entry_t* loc_entry,
                                                     const char *message,
                                                     uint16_t t_id)
{
  if (loc_entry->helping_flag != PROPOSE_NOT_LOCALLY_ACKED &&
      loc_entry->helping_flag != PROPOSE_LOCALLY_ACCEPTED) {
    if (ENABLE_ASSERTIONS) { // make sure the loc_entry is correctly set-up
      if (loc_entry->help_loc_entry == NULL) {
        //my_printf(red, "The help_loc_ptr is NULL. The reason is typically that help_loc_entry was passed to the function "
        //           "instead of loc entry to check \n");
        assert(loc_entry->state == INVALID_RMW);
      } else {
        if (loc_entry->help_loc_entry->state != INVALID_RMW) {
          my_printf(red, "Wrkr %u: %s \n", t_id, message);
          assert(false);
        }
        assert(loc_entry->rmw_reps.tot_replies == 1);
        assert(loc_entry->back_off_cntr == 0);
      }
    }
  }
}


// When going to ack an accept/propose because the log it refers to is higher than what we are working on
static inline void check_that_log_is_high_enough(mica_op_t *kv_ptr, uint32_t log_no)
{
  if (ENABLE_ASSERTIONS) {
    assert(log_no > kv_ptr->last_committed_log_no);
    if (log_no == kv_ptr->last_committed_log_no + 1) {
      if (kv_ptr->state != INVALID_RMW) {
        my_printf(red, "Checking_that_log_is to high: log_no %u/%u, kv_ptr committed last_log %u, state %u \n",
                  log_no, kv_ptr->log_no, kv_ptr->last_committed_log_no, kv_ptr->state);
        assert(false);
      }
    } else if (kv_ptr->state != INVALID_RMW)
      assert(kv_ptr->last_committed_log_no + 1);
  }
}

//
static inline void check_log_nos_of_kv_ptr(mica_op_t *kv_ptr, const char *message, uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    if (kv_ptr->state != INVALID_RMW) {
      if (kv_ptr->last_committed_rmw_id.id == kv_ptr->rmw_id.id) {
        my_printf(red, "Wrkr %u Last committed rmw id is equal to current, kv_ptr state %u, com log/log %u/%u "
                    "rmw id %u/%u,  %s \n",
                  t_id, kv_ptr->state, kv_ptr->last_committed_log_no, kv_ptr->log_no,
                  kv_ptr->last_committed_rmw_id.id, kv_ptr->rmw_id.id,
                   message);
        assert(false);
      }

      if (kv_ptr->last_committed_log_no >= kv_ptr->log_no) {
        my_printf(red, "Wrkr %u t_id, kv_ptr state %u, com log/log %u/%u : %s \n",
                  t_id, kv_ptr->state, kv_ptr->last_committed_log_no, kv_ptr->log_no, message);
        assert(false);
      }
    }
  }
}

//
static inline void check_for_same_ts_as_already_proposed(mica_op_t *kv_ptr, struct propose *prop, uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    if (kv_ptr->state == PROPOSED) {
      if (compare_netw_ts_with_ts(&prop->ts, &kv_ptr->prop_ts) == EQUAL) {
        my_printf(red, "Wrkr %u Received a proposal with same TS as an already acked proposal, "
                    " prop log/kv_ptr log %u/%u, , rmw_id %u/%u, version %u/%u, m_id %u/%u \n",
                  t_id, prop->log_no, kv_ptr->log_no,
                  prop->t_rmw_id, kv_ptr->rmw_id.id,
                  prop->ts.version, kv_ptr->prop_ts.version, prop->ts.m_id, kv_ptr->prop_ts.m_id);
        assert(false);
      }
    }
  }
}

static inline void verify_paxos(loc_entry_t *loc_entry, uint16_t t_id)
{
  if (VERIFY_PAXOS && is_global_ses_id_local((uint32_t)loc_entry->rmw_id.id % GLOBAL_SESSION_NUM, t_id)) {
    //if (committed_log_no != *(uint32_t *)loc_entry->value_to_write)
    //  red_printf ("vale_to write/log no %u/%u",
    //             *(uint32_t *)loc_entry->value_to_write, committed_log_no );
    uint64_t val = *(uint64_t *)loc_entry->value_to_read;
    //assert(val == loc_entry->accepted_log_no - 1);
    fprintf(rmw_verify_fp[t_id], "%u %lu %u \n", loc_entry->key.bkt, val, loc_entry->accepted_log_no);
  }
}


static inline void check_last_registered_rmw_id(loc_entry_t *loc_entry,
                                                mica_op_t *kv_ptr, uint8_t helping_flag, uint16_t t_id)
{
//  if (ENABLE_ASSERTIONS) {
//    if (kv_ptr->last_registered_log_no != loc_entry->log_no - 1) {
//      my_printf(red, "Last registered/last-committed/working  %u/%u/%u, key %u, helping flag %u \n",
//                kv_ptr->last_registered_log_no, kv_ptr->last_committed_log_no,
//                loc_entry->log_no, loc_entry->key.bkt, helping_flag);
//      sleep(2);
//      assert(false);
//    }
//    if (loc_entry->log_no == kv_ptr->last_committed_log_no + 1) {
//      if (!rmw_ids_are_equal(&kv_ptr->last_registered_rmw_id, &kv_ptr->last_committed_rmw_id)) {
//        my_printf(red,
//                  "Wrkr %u, filling help loc entry last registered rmw id, help log no/ kv_ptr last committed log no %u/%u,"
//                    "glob rmw ids: last committed/last registered %lu/%lu \n", t_id,
//                  loc_entry->log_no, kv_ptr->last_committed_log_no,
//                  kv_ptr->last_registered_rmw_id.id, kv_ptr->last_committed_rmw_id.id);
//      }
//      assert(rmw_ids_are_equal(&kv_ptr->last_registered_rmw_id, &kv_ptr->last_committed_rmw_id));
//    }
//      // If I am helping log_no X, without having committed log_no X-1, the i better have the correct last registered RMW-id
//    else if (loc_entry->log_no > kv_ptr->last_committed_log_no + 1) {
//      assert(!rmw_ids_are_equal(&kv_ptr->last_registered_rmw_id, &kv_ptr->last_committed_rmw_id));
//    } else
//      assert(false);
//  }
}


static inline void check_that_the_rmw_ids_match(mica_op_t *kv_ptr, uint64_t rmw_id,
                                                 uint32_t log_no, uint32_t version,
                                                uint8_t m_id, const char *message, uint16_t t_id)
{
  uint64_t glob_sess_id = rmw_id % GLOBAL_SESSION_NUM;
  if (kv_ptr->last_committed_rmw_id.id != rmw_id) {
    my_printf(red, "~~~~~~~~COMMIT MISSMATCH Worker %u key: %u, Log %u %s ~~~~~~~~ \n", t_id, kv_ptr->key.bkt, log_no, message);
    my_printf(green, "GLOBAL ENTRY COMMITTED log %u: rmw_id %lu glob_sess-id- %u\n",
              kv_ptr->last_committed_log_no, kv_ptr->last_committed_rmw_id.id,
              kv_ptr->last_committed_rmw_id.id % GLOBAL_SESSION_NUM);
    my_printf(yellow, "COMMIT log %u: rmw_id %lu glob_sess-id-%u version %u m_id %u \n",
              log_no, rmw_id, glob_sess_id, version, m_id);
    /*if (ENABLE_DEBUG_RMW_KV_PTR) {
      my_printf(green, "GLOBAL ENTRY COMMITTED log %u: rmw_id %lu glob_sess-id- %u, FLAG %u\n",
                   kv_ptr->last_committed_log_no, kv_ptr->last_committed_rmw_id.id,
                   kv_ptr->last_committed_rmw_id.glob_sess_id, kv_ptr->dbg->last_committed_flag);
      my_printf(yellow, "COMMIT log %u: rmw_id %lu glob_sess-id-%u version %u m_id %u \n",
                    log_no, rmw_id, glob_sess_id, version, m_id);
      if (kv_ptr->dbg->last_committed_flag <= 1) {
        my_printf(cyan, "PROPOSED log %u: rmw_id %lu glob_sess-id-%u version %u m_id %u \n",
                    kv_ptr->dbg->proposed_log_no, kv_ptr->dbg->proposed_rmw_id.id,
                    kv_ptr->dbg->proposed_rmw_id.glob_sess_id,
                    kv_ptr->dbg->proposed_ts.version, kv_ptr->dbg->proposed_ts.m_id);


        my_printf(cyan, "LAST COMMIT log %u: rmw_id %lu glob_sess-id-%u version %u m_id %u \n",
                    kv_ptr->dbg->last_committed_log_no, kv_ptr->dbg->last_committed_rmw_id.id,
                    kv_ptr->dbg->last_committed_rmw_id.glob_sess_id,
                    kv_ptr->dbg->last_committed_ts.version, kv_ptr->dbg->last_committed_ts.m_id);

      }
    }*/
    assert(false);
  }
}


// After registering, make sure the registered is bigger/equal to what is saved as registered
static inline void check_registered_against_kv_ptr_last_committed(mica_op_t *kv_ptr,
                                                                  uint64_t committed_id,
                                                                  const char *message, uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    uint32_t committed_glob_ses_id = (uint32_t)(committed_id % GLOBAL_SESSION_NUM);
    uint32_t glob_sess_id = (uint32_t)(kv_ptr->last_committed_rmw_id.id % GLOBAL_SESSION_NUM);
    uint64_t id = kv_ptr->last_committed_rmw_id.id;
    assert(glob_sess_id < GLOBAL_SESSION_NUM);
    if (committed_glob_sess_rmw_id[glob_sess_id] < id) {
      my_printf(yellow, "Committing %s rmw_id: %u glob_sess_id: %u \n", message, committed_id, committed_glob_ses_id);
      my_printf(red, "Wrkr %u: %s rmw_id: kv_ptr last committed %lu, "
                  "glob_sess_id :kv_ptr last committed %u,"
                  "committed_glob_sess_rmw_id %lu,   \n", t_id, message,
                kv_ptr->last_committed_rmw_id.id,
                glob_sess_id,
                committed_glob_sess_rmw_id[glob_sess_id]);
      //assert(false);
    }
  }
}

// Perofrm checks after receiving a rep to commit an RMW
static inline void check_local_commit_from_rep(mica_op_t *kv_ptr, loc_entry_t *loc_entry,
                                               struct rmw_rep_last_committed *rmw_rep, uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    if (kv_ptr->state != INVALID_RMW) {
      loc_entry_t *working_entry = loc_entry->helping_flag == HELPING ?
                                              loc_entry->help_loc_entry : loc_entry;
      if (kv_ptr->rmw_id.id == working_entry->rmw_id.id &&
          kv_ptr->log_no == working_entry->log_no) {
        my_printf(red, "Wrkr: %u Received a rep opcode %u for rmw id %lu , log no %u "
                    "received highest committed log %u with rmw_id id %u, "
                    "but kv_ptr is in state %u, for rmw_id %u, on log no %u\n",
                  t_id, rmw_rep->opcode, working_entry->rmw_id.id,
                  working_entry->log_no,
                  rmw_rep->log_no_or_base_version, rmw_rep->rmw_id,
                  kv_ptr->state, kv_ptr->rmw_id.id,
                  kv_ptr->log_no);
        assert(rmw_rep->opcode == RMW_ID_COMMITTED);
      }
      assert(kv_ptr->rmw_id.id !=rmw_rep->rmw_id);
    }
  }
}

static inline void check_when_polling_for_props(context_t* ctx,
                                                cp_prop_mes_t *prop_mes)
{
  uint8_t coalesce_num = prop_mes->coalesce_num;
  per_qp_meta_t *qp_meta = &ctx->qp_meta[PROP_QP_ID];
  fifo_t *recv_fifo = qp_meta->recv_fifo;
  if (ENABLE_ASSERTIONS) {
    //struct prop_message *p_mes = (struct prop_message *)r_mes;
    cp_prop_t *prop = &prop_mes->prop[0];
    assert(coalesce_num > 0);
   if (DEBUG_RMW) {
      my_printf(cyan, "Worker %u sees a Propose "
                      "from m_id %u: opcode %d at offset %d, rmw_id %lu, "
                  "log_no %u, coalesce_num %u version %u \n",
                ctx->t_id, prop_mes->m_id,
                prop->opcode,
                recv_fifo->pull_ptr,
                prop->t_rmw_id,
                prop->log_no,
                coalesce_num,
                prop->ts.version);
      assert(prop_mes->m_id != machine_id);

    }
    if (qp_meta->polled_messages + coalesce_num > MAX_INCOMING_PROP) assert(false);
  }
  if (ENABLE_STAT_COUNTING) {
    if (ENABLE_ASSERTIONS)
      t_stats[ctx->t_id].per_worker_props_received[prop_mes->m_id] += coalesce_num;
    t_stats[ctx->t_id].received_props += coalesce_num;
    t_stats[ctx->t_id].received_props_mes_num++;
  }
}

static inline void check_when_polling_for_accs(context_t* ctx,
                                                cp_acc_mes_t *acc_mes)
{
  uint8_t coalesce_num = acc_mes->coalesce_num;
  per_qp_meta_t *qp_meta = &ctx->qp_meta[ACC_QP_ID];
  fifo_t *recv_fifo = qp_meta->recv_fifo;
  if (ENABLE_ASSERTIONS) {
    //struct acc_message *p_mes = (struct acc_message *)r_mes;
    cp_acc_t *acc = &acc_mes->acc[0];
    assert(coalesce_num > 0);
    if (DEBUG_RMW) {
      my_printf(cyan, "Worker %u sees an Accept "
                      "from m_id %u: opcode %d at offset %d, rmw_id %lu, "
                      "log_no %u, coalesce_num %u version %u \n",
                ctx->t_id, acc_mes->m_id,
                acc->opcode,
                recv_fifo->pull_ptr,
                acc->t_rmw_id,
                acc->log_no,
                coalesce_num,
                acc->ts.version);
      assert(acc_mes->m_id != machine_id);

    }
    if (qp_meta->polled_messages + coalesce_num > MAX_INCOMING_ACC) assert(false);
  }
  if (ENABLE_STAT_COUNTING) {
    //t_stats[ctx->t_id].received_reads += coalesce_num;
    //t_stats[ctx->t_id].received_reads_mes_num++;
  }
}


static inline void check_when_polling_for_coms(context_t* ctx,
                                               cp_com_mes_t *com_mes)
{
  uint8_t coalesce_num = com_mes->coalesce_num;
  per_qp_meta_t *qp_meta = &ctx->qp_meta[COM_QP_ID];
  fifo_t *recv_fifo = qp_meta->recv_fifo;
  if (ENABLE_ASSERTIONS) {
    cp_com_t *com = &com_mes->com[0];
    assert(coalesce_num > 0);
    if (DEBUG_RMW) {
      my_printf(cyan, "Worker %u sees a Commit "
                      "from m_id %u: opcode %d at offset %d, rmw_id %lu, "
                      "log_no %u, coalesce_num %u version %u \n",
                ctx->t_id, com_mes->m_id,
                com->opcode,
                recv_fifo->pull_ptr,
                com->t_rmw_id,
                com->log_no,
                coalesce_num,
                com->base_ts.version);
      assert(com_mes->m_id != machine_id);

    }
    if (qp_meta->polled_messages + coalesce_num > MAX_INCOMING_COM) assert(false);
  }
  if (ENABLE_STAT_COUNTING) {
    //t_stats[ctx->t_id].received_reads += coalesce_num;
    //t_stats[ctx->t_id].received_reads_mes_num++;
  }
}


static inline void print_on_remote_com(cp_com_t *com,
                                       uint16_t op_i,
                                       uint16_t t_id)
{
  if (DEBUG_RMW)
    my_printf(green, "Worker %u is handling a remote "
                     "RMW commit on com %u, "
                     "rmw_l_id %u, glob_ses_id %u, "
                     "log_no %u, version %u  \n",
              t_id, op_i, com->t_rmw_id,
              com->t_rmw_id % GLOBAL_SESSION_NUM,
              com->log_no, com->base_ts.version);
}

static inline void print_log_remote_com(cp_com_t *com,
                                        cp_com_mes_t *com_mes,
                                        mica_op_t *kv_ptr,
                                        uint16_t t_id)
{
  if (PRINT_LOGS) {
    uint8_t acc_m_id = com_mes->m_id;
    uint64_t number_of_rquests = 0;
    fprintf(rmw_verify_fp[t_id], "Key: %u, log %u: Req %lu, Com: m_id:%u, rmw_id %lu, glob_sess id: %u, "
                                 "version %u, m_id: %u \n",
            kv_ptr->key.bkt, com->log_no, number_of_rquests, acc_m_id, com->t_rmw_id,
            (uint32_t) (com->t_rmw_id % GLOBAL_SESSION_NUM), com->base_ts.version, com->base_ts.m_id);
  }
}



static inline void debug_fail_help(loc_entry_t *loc_entry, const char *message, uint16_t t_id)
{
  if (DEBUG_RMW) {
    if (loc_entry->helping_flag == PROPOSE_NOT_LOCALLY_ACKED && t_id == 0)
      my_printf(cyan, "Sess %u %s \n", loc_entry->sess_id, message);
  }
}

// When stealing kv_ptr from a stuck proposal, check that the proposal was referring to a valid log no
static inline void check_the_proposed_log_no(mica_op_t *kv_ptr, loc_entry_t *loc_entry,
                                             uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    if (kv_ptr->log_no > kv_ptr->last_committed_log_no + 1) {
      my_printf(red, "Key %u Last committed//accepted/active %u/%u/%u \n", loc_entry->key.bkt,
                kv_ptr->last_committed_log_no,
                kv_ptr->accepted_log_no,
                kv_ptr->log_no);
      assert(false);
    }
  }
}


static inline void check_when_rmw_has_committed(mica_op_t *kv_ptr,
                                                struct rmw_rep_last_committed *rep,
                                                uint64_t glob_sess_id,
                                                uint32_t log_no,
                                                uint64_t rmw_id,
                                                uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    //if (DEBUG_RMW)
    my_printf(green, "Worker %u: Remote machine  global sess_id %u is trying a propose/accept, \n"
                     "kv_ptr/ prop \n"
                     "log_no: %u/%u \n"
                     "rmw_id %lu/%lu \n It has been already committed, "
                     "because for this global_sess, we have seen rmw_id %lu\n",
              t_id, glob_sess_id,
              kv_ptr->last_committed_log_no, log_no,
              kv_ptr->last_committed_rmw_id.id, rmw_id,
              committed_glob_sess_rmw_id[glob_sess_id]);

    //for (uint64_t i = 0; i < GLOBAL_SESSION_NUM; i++)
    //  printf("Glob sess num %lu: %lu \n", i, committed_glob_sess_rmw_id[i]);
    assert(rep->opcode == RMW_ID_COMMITTED_SAME_LOG || RMW_ID_COMMITTED);
    assert(kv_ptr->last_committed_log_no > 0);
    if (rep->opcode == RMW_ID_COMMITTED_SAME_LOG) {
      assert(kv_ptr->last_committed_log_no == log_no);
      assert(kv_ptr->last_committed_rmw_id.id == rmw_id);
      printf("assert was okay \n");
    }
  }

}







static inline void checks_when_handling_prop_acc_rep(loc_entry_t *loc_entry,
                                                     struct rmw_rep_last_committed *rep,
                                                     bool is_accept, uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    rmw_rep_info_t *rep_info = &loc_entry->rmw_reps;
    assert(rep_info->tot_replies > 0);
    if (is_accept) assert(loc_entry->state == ACCEPTED);
    else {
      assert(loc_entry->state == PROPOSED);
      // this checks that the performance optimization of NO-op reps is valid
      assert(rep->opcode != NO_OP_PROP_REP);
      check_state_with_allowed_flags(4, loc_entry->helping_flag, NOT_HELPING,
                                     PROPOSE_NOT_LOCALLY_ACKED, PROPOSE_LOCALLY_ACCEPTED);
      if (loc_entry->helping_flag == PROPOSE_LOCALLY_ACCEPTED ||
          loc_entry->helping_flag == PROPOSE_NOT_LOCALLY_ACKED)
        assert(rep_info->already_accepted > 0);
    }
  }
}
/*--------------------------------------------------------------------------
 * --------------------ACCEPTING-------------------------------------
 * --------------------------------------------------------------------------*/

static inline void checks_preliminary_local_accept(mica_op_t *kv_ptr,
                                                   loc_entry_t *loc_entry,
                                                   uint16_t t_id)
{
  my_assert(keys_are_equal(&loc_entry->key, &kv_ptr->key),
            "Attempt local accept: Local entry does not contain the same key as kv_ptr");

  if (ENABLE_ASSERTIONS) assert(loc_entry->glob_sess_id < GLOBAL_SESSION_NUM);
}

static inline void checks_before_local_accept(mica_op_t *kv_ptr,
                                              loc_entry_t *loc_entry,
                                              uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    //assert(compare_ts(&loc_entry->new_ts, &kv_ptr->prop_ts) == EQUAL);
    assert(kv_ptr->log_no == loc_entry->log_no);
    assert(kv_ptr->last_committed_log_no == loc_entry->log_no - 1);
  }

  if (DEBUG_RMW)
    my_printf(green, "Wrkr %u got rmw id %u, accepted locally \n",
              t_id, loc_entry->rmw_id.id);
}


static inline void checks_after_local_accept(mica_op_t *kv_ptr,
                                              loc_entry_t *loc_entry,
                                              uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    assert(loc_entry->accepted_log_no == loc_entry->log_no);
    assert(loc_entry->log_no == kv_ptr->last_committed_log_no + 1);
    assert(compare_ts(&kv_ptr->prop_ts, &kv_ptr->accepted_ts) != SMALLER);
    kv_ptr->accepted_rmw_id = kv_ptr->rmw_id;
  }
  if (ENABLE_DEBUG_RMW_KV_PTR) {
    //kv_ptr->dbg->proposed_ts = loc_entry->new_ts;
    //kv_ptr->dbg->proposed_log_no = loc_entry->log_no;
    //kv_ptr->dbg->proposed_rmw_id = loc_entry->rmw_id;
  }
  check_log_nos_of_kv_ptr(kv_ptr, "attempt_local_accept and succeed", t_id);
}


static inline void checks_after_failure_to_locally_accept(mica_op_t *kv_ptr,
                                                          loc_entry_t *loc_entry,
                                                          uint16_t t_id)
{
  if (DEBUG_RMW)
    my_printf(green, "Wrkr %u failed to get rmw id %u, accepted locally "
                "kv_ptr rmw id %u, state %u \n",
              t_id, loc_entry->rmw_id.id,
              kv_ptr->rmw_id.id, kv_ptr->state);
  // --CHECKS--
  if (ENABLE_ASSERTIONS) {
    if (kv_ptr->state == PROPOSED || kv_ptr->state == ACCEPTED) {
      if(!(compare_ts(&kv_ptr->prop_ts, &loc_entry->new_ts) == GREATER ||
           kv_ptr->log_no > loc_entry->log_no)) {
        my_printf(red, "State: %s,  loc-entry-helping %d, Kv prop/base_ts %u/%u -- loc-entry base_ts %u/%u, "
                    "kv-log/loc-log %u/%u kv-rmw_id/loc-rmw-id %u/%u\n",
                  kv_ptr->state == ACCEPTED ? "ACCEPTED" : "PROPOSED",
                  loc_entry->helping_flag,
                  kv_ptr->prop_ts.version, kv_ptr->prop_ts.m_id,
                  loc_entry->new_ts.version, loc_entry->new_ts.m_id,
                  kv_ptr->log_no, loc_entry->log_no,
                  kv_ptr->rmw_id.id, loc_entry->rmw_id.id);
        assert(false);
      }
    }
    else if (kv_ptr->state == INVALID_RMW) // some other rmw committed
      // with cancelling it is possible for some other RMW to stole and then cancelled itself
      if (!ENABLE_CAS_CANCELLING) assert(kv_ptr->last_committed_log_no >= loc_entry->log_no);
  }


  check_log_nos_of_kv_ptr(kv_ptr, "attempt_local_accept and fail", t_id);
}


static inline void checks_acting_on_quorum_of_prop_ack(loc_entry_t *loc_entry, uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    if (loc_entry->helping_flag == PROPOSE_LOCALLY_ACCEPTED) {
      assert(loc_entry->rmw_reps.tot_replies >= QUORUM_NUM);
      assert(loc_entry->rmw_reps.already_accepted >= 0);
      assert(loc_entry->rmw_reps.seen_higher_prop_acc == 0);
      assert(glob_ses_id_to_t_id((uint32_t) (loc_entry->rmw_id.id % GLOBAL_SESSION_NUM)) == t_id &&
             glob_ses_id_to_m_id((uint32_t) (loc_entry->rmw_id.id % GLOBAL_SESSION_NUM)) == machine_id);

    }
  }
}

static inline void checks_preliminary_local_accept_help(mica_op_t *kv_ptr,
                                                        loc_entry_t *loc_entry,
                                                        loc_entry_t *help_loc_entry)
{
  if (ENABLE_ASSERTIONS) {
    my_assert(keys_are_equal(&help_loc_entry->key, &kv_ptr->key),
              "Attempt local accpet to help: Local entry does not contain the same key as kv_ptr");
    my_assert(loc_entry->help_loc_entry->log_no == loc_entry->log_no,
              " the help entry and the regular have not the same log nos");
    assert(help_loc_entry->glob_sess_id < GLOBAL_SESSION_NUM);
    assert(loc_entry->log_no == help_loc_entry->log_no);
  }
}

static inline void checks_and_prints_local_accept_help(loc_entry_t *loc_entry,
                                                       loc_entry_t* help_loc_entry,
                                                       mica_op_t *kv_ptr, bool kv_ptr_is_the_same,
                                                       bool kv_ptr_is_invalid_but_not_committed,
                                                       bool helping_stuck_accept,
                                                       bool propose_locally_accepted,
                                                       uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    assert(compare_ts(&kv_ptr->prop_ts, &help_loc_entry->new_ts) != SMALLER);
    assert(kv_ptr->last_committed_log_no == help_loc_entry->log_no - 1);
    if (kv_ptr_is_invalid_but_not_committed) {
      printf("last com/log/help-log/loc-log %u/%u/%u/%u \n",
             kv_ptr->last_committed_log_no, kv_ptr->log_no,
             help_loc_entry->log_no, loc_entry->log_no);
      assert(false);
    }
    // if the TS are equal it better be that it is because it remembers the proposed request
    if (kv_ptr->state != INVALID_RMW &&
        compare_ts(&kv_ptr->prop_ts, &loc_entry->new_ts) == EQUAL && !helping_stuck_accept &&
        !helping_stuck_accept && !propose_locally_accepted) {
      assert(kv_ptr->rmw_id.id == loc_entry->rmw_id.id);
      if (kv_ptr->state != PROPOSED) {
        my_printf(red, "Wrkr: %u, state %u \n", t_id, kv_ptr->state);
        assert(false);
      }
    }
    if (propose_locally_accepted)
      assert(compare_ts(&help_loc_entry->new_ts, &kv_ptr->accepted_ts) == GREATER);
  }
  if (DEBUG_RMW)
    my_printf(green, "Wrkr %u on attempting to locally accept to help "
                "got rmw id %u, accepted locally \n",
              t_id, help_loc_entry->rmw_id.id);
}

static inline void checks_after_local_accept_help(mica_op_t *kv_ptr,
                                                  loc_entry_t *loc_entry,
                                                  uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    assert(compare_ts(&kv_ptr->prop_ts, &kv_ptr->accepted_ts) != SMALLER);
    kv_ptr->accepted_rmw_id = kv_ptr->rmw_id;
    check_log_nos_of_kv_ptr(kv_ptr, "attempt_local_accept_to_help and succeed", t_id);
  }
}


static inline void checks_after_failure_to_locally_accept_help(mica_op_t *kv_ptr,
                                                               loc_entry_t *loc_entry,
                                                               uint16_t t_id)
{
  if (DEBUG_RMW)
    my_printf(green, "Wrkr %u sess %u failed to get rmw id %u, accepted locally "
                "kv_ptr rmw id %u, state %u \n",
              t_id, loc_entry->sess_id, loc_entry->rmw_id.id,
              kv_ptr->rmw_id.id, kv_ptr->state);


  check_log_nos_of_kv_ptr(kv_ptr, "attempt_local_accept_to_help and fail", t_id);
}

static inline void checks_acting_on_already_accepted_rep(loc_entry_t *loc_entry, uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    loc_entry_t* help_loc_entry = loc_entry->help_loc_entry;
    assert(loc_entry->log_no == help_loc_entry->log_no);
    assert(loc_entry->help_loc_entry->state == ACCEPTED);
    assert(compare_ts(&help_loc_entry->new_ts, &loc_entry->new_ts) == SMALLER);
  }
}

/*--------------------------------------------------------------------------
 * --------------------COMMITS-------------------------------------
 * --------------------------------------------------------------------------*/
static inline void error_mesage_on_commit_check(mica_op_t *kv_ptr,
                                           commit_info_t *com_info,
                                           const char* message,
                                           uint16_t t_id)
{
my_printf(red, "---Worker %u----- \n"
               "%s \n"
                "Flag: %s \n"
                "kv_ptr / com_info \n"
                "rmw_id  %lu/%lu\n, "
                "log_no %u/%u \n"
                "base ts %u-%u/%u-%u \n",
                t_id, message,
                com_info->message,
                kv_ptr->last_committed_rmw_id.id, com_info->rmw_id.id,
                kv_ptr->last_committed_log_no, com_info->log_no,
                kv_ptr->ts.version, kv_ptr->ts.m_id,
                com_info->base_ts.version, com_info->base_ts.m_id);
}

static inline void check_inputs_commit_algorithm(mica_op_t *kv_ptr,
                                                 commit_info_t *com_info,
                                                 uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    assert(kv_ptr != NULL);
    if (com_info->value == NULL) assert(com_info->no_value);
    if (com_info->log_no == 0) {
      if (com_info->rmw_id.id != 0)
        error_mesage_on_commit_check(kv_ptr, com_info, "Rmw-id is zero but not log-no", t_id);
      assert(com_info->rmw_id.id == 0);
    }
    if (com_info->rmw_id.id == 0) assert(com_info->log_no == 0);
    if (!com_info->overwrite_kv)
      assert(com_info->flag == FROM_LOCAL ||
             com_info->flag == FROM_ALREADY_COMM_REP);
  }
}

static inline void check_on_overwriting_commit_algorithm(mica_op_t *kv_ptr,
                                                         commit_info_t *com_info,
                                                         compare_t cart_comp,
                                                         uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    if (cart_comp == EQUAL) {
      assert(kv_ptr->last_committed_log_no == com_info->log_no);
      if (kv_ptr->last_committed_rmw_id.id != com_info->rmw_id.id)
        error_mesage_on_commit_check(kv_ptr, com_info, "Carts equal, but not rmw-ids", t_id);
      assert(kv_ptr->last_committed_rmw_id.id == com_info->rmw_id.id);
      //assert(memcmp(kv_ptr->value, com_info->value, (size_t) 8) == 0);
    }
    else if (cart_comp == SMALLER) {
      assert(compare_ts(&com_info->base_ts, &kv_ptr->ts) == SMALLER ||
             (compare_ts(&com_info->base_ts, &kv_ptr->ts) == EQUAL &&
               com_info->log_no < kv_ptr->last_committed_log_no));
    }
  }
}

static inline void check_on_updating_rmw_meta_commit_algorithm(mica_op_t *kv_ptr,
                                                               commit_info_t *com_info,
                                                               uint16_t t_id)
{
  if (kv_ptr->last_committed_log_no < com_info->log_no) {
    if (DEBUG_RMW)
      my_printf(green, "Wrkr %u commits locally rmw id %u: %s \n",
                t_id, com_info->rmw_id, com_info->message);
    update_commit_logs(t_id, kv_ptr->key.bkt, com_info->log_no, kv_ptr->value,
                       com_info->value, com_info->message, LOG_COMS);
  }
  else if (kv_ptr->last_committed_log_no == com_info->log_no) {
    check_that_the_rmw_ids_match(kv_ptr,  com_info->rmw_id.id, com_info->log_no,
    com_info->base_ts.version, com_info->base_ts.m_id,
    com_info->message, t_id);
  }
}

static inline void check_state_before_commit_algorithm(mica_op_t *kv_ptr,
                                                       commit_info_t *com_info,
                                                       uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    if (com_info->flag == FROM_LOCAL || com_info->flag == FROM_LOCAL_HELP) {
      // make sure that if we are on the same log
      if (kv_ptr->log_no == com_info->log_no) {
        if (!rmw_ids_are_equal(&com_info->rmw_id, &kv_ptr->rmw_id)) {
          my_printf(red, "kv_ptr is on same log as what is about to be committed but on different rmw-id \n");
          print_commit_info(com_info, yellow, t_id);
          print_kv_ptr(kv_ptr, cyan, t_id);
         // this is a hard error
          assert(false);
        }
        if (kv_ptr->state != INVALID_RMW) {
          if (kv_ptr->state != ACCEPTED) {
            my_printf(red, "Committing: Logs are equal, rmw-ids are equal "
              "but state is not accepted \n");
            print_commit_info(com_info, yellow, t_id);
            print_kv_ptr(kv_ptr, cyan, t_id);
            assert(false);
          }
        }
      }
      else {
        // if the log has moved on then the RMW has been helped,
        // it has been committed in the other machines so there is no need to change its state
        check_log_nos_of_kv_ptr(kv_ptr, "commit_helped_or_local_from_loc_entry", t_id);
        if (ENABLE_ASSERTIONS) {
          if (kv_ptr->state != INVALID_RMW)
            assert(!rmw_ids_are_equal(&kv_ptr->rmw_id, &com_info->rmw_id));
        }
      }
    }
    else if (com_info->flag == FROM_REMOTE_COMMIT_NO_VAL) {
      if (kv_ptr->last_committed_log_no < com_info->log_no) {
        if (ENABLE_ASSERTIONS) {
          assert(kv_ptr->state == ACCEPTED);
          assert(kv_ptr->log_no == com_info->log_no);
          assert(kv_ptr->accepted_rmw_id.id == com_info->rmw_id.id);
        }
      }
    }



  }
}

//------------------------------HELP STUCK RMW------------------------------------------

static inline void
checks_and_prints_proposed_but_not_locally_acked(cp_ctx_t *cp_ctx,
                                                 mica_op_t *kv_ptr,
                                                 loc_entry_t * loc_entry,
                                                 uint16_t t_id)
{
  if (DEBUG_RMW)
    my_printf(cyan, "Wrkr %u, session %u helps RMW id %u with version %u, m_id %u,"
                " kv_ptr log/help log %u/%u kv_ptr committed log %u , "
                " stashed rmw_id: %u state %u \n",
              t_id, loc_entry->sess_id, loc_entry->rmw_id.id,
              loc_entry->new_ts.version, loc_entry->new_ts.m_id,
              kv_ptr->log_no, loc_entry->log_no, kv_ptr->last_committed_log_no,
              loc_entry->help_rmw->rmw_id.id, loc_entry->help_rmw->state);

  if (ENABLE_ASSERTIONS) {
    assert(cp_ctx->stall_info.stalled);
    assert(loc_entry->rmw_reps.tot_replies == 0);
  }
}

static inline void logging_proposed_but_not_locally_acked(mica_op_t *kv_ptr,
                                                          loc_entry_t * loc_entry,
                                                          loc_entry_t *help_loc_entry,
                                                          uint16_t t_id)
{
  if (PRINT_LOGS && ENABLE_DEBUG_RMW_KV_PTR)
    fprintf(rmw_verify_fp[t_id], "Key: %u, log %u: Prop-not-locally accepted: helping rmw_id %lu, "
              "version %u, m_id: %u, From: rmw_id %lu, with version %u, m_id: %u \n",
            loc_entry->key.bkt, loc_entry->log_no, help_loc_entry->rmw_id.id,
            help_loc_entry->new_ts.version, help_loc_entry->new_ts.m_id, loc_entry->rmw_id.id,
            loc_entry->new_ts.version, loc_entry->new_ts.m_id);
}


static inline void checks_init_attempt_to_grab_kv_ptr(loc_entry_t * loc_entry,
                                                      uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    assert(loc_entry->rmw_id.id % GLOBAL_SESSION_NUM == loc_entry->glob_sess_id);
    assert(!loc_entry->rmw_reps.ready_to_inspect);
    assert(loc_entry->rmw_reps.tot_replies == 0);
  }
}



static inline void print_when_grabbing_kv_ptr(loc_entry_t * loc_entry,
                                              uint16_t t_id)
{
  if (DEBUG_RMW)
    my_printf(yellow, "Wrkr %u, after waiting for %u cycles, session %u  \n",
              t_id, loc_entry->back_off_cntr, loc_entry->sess_id);
}


static inline void print_when_state_changed_not_grabbing_kv_ptr(mica_op_t *kv_ptr,
                                                                loc_entry_t * loc_entry,
                                                                uint16_t t_id)
{
  if (DEBUG_RMW)
    my_printf(yellow, "Wrkr %u, session %u changed who is waiting: waited for %u cycles on "
                "state %u rmw_id %u , now waiting on rmw_id %u , state %u\n",
              t_id, loc_entry->sess_id, loc_entry->back_off_cntr,
              loc_entry->help_rmw->state, loc_entry->help_rmw->rmw_id.id,
              kv_ptr->rmw_id.id, kv_ptr->state);
}


static inline void check_and_print_when_rmw_fails(mica_op_t *kv_ptr,
                                                  loc_entry_t * loc_entry,
                                                  uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    assert(loc_entry->accepted_log_no == 0);
    assert(loc_entry->killable);
    assert(ENABLE_CAS_CANCELLING);
  }
  if (DEBUG_RMW)
    printf("Cancelling on needing kv_ptr Wrkr%u, sess %u, rmw_failing \n",
           t_id, loc_entry->sess_id);

}


static inline void checks_attempt_to_help_locally_accepted(mica_op_t *kv_ptr,
                                                           loc_entry_t * loc_entry,
                                                           uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    assert(kv_ptr->accepted_log_no == kv_ptr->log_no);
    assert(kv_ptr->prop_ts.version > kv_ptr->accepted_ts.version);
    assert(rmw_ids_are_equal(&kv_ptr->rmw_id, &kv_ptr->accepted_rmw_id));
    assert(loc_entry->key.bkt == kv_ptr->key.bkt);
    assert(kv_ptr->state == ACCEPTED);
  }
}

static inline void print_when_state_changed_steal_proposed(mica_op_t *kv_ptr,
                                                           loc_entry_t *loc_entry,
                                                           uint16_t t_id)
{
  if (DEBUG_RMW)
    my_printf(yellow, "Wrkr %u, session %u on attempting to steal the propose, changed who is "
                "waiting: waited for %u cycles for state %u "
                "rmw_id %u  state %u,  now waiting on rmw_id % , state %u\n",
              t_id, loc_entry->sess_id, loc_entry->back_off_cntr,
              loc_entry->help_rmw->state, loc_entry->help_rmw->rmw_id.id,
              kv_ptr->rmw_id.id, kv_ptr->state);
}


static inline void print_after_stealing_proposed(mica_op_t *kv_ptr,
                                                 loc_entry_t * loc_entry,
                                                 uint16_t t_id)
{
  if (DEBUG_RMW)
    my_printf(cyan, "Wrkr %u: session %u steals kv_ptr to do its propose \n",
              t_id, loc_entry->sess_id);
}


//-------------------------SENDING ACKS
// This is to be called from within odyssey
static inline void checks_stats_prints_when_sending_acks(ctx_ack_mes_t *acks,
                                                         uint8_t m_i, uint16_t t_id)
{
  if (ENABLE_STAT_COUNTING) {
    t_stats[t_id].per_worker_acks_sent[m_i] += acks[m_i].ack_num;
    t_stats[t_id].per_worker_acks_mes_sent[m_i]++;
    t_stats[t_id].acks_sent += acks[m_i].ack_num;
    t_stats[t_id].acks_sent_mes_num++;
  }
  if (DEBUG_ACKS)
    my_printf(yellow, "Wrkr %d is sending an ack  to machine %u for lid %lu, credits %u and ack num %d and m id %d \n",
              t_id, m_i, acks[m_i].l_id, acks[m_i].credits, acks[m_i].ack_num, acks[m_i].m_id);

  if (ENABLE_ASSERTIONS) {
    assert(acks[m_i].credits <= acks[m_i].ack_num);
    if (acks[m_i].ack_num > COM_COALESCE) assert(acks[m_i].credits > 1);
    if (!ENABLE_MULTICAST) assert(acks[m_i].credits <= COM_CREDITS);
    assert(acks[m_i].ack_num > 0);
  }
}




#endif //CP_DEBUG_UTIL_H
