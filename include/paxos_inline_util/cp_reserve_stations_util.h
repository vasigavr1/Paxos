//
// Created by vasilis on 22/05/20.
//

#ifndef CP_RESERVE_STATIONS_UTIL_H
#define CP_RESERVE_STATIONS_UTIL_H


#include <od_inline_util.h>
#include "od_wrkr_side_calls.h"
#include "od_latency_util.h"
#include "cp_main.h"
#include "cp_debug_util.h"
#include "cp_config_util.h"
#include "cp_paxos_util.h"
#include "cp_paxos_generic_util.h"
#include "cp_config.h"

//-------------------------------------------------------------------------------------
// -------------------------------FORWARD DECLARATIONS--------------------------------
//-------------------------------------------------------------------------------------
static inline void fill_commit_message_from_l_entry(struct commit *com, loc_entry_t *loc_entry,
                                                    uint8_t broadcast_state, uint16_t t_id);
static inline void fill_commit_message_from_r_info(struct commit *com,
                                                   r_info_t* r_info, uint16_t t_id);

static inline void KVS_isolated_op(int t_id, write_t *write);
static inline void create_prop_rep(cp_prop_t *,
                                   cp_prop_mes_t *prop_mes,
                                   cp_rmw_rep_t *,
                                   mica_op_t *,
                                   uint16_t t_id);
static inline void create_acc_rep(cp_acc_t *acc,
                                  cp_acc_mes_t *acc_mes,
                                  cp_rmw_rep_t *acc_rep,
                                  mica_op_t *kv_ptr,
                                  uint16_t t_id) ;


/* ---------------------------------------------------------------------------
//------------------------------ Inserting-utility----------------------------
//---------------------------------------------------------------------------*/

static inline void cp_fill_prop(cp_prop_t *prop,
                                loc_entry_t *loc_entry,
                                uint16_t t_id)
{
  check_loc_entry_metadata_is_reset(loc_entry, "insert_prop_to_read_fifo", t_id);
  assign_ts_to_netw_ts(&prop->ts, &loc_entry->new_ts);
  memcpy(&prop->key, (void *)&loc_entry->key, KEY_SIZE);
  prop->opcode = PROPOSE_OP;
  prop->l_id = loc_entry->l_id;
  prop->t_rmw_id = loc_entry->rmw_id.id;
  prop->log_no = loc_entry->log_no;
  if (!loc_entry->base_ts_found)
    prop->base_ts = loc_entry->base_ts;
  else prop->base_ts.version = DO_NOT_CHECK_BASE_TS;
  if (ENABLE_ASSERTIONS) {
    assert(prop->ts.version >= PAXOS_TS);
  }
}

static inline void cp_fill_acc(cp_acc_t *acc,
                               loc_entry_t *loc_entry,
                               bool helping,
                               uint16_t t_id)
{
  check_loc_entry_metadata_is_reset(loc_entry, "insert_accept_in_writes_message_fifo", t_id);
  if (ENABLE_ASSERTIONS) assert(loc_entry->helping_flag != PROPOSE_NOT_LOCALLY_ACKED);
  if (DEBUG_RMW) {
    my_printf(yellow, "Wrkr %u Inserting an accept, l_id %lu, "
                      "rmw_id %lu, log %u,  helping %u,\n",
              t_id, loc_entry->l_id, loc_entry->rmw_id.id,
              loc_entry->log_no, helping);
  }

  acc->l_id = loc_entry->l_id;
  acc->t_rmw_id = loc_entry->rmw_id.id;
  acc->base_ts = loc_entry->base_ts;
  assign_ts_to_netw_ts(&acc->ts, &loc_entry->new_ts);
  memcpy(&acc->key, &loc_entry->key, KEY_SIZE);
  acc->opcode = ACCEPT_OP;
  if (!helping && !loc_entry->rmw_is_successful)
    memcpy(acc->value, loc_entry->value_to_read, (size_t) RMW_VALUE_SIZE);
  else memcpy(acc->value, loc_entry->value_to_write, (size_t) RMW_VALUE_SIZE);
  acc->log_no = loc_entry->log_no;
  acc->val_len = (uint8_t) loc_entry->rmw_val_len;
}


static inline void cp_insert_prop_help(context_t *ctx, void* prop_ptr,
                                       void *source, uint32_t source_flag)
{
  per_qp_meta_t *qp_meta = &ctx->qp_meta[PROP_QP_ID];
  fifo_t *send_fifo = qp_meta->send_fifo;
  p_ops_t *p_ops = (p_ops_t *) ctx->appl_ctx;

  cp_prop_t *prop = (cp_prop_t *) prop_ptr;
  loc_entry_t *loc_entry = (loc_entry_t *) source;

  cp_fill_prop(prop, loc_entry, ctx->t_id);

  slot_meta_t *slot_meta = get_fifo_slot_meta_push(send_fifo);
  cp_prop_mes_t *prop_mes = (cp_prop_mes_t *) get_fifo_push_slot(send_fifo);
  prop_mes->coalesce_num = (uint8_t) slot_meta->coalesce_num;
  // If it's the first message give it an lid
  if (slot_meta->coalesce_num == 1) {
    prop_mes->l_id = p_ops->inserted_prop_id[ctx->m_id];
    p_ops->inserted_prop_id[ctx->m_id]++;
  }
}


static inline uint64_t get_rmw_mes_l_id(void *mes, bool is_accept)
{
  return is_accept?
    ((cp_acc_mes_t *) mes)->l_id :
    ((cp_prop_mes_t *) mes)->l_id;
}

static inline uint8_t get_rmw_mes_m_id(void *mes, bool is_accept)
{
  return is_accept?
         ((cp_acc_mes_t *) mes)->m_id :
         ((cp_prop_mes_t *) mes)->m_id;
}

static inline void cp_insert_rmw_rep_helper(context_t *ctx,
                                            cp_rmw_rep_t *rep,
                                            mica_op_t *kv_ptr,
                                            uint32_t op_i,
                                            uint8_t qp_id)
{
  p_ops_t *p_ops = (p_ops_t *) ctx->appl_ctx;
  per_qp_meta_t *qp_meta = &ctx->qp_meta[qp_id];
  fifo_t *send_fifo = qp_meta->send_fifo;
  cp_ptrs_to_ops_t *ptrs_to_ops = p_ops->ptrs_to_ops;
  bool is_accept = qp_id == ACC_QP_ID;
  void *op = ptrs_to_ops->ptr_to_ops[op_i];
  void *mes = ptrs_to_ops->ptr_to_mes[op_i];

  is_accept ? create_acc_rep(op, mes, rep, kv_ptr, ctx->t_id) :
              create_prop_rep(op, mes, rep, kv_ptr, ctx->t_id);

  slot_meta_t *slot_meta = get_fifo_slot_meta_push(send_fifo);
  uint16_t rep_size = get_size_from_opcode(rep->opcode) - RMW_REP_SMALL_SIZE;
  slot_meta->byte_size += rep_size;

  cp_rmw_rep_mes_t *rep_mes = (cp_rmw_rep_mes_t *) get_fifo_push_slot(send_fifo);
  if (slot_meta->coalesce_num == 1) {
    rep_mes->l_id = get_rmw_mes_l_id(mes, is_accept);
    slot_meta->rm_id = get_rmw_mes_m_id(mes, is_accept);
    rep_mes->opcode = PROP_REPLY; //TODO remove the opcode field
  }
}


static inline void cp_insert_prop_rep_helper(context_t *ctx, void* prop_rep_ptr,
                                             void *source, uint32_t op_i)
{
  cp_insert_rmw_rep_helper(ctx, (cp_rmw_rep_t *) prop_rep_ptr,
                           (mica_op_t *) source, op_i, PROP_QP_ID);
}

static inline void cp_insert_acc_rep_helper(context_t *ctx, void* acc_rep_ptr,
                                             void *source, uint32_t op_i)
{
  cp_insert_rmw_rep_helper(ctx, (cp_rmw_rep_t *) acc_rep_ptr,
                           (mica_op_t *) source, op_i, ACC_QP_ID);
}


static inline void cp_insert_acc_help(context_t *ctx, void* acc_ptr,
                                      void *source, uint32_t source_flag)
{
  per_qp_meta_t *qp_meta = &ctx->qp_meta[ACC_QP_ID];
  fifo_t *send_fifo = qp_meta->send_fifo;
  p_ops_t *p_ops = (p_ops_t *) ctx->appl_ctx;

  cp_acc_t *acc = (cp_acc_t *) acc_ptr;
  loc_entry_t *loc_entry = (loc_entry_t *) source;
  cp_fill_acc(acc, loc_entry, (bool) source_flag, ctx->t_id);

  slot_meta_t *slot_meta = get_fifo_slot_meta_push(send_fifo);
  cp_acc_mes_t *acc_mes = (cp_acc_mes_t *) get_fifo_push_slot(send_fifo);
  acc_mes->coalesce_num = (uint8_t) slot_meta->coalesce_num;

  if (slot_meta->coalesce_num == 1) {
    acc_mes->l_id = p_ops->inserted_acc_id[ctx->m_id];
    p_ops->inserted_acc_id[ctx->m_id]++;
  }
}




/*-------------WRITES/ACCEPTS/COMMITS------------- */

// Set up a fresh write message to coalesce requests -- Accepts, commits, writes, releases
static inline void reset_write_message(p_ops_t *p_ops)
{

  MOD_INCR(p_ops->w_fifo->push_ptr, W_FIFO_SIZE);
  uint32_t w_mes_ptr = p_ops->w_fifo->push_ptr;
  struct w_message *w_mes = (struct w_message *)
    &p_ops->w_fifo->w_message[w_mes_ptr];
  w_mes_info_t * info = &p_ops->w_fifo->info[w_mes_ptr];
  //my_printf(cyan, "resetting message %u \n", p_ops->w_fifo->w_push_ptr);
  w_mes->l_id = 0;
  w_mes->coalesce_num = 0;
  info->message_size = (uint16_t) W_MES_HEADER;
  info->max_rep_message_size = 0;
  info->writes_num = 0;
  info->is_release = false;
  info->valid_header_l_id = false;
}


// Find out if a release can be coalesced
static inline bool coalesce_release(w_mes_info_t *info, struct w_message *w_mes,
                                    uint16_t session_id, uint16_t t_id)
{
  /* release cannot be coalesced when
   * -- A write from the same session exists already in the message
   **/
  for (uint8_t i = 0; i < w_mes->coalesce_num; i++) {
    if (session_id == info->per_message_sess_id[i]) {
//      printf("Wrkr %u release is of session %u, which exists in write %u/%u \n",
//             t_id,session_id, i, w_mes->coalesce_num);
      return false;
    }
  }
  //my_printf(green, "Wrkr %u release is of session %u, and can be coalesced at %u \n",
  //       t_id, session_id, w_mes->coalesce_num);
  return true;

}

// Return a pointer, where the next request can be created -- Accepts, commits, writes, releases
static inline void* get_w_ptr(p_ops_t *p_ops, uint8_t opcode,
                              uint16_t session_id, uint16_t t_id)
{
  check_state_with_allowed_flags(9, opcode, OP_RELEASE, KVS_OP_PUT, ACCEPT_OP,
                                 COMMIT_OP, RMW_ACQ_COMMIT_OP, OP_RELEASE_SECOND_ROUND,
                                 OP_ACQUIRE, COMMIT_OP_NO_VAL);
  if (ENABLE_ASSERTIONS) assert(session_id < SESSIONS_PER_THREAD);

  bool is_accept = opcode == ACCEPT_OP;
  bool is_release = opcode == OP_RELEASE;
  bool release_or_acc = (!TURN_OFF_KITE) &&
    (is_release || (is_accept && ACCEPT_IS_RELEASE)); //is_accept || is_release;

  uint32_t w_mes_ptr = p_ops->w_fifo->push_ptr;
  w_mes_info_t *info = &p_ops->w_fifo->info[w_mes_ptr];
  struct w_message *w_mes = (struct w_message *) &p_ops->w_fifo->w_message[w_mes_ptr];
  uint16_t new_size = get_write_size_from_opcode(opcode);
  bool new_message_because_of_release =
    release_or_acc ? (!coalesce_release(info, w_mes, session_id, t_id)) : false;

  if (is_accept) info->max_rep_message_size += ACC_REP_SIZE;
  bool new_message_because_of_r_rep = info->max_rep_message_size > MTU;
  bool new_message = ((info->message_size + new_size) > W_SEND_SIZE) ||
                     new_message_because_of_release ||
                     new_message_because_of_r_rep;

  if (ENABLE_ASSERTIONS && release_or_acc) {
    assert(p_ops->sess_info[session_id].writes_not_yet_inserted == 0);
  }
  if (new_message) {
    reset_write_message(p_ops);
    w_mes_ptr = p_ops->w_fifo->push_ptr;
    info = &p_ops->w_fifo->info[w_mes_ptr];
    w_mes = (struct w_message *) &p_ops->w_fifo->w_message[w_mes_ptr];
  }
  // Write opcode if it;s the first message
  if (w_mes->coalesce_num == 0)
    w_mes->opcode = (uint8_t) (is_accept ? ONLY_ACCEPTS : ONLY_WRITES);

  if (release_or_acc && !info->is_release) {
    info->is_release = true;
    info->first_release_byte_ptr = info->message_size;
    info->first_release_w_i = info->writes_num;
    if (ENABLE_ASSERTIONS && ACCEPT_IS_RELEASE)
      assert(info->writes_num == w_mes->coalesce_num);
  }
  info->per_message_release_flag[w_mes->coalesce_num] = release_or_acc;
  // Set up the backwards pointers to be able to change
  // the state of requests, after broadcasting
  if (!is_accept) {
    if (!info->valid_header_l_id) {
      info->valid_header_l_id = true;
      info->backward_ptr = p_ops->w_push_ptr;
      w_mes->l_id = (uint64_t) (p_ops->local_w_id + p_ops->w_size);
      //my_printf(yellow, "Setting l_id of ms %u to %lu \n", w_mes_ptr, w_mes->l_id);
    }
    info->writes_num++;
    if (w_mes->opcode == ONLY_ACCEPTS) w_mes->opcode = WRITES_AND_ACCEPTS;
  }
  else if (w_mes->opcode == ONLY_WRITES) w_mes->opcode = WRITES_AND_ACCEPTS;

  info->per_message_sess_id[w_mes->coalesce_num] = session_id;
  w_mes->coalesce_num++;
  uint32_t inside_w_ptr = info->message_size;
  info->message_size += new_size;
  if (DEBUG_WRITES)
    my_printf(green, "Wrkr %u, sess %u inserts write %u, new_message %d, coalesce num %u, "
                "w_num %u, w_mes_ptr %u, mes_l_id %lu valid l_id %d,  message capacity %u \n",
              t_id, session_id, opcode, new_message, w_mes->coalesce_num,
              info->writes_num, w_mes_ptr, w_mes->l_id, info->valid_header_l_id, info->message_size);



  if (ENABLE_ASSERTIONS) assert(info->message_size <= W_SEND_SIZE);
  return (void *) (((void *)w_mes) + inside_w_ptr);
}

//
static inline uint8_t get_write_opcode(const uint8_t source, trace_op_t *op,
                                       r_info_t *r_info,
                                       loc_entry_t *loc_entry)
{
  switch(source) {
    case FROM_TRACE:
      return op->opcode;
    case FROM_READ:
      check_state_with_allowed_flags(4, r_info->opcode, OP_ACQUIRE, OP_RELEASE, KVS_OP_PUT);
      if (r_info->opcode == OP_ACQUIRE)
        return RMW_ACQ_COMMIT_OP;
      else return r_info->opcode;
    case FROM_COMMIT:
      if (loc_entry->avoid_val_in_com) {
        //loc_entry->avoid_val_in_com = false;
        return COMMIT_OP_NO_VAL;
      }
      return COMMIT_OP;
    case RELEASE_THIRD:
      return OP_RELEASE_SECOND_ROUND;
    default: assert(false);

  }
}

//
static inline void increase_virt_w_size(p_ops_t *p_ops, write_t *write,
                                        uint8_t source, uint16_t t_id) {
  if (write->opcode == OP_RELEASE) {
    if (ENABLE_ASSERTIONS) assert(source == FROM_READ);
    p_ops->virt_w_size += 2;
    //my_printf(yellow, "+2 %u at %u \n",  p_ops->virt_w_size, p_ops->w_push_ptr);
  } else {
    //my_printf(yellow, "Increasing virt_w_size %u at %u, source %u \n",
    //              p_ops->virt_w_size, p_ops->w_push_ptr, source);
    p_ops->virt_w_size++;
  }

  if (ENABLE_ASSERTIONS) {
    if (p_ops->virt_w_size > MAX_ALLOWED_W_SIZE + 1)
      my_printf(red, "Wrkr %u Virt_w_size %u/%d, source %u, write->opcode %u \n",
                t_id, p_ops->virt_w_size, MAX_ALLOWED_W_SIZE, source, write->opcode);
    assert(p_ops->w_size <= MAX_ALLOWED_W_SIZE);
    assert(p_ops->w_size <= p_ops->virt_w_size);
  }
}

//
static inline uint16_t get_w_sess_id(p_ops_t *p_ops, trace_op_t *op,
                                     const uint8_t source,
                                     const uint32_t incoming_pull_ptr,
                                     const uint16_t t_id)
{
  loc_entry_t *loc_entry = (loc_entry_t *) op;

  switch (source) {
    case FROM_COMMIT:
      return loc_entry->sess_id;
      // source = FROM_READ: 2nd round of Acquires/Releases, 2nd round of out-of-epoch Writes
      // This also includes Commits triggered by RMW-Acquires
    case FROM_READ:
      return (uint16_t) p_ops->r_session_id[incoming_pull_ptr];
    case FROM_TRACE:
    case RELEASE_THIRD: //source = FROM_WRITE || LIN_WRITE
      if (ENABLE_ASSERTIONS) {
        assert(op != NULL);
        uint16_t session_id = op->session_id;
        assert(session_id == *(uint16_t *) op);
        assert(session_id < SESSIONS_PER_THREAD);
        if (source == RELEASE_THIRD) {
          write_t *w = (write_t *) &op->ts;
          check_state_with_allowed_flags(3, w->opcode, OP_RELEASE_BIT_VECTOR, NO_OP_RELEASE);
        }
      }
      return op->session_id;
    default: if (ENABLE_ASSERTIONS) assert(false);
  }
}

// When inserting a write
static inline void
set_w_sess_info_and_index_to_req_array(p_ops_t *p_ops, trace_op_t *write,
                                       const uint8_t source, uint32_t w_ptr,
                                       const uint32_t incoming_pull_ptr,
                                       uint16_t sess_id, const uint16_t t_id)
{
  p_ops->w_meta[w_ptr].sess_id = sess_id;
  switch (source) {
    case FROM_TRACE:
      if (ENABLE_CLIENTS) {
        p_ops->w_index_to_req_array[w_ptr] = write->index_to_req_array;
      }
      return;
    case FROM_READ:
      if (ENABLE_CLIENTS) {
        p_ops->w_index_to_req_array[w_ptr] = p_ops->r_index_to_req_array[incoming_pull_ptr];
      }
      return;
    case FROM_COMMIT:
      add_request_to_sess_info(&p_ops->sess_info[sess_id], t_id);
      return;
    case RELEASE_THIRD: //source = FROM_WRITE || LIN_WRITE
      p_ops->w_index_to_req_array[w_ptr] = p_ops->w_index_to_req_array[incoming_pull_ptr];
      return;
    default:
      if (ENABLE_ASSERTIONS) assert(false);
  }
}


// Set up the message depending on where it comes from: trace, 2nd round of release, 2nd round of read etc.
static inline void write_bookkeeping_in_insertion_based_on_source
  (p_ops_t *p_ops, write_t *write, trace_op_t *op,
   const uint8_t source, const uint32_t incoming_pull_ptr,
   r_info_t *r_info, const uint16_t t_id)
{
  my_assert(source <= FROM_COMMIT, "When inserting a write source is too high. Have you enabled lin writes?");

  if (source == FROM_TRACE) {
    write->version = op->ts.version;
    write->key = op->key;
    write->opcode = op->opcode;
    write->val_len = op->val_len;
    //memcpy(&write->version, (void *) &op->base_ts.version, 4 + KEY_SIZE + 2);
    if (ENABLE_ASSERTIONS) assert(op->real_val_len <= VALUE_SIZE);
    memcpy(write->value, op->value_to_write, op->real_val_len);
    write->m_id = (uint8_t) machine_id;
  }
  else if (source == RELEASE_THIRD) { // Second round of a release
    write_t *tmp = (write_t *) &op->ts; // we have treated the rest as a write_t
    memcpy(&write->m_id, tmp, W_SIZE);
    write->opcode = OP_RELEASE_SECOND_ROUND;
    //if (DEBUG_SESSIONS)
    // my_printf(cyan, "Wrkr %u: Changing the opcode from %u to %u of write %u of w_mes %u \n",
    //             t_id, op->opcode, write->opcode, inside_w_ptr, w_mes_ptr);
    if (ENABLE_ASSERTIONS) assert (write->m_id == (uint8_t) machine_id);
    if (DEBUG_QUORUM) {
      printf("Thread %u: Second round release, from ptr: %u to ptr %u, key: ", t_id, incoming_pull_ptr, p_ops->w_push_ptr);
      print_key(&write->key);
    }
  }
  else if (source == FROM_COMMIT || (source == FROM_READ && r_info->is_read)) {
    if (source == FROM_READ){
      if (ENABLE_ASSERTIONS) assert(r_info->opcode == OP_ACQUIRE);
      fill_commit_message_from_r_info((struct commit *) write, r_info, t_id);}
    else {
      uint8_t broadcast_state = (uint8_t) incoming_pull_ptr;
      fill_commit_message_from_l_entry((struct commit *) write,
                                       (loc_entry_t *) op, broadcast_state,  t_id);
    }
  }
  else { //source = FROM_READ: 2nd round of ooe-write/release
    write->m_id = r_info->ts_to_read.m_id;
    write->version = r_info->ts_to_read.version;
    write->key = r_info->key;
    memcpy(write->value, r_info->value, r_info->val_len);
    write->opcode = r_info->opcode;
    write->val_len = VALUE_SIZE >> SHIFT_BITS;
    if (ENABLE_ASSERTIONS) {
      assert(!r_info->is_read);
      assert(source == FROM_READ);
      check_state_with_allowed_flags(4, r_info->opcode, KVS_OP_PUT, OP_RELEASE);
    }
  }
  // Make sure the pointed values are correct
}



/* ---------------------------------------------------------------------------
//------------------------------INSERTS-TO-MESSAGE_FIFOS----------------------
//---------------------------------------------------------------------------*/




// Insert a new local or remote write to the pending writes
static inline void insert_write(p_ops_t *p_ops, trace_op_t *op, const uint8_t source,
                                const uint32_t incoming_pull_ptr, uint16_t t_id)
{
  r_info_t *r_info = NULL;
  loc_entry_t *loc_entry = (loc_entry_t *) op;
  if (source == FROM_READ) r_info = &p_ops->read_info[incoming_pull_ptr];
  uint32_t w_ptr = p_ops->w_push_ptr;
  uint8_t opcode = get_write_opcode(source, op, r_info, loc_entry);
  uint16_t sess_id =  get_w_sess_id(p_ops, op, source, incoming_pull_ptr, t_id);
  set_w_sess_info_and_index_to_req_array(p_ops, op, source, w_ptr, incoming_pull_ptr,
                                         sess_id, t_id);

  if (ENABLE_ASSERTIONS && source == FROM_READ &&
      r_info->opcode == KVS_OP_PUT) {
    assert(p_ops->sess_info[sess_id].writes_not_yet_inserted > 0);
    p_ops->sess_info[sess_id].writes_not_yet_inserted--;
  }

  write_t *write = (write_t *)
    get_w_ptr(p_ops, opcode, (uint16_t) p_ops->w_meta[w_ptr].sess_id, t_id);

  uint32_t w_mes_ptr = p_ops->w_fifo->push_ptr;
  struct w_message *w_mes = (struct w_message *) &p_ops->w_fifo->w_message[w_mes_ptr];

  //printf("Insert a write %u \n", *(uint32_t *)write);
  if (DEBUG_READS && source == FROM_READ) {
    my_printf(yellow, "Wrkr %u Inserting a write as a second round of read/write w_size %u/%d, bcast capacity %u, "
                " w_push_ptr %u, w_pull_ptr %u "
                "l_id %lu, fifo w_push_ptr %u, fifo pull ptr %u\n", t_id,
              p_ops->w_size, PENDING_WRITES, p_ops->w_fifo->bcast_size,
              p_ops->w_push_ptr, p_ops->w_pull_ptr,
              w_mes->l_id, p_ops->w_fifo->push_ptr, p_ops->w_fifo->bcast_pull_ptr);
  }

  write_bookkeeping_in_insertion_based_on_source(p_ops, write, op, source, incoming_pull_ptr,
                                                 r_info, t_id);

  if (ENABLE_ASSERTIONS) {
    debug_checks_when_inserting_a_write(source, write, w_mes_ptr,
                                        w_mes->l_id, p_ops, w_ptr, t_id);
    assert(p_ops->w_meta[w_ptr].w_state == INVALID);
  }
  //if (t_id == 1) printf("Wrkr %u Validating state at ptr %u \n", t_id, w_ptr);
  p_ops->w_meta[w_ptr].w_state = VALID;
  if (ENABLE_ASSERTIONS) {
    if (p_ops->w_size > 0) assert(p_ops->w_push_ptr != p_ops->w_pull_ptr);
  }
  p_ops->w_size++;
  p_ops->w_fifo->bcast_size++;
  increase_virt_w_size(p_ops, write, source, t_id);
  MOD_INCR(p_ops->w_push_ptr, PENDING_WRITES);
}


// Fill the trace_op to be passed to the KVS. Returns whether no more requests can be processed
static inline bool fill_trace_op(context_t *ctx,
                                 p_ops_t *p_ops, trace_op_t *op,
                                 trace_t *trace,
                                 int working_session,
                                 uint16_t t_id)
{
  create_inputs_of_op(&op->value_to_write, &op->value_to_read, &op->real_val_len,
                      &op->opcode, &op->index_to_req_array,
                      &op->key, op->value, trace, working_session, t_id);
  if (!ENABLE_CLIENTS) check_trace_req(p_ops, trace, op, working_session, t_id);


  bool is_rmw = opcode_is_rmw(op->opcode);
  if (ENABLE_ASSERTIONS) assert(is_rmw);
  if (ENABLE_ASSERTIONS && !ENABLE_CLIENTS && op->opcode == FETCH_AND_ADD) {
    assert(is_rmw);
    assert(op->value_to_write == op->value);
    assert(*(uint64_t *) op->value_to_write == 1);
  }
  if (is_rmw && ENABLE_ALL_ABOARD) {
    op->attempt_all_aboard = ctx->q_info->missing_num == 0;
  }
  increment_per_req_counters(op->opcode, t_id);
  if (ENABLE_ASSERTIONS) assert(!p_ops->stalled[working_session]);
  p_ops->stalled[working_session] = true;
  op->session_id = (uint16_t) working_session;

  if (ENABLE_ASSERTIONS && DEBUG_SESSIONS)
    p_ops->debug_loop->ses_dbg->dbg_cnt[working_session] = 0;
  //if (w_pull_ptr[[working_session]] == 100000) my_printf(yellow, "Working ses %u \n", working_session);
  //my_printf(yellow, "BEFORE: OP_i %u -> session %u, opcode: %u \n", op_i, working_session, ops[op_i].opcode);
  //my_printf(yellow, "Wrkr %u, session %u, opcode %u \n", t_id, working_session, op->opcode);

  if (ENABLE_CLIENTS) {
    signal_in_progress_to_client(op->session_id, op->index_to_req_array, t_id);
    if (ENABLE_ASSERTIONS) assert(interface[t_id].wrkr_pull_ptr[working_session] == op->index_to_req_array);
    MOD_INCR(interface[t_id].wrkr_pull_ptr[working_session], PER_SESSION_REQ_NUM);
  }
}


/*----------------------------------------------------
 * ----------SENDING/RECEIVING RPCs-------------------
 * --------------------------------------------------*/


// When forging a write
static inline void set_w_state_for_each_write(p_ops_t *p_ops, w_mes_info_t *info,
                                              struct w_message *w_mes, uint32_t backward_ptr,
                                              uint8_t coalesce_num, struct ibv_sge *send_sgl,
                                              uint16_t br_i, quorum_info_t *q_info, uint16_t t_id)
{
  uint16_t byte_ptr = W_MES_HEADER;
  bool failure = false;

  if (info->is_release ) {
    failure = add_failure_to_release_from_sess_id(p_ops, w_mes, info, q_info, backward_ptr, t_id);
  }
  for (uint8_t i = 0; i < coalesce_num; i++) {
    write_t *write = (write_t *)(((void *)w_mes) + byte_ptr);
    //printf("Write %u/%u opcode %u \n", i, coalesce_num, write->opcode);
    byte_ptr += get_write_size_from_opcode(write->opcode);

    per_write_meta_t *w_meta = &p_ops->w_meta[backward_ptr];
    uint8_t *w_state = &w_meta->w_state;

    sess_info_t *sess_info = &p_ops->sess_info[info->per_message_sess_id[i]];
    switch (write->opcode) {
      case ACCEPT_OP:
      case ACCEPT_OP_BIT_VECTOR:
        if (ACCEPT_IS_RELEASE) reset_sess_info_on_accept(sess_info, t_id);
        checks_when_forging_an_accept((struct accept *) write, send_sgl, br_i, i, coalesce_num, t_id);
        // accept gets a custom response from r_rep and need not set the w_state
        break;
      case KVS_OP_PUT:
        checks_when_forging_a_write(write, send_sgl, br_i, i, coalesce_num, t_id);
        update_sess_info_missing_ids_when_sending(p_ops, info, q_info, i, t_id);
        w_meta->acks_expected = q_info->active_num;
        *w_state = SENT_PUT;
        break;
      case COMMIT_OP:
      case COMMIT_OP_NO_VAL:
        checks_when_forging_a_commit((struct commit*) write, send_sgl, br_i, i, coalesce_num, t_id);
        update_sess_info_missing_ids_when_sending(p_ops, info, q_info, i, t_id);
        w_meta->acks_expected = q_info->active_num;
        *w_state = SENT_COMMIT;
        break;
      case RMW_ACQ_COMMIT_OP:
        *w_state = SENT_RMW_ACQ_COMMIT;
        write->opcode = COMMIT_OP;
        w_meta->acks_expected = (uint8_t) REMOTE_QUORUM;
        break;
      case OP_RELEASE_BIT_VECTOR:
        w_meta->acks_expected = (uint8_t) REMOTE_QUORUM;
        checks_when_forging_a_write(write, send_sgl, br_i, i, coalesce_num, t_id);
        *w_state = SENT_BIT_VECTOR;
        break;
      case OP_RELEASE:
        if (!TURN_OFF_KITE && failure) {
          write->opcode = NO_OP_RELEASE;
          //write_t *first_rel = (((write *)w_mes) + info->first_release_byte_ptr);
          //my_printf(yellow, "Wrkr %u Adding a no_op_release in position %u/%u, first opcode %u \n",
          //              t_id, i, coalesce_num, first_rel->opcode);
          *w_state = SENT_NO_OP_RELEASE;
          p_ops->ptrs_to_local_w[backward_ptr] = write;
          w_meta->acks_expected = (uint8_t) REMOTE_QUORUM;
          break;
        }
        // NO break here -- merge with actions of OP_RELEASE_SECOND_ROUND
      case OP_RELEASE_SECOND_ROUND:
        write->opcode = OP_RELEASE;
        reset_sess_info_on_release(sess_info, q_info, t_id);
        KVS_isolated_op(t_id, write);
        w_meta->acks_expected = q_info->active_num;
        checks_when_forging_a_write(write, send_sgl, br_i, i, coalesce_num, t_id);
        *w_state = SENT_RELEASE;
        break;
      case OP_ACQUIRE:
        checks_when_forging_a_write(write, send_sgl, br_i, i, coalesce_num, t_id);
        *w_state = SENT_ACQUIRE;
        w_meta->acks_expected = (uint8_t) REMOTE_QUORUM;
        break;
      default: if (ENABLE_ASSERTIONS) assert(false);
    }
    if (ENABLE_ASSERTIONS) (w_meta->acks_expected >= REMOTE_QUORUM);
//    if (write->opcode != ACCEPT_OP && t_id == 1)
//      my_printf(yellow, "Wrkr %u Setting state %u ptr %u/%d opcode %u message %u/%u \n",
//                  t_id, *w_state, backward_ptr, PENDING_WRITES, write->opcode,
//                  i, coalesce_num);
    if (write->opcode != ACCEPT_OP && write->opcode != ACCEPT_OP_BIT_VECTOR) {
      for (uint8_t m_i = 0; m_i < q_info->active_num; m_i++)
        w_meta->expected_ids[m_i] = q_info->active_ids[m_i];

      MOD_INCR(backward_ptr, PENDING_WRITES);
    }


  }
}



static inline bool release_not_ready(p_ops_t *p_ops,
                                     w_mes_info_t *info,
                                     struct w_message *w_mes,
                                     uint16_t t_id)
{
  if (TURN_OFF_KITE) return false;
  if (!info->is_release)
    return false; // not even a release

  //sess_info_t *sess_info = p_ops->sess_info;
  // We know the message contains releases. let's check their sessions!
  for (uint8_t i = 0; i < w_mes->coalesce_num; i++) {
    if (info->per_message_release_flag[i]) {
      sess_info_t *sess_info = &p_ops->sess_info[info->per_message_sess_id[i]];
      if (!sess_info->ready_to_release) {
        if (ENABLE_ASSERTIONS) {
          assert(sess_info->live_writes > 0);
          p_ops->debug_loop->release_rdy_dbg_cnt++;
          if (p_ops->debug_loop->release_rdy_dbg_cnt == M_4) {
            if (t_id == 0) printf("Wrkr %u stuck. Release cannot fire \n", t_id);
            p_ops->debug_loop->release_rdy_dbg_cnt = 0;
          }
        }
        return true; // release is not ready yet
      }
    }
  }
  if (ENABLE_ASSERTIONS) p_ops->debug_loop->release_rdy_dbg_cnt = 0;
  return false; // release is ready
}


// return true if the loc_entry cannot be inspected
static inline bool cannot_accept_if_unsatisfied_release(loc_entry_t* loc_entry,
                                                        sess_info_t *sess_info)
{
  if (TURN_OFF_KITE || (!ACCEPT_IS_RELEASE)) return false;
  if (loc_entry->must_release && !sess_info->ready_to_release) {
    loc_entry->stalled_reason = STALLED_BECAUSE_ACC_RELEASE;
    return true;
  }
  else if (loc_entry->must_release) {
    loc_entry->stalled_reason = NO_REASON;
    loc_entry->must_release = false;
  }
  return false;
}

/*----------------------------------------------------
 * ---------------COMMITTING----------------------------
 * --------------------------------------------------*/

// Release performs two writes when the first round must carry the send vector
static inline void commit_first_round_of_release_and_spawn_the_second (p_ops_t *p_ops,
                                                                       uint16_t t_id)
{
  uint32_t w_pull_ptr = p_ops->w_pull_ptr;
  bool is_no_op = p_ops->w_meta[p_ops->w_pull_ptr].w_state == READY_NO_OP_RELEASE;
  write_t *rel = p_ops->ptrs_to_local_w[w_pull_ptr];
  if (ENABLE_ASSERTIONS) {
    assert (rel != NULL);
    if (is_no_op) assert(rel->opcode == NO_OP_RELEASE);
    else assert(rel->opcode == OP_RELEASE_BIT_VECTOR);
  }
  // because we overwrite the value,
  if (!is_no_op)
    memcpy(rel->value, &p_ops->overwritten_values[SEND_CONF_VEC_SIZE * w_pull_ptr], SEND_CONF_VEC_SIZE);
  trace_op_t op;
  op.session_id = (uint16_t) p_ops->w_meta[w_pull_ptr].sess_id;
  memcpy((void *) &op.ts, rel, W_SIZE); // We are treating the trace op as a sess_id + write_t
  //if (DEBUG_SESSIONS)
  //my_printf(cyan, "Wrkr: %u Inserting the write for the second round of the "
  //            "release opcode %u that carried a bit vector: session %u\n",
  //            t_id, op.opcode, p_ops->w_session_id[w_pull_ptr]);
  insert_write(p_ops, &op, RELEASE_THIRD, w_pull_ptr, t_id); // the push pointer is not needed because the session id is inside the op
  if (ENABLE_ASSERTIONS) {
    p_ops->ptrs_to_local_w[w_pull_ptr] =  NULL;
    memset(&p_ops->overwritten_values[SEND_CONF_VEC_SIZE * w_pull_ptr], 0, SEND_CONF_VEC_SIZE);
  }
}


// When a write has not gathered all acks but time-out expires
static inline bool complete_requests_that_wait_all_acks(uint8_t *w_state,
                                                        uint32_t w_ptr, uint16_t t_id)
{
  switch(*w_state) {
    case SENT_PUT:
    case SENT_RELEASE:
    case SENT_COMMIT:
      (*w_state) += W_STATE_OFFSET;
      return true;
    default:
      if (ENABLE_ASSERTIONS) {
        if (*w_state >= READY_PUT && *w_state <= READY_NO_OP_RELEASE)
          break;
        my_printf(red, "Wrkr %u state %u, ptr %u \n", t_id, w_state, w_ptr);
        assert(false);
      }
  }
  return false;
}



//
static inline void attempt_to_free_partially_acked_write(p_ops_t *p_ops, uint16_t t_id)
{
  per_write_meta_t *w_meta = &p_ops->w_meta[p_ops->w_pull_ptr];

  if (w_meta->w_state >= SENT_PUT && w_meta->acks_seen >= REMOTE_QUORUM) {
    p_ops->full_w_q_fifo++;
    if (p_ops->full_w_q_fifo == WRITE_FIFO_TIMEOUT) {
      //printf("Wrkr %u expires write fifo timeout and "
      //         "releases partially acked writes \n", t_id);
      p_ops->full_w_q_fifo = 0;
      uint32_t w_pull_ptr = p_ops->w_pull_ptr;
      for (uint32_t i = 0; i < p_ops->w_size; i++) {
        w_meta = &p_ops->w_meta[w_pull_ptr];
        if (w_meta->w_state >= SENT_PUT && w_meta->acks_seen >= REMOTE_QUORUM) {
          if (complete_requests_that_wait_all_acks(&w_meta->w_state, w_pull_ptr, t_id))
            update_sess_info_partially_acked_write(p_ops, w_pull_ptr, t_id);
        }
        else if (p_ops->w_meta[w_pull_ptr].w_state < SENT_PUT) { break; }
        MOD_INCR(w_pull_ptr, PENDING_WRITES);
      }
    }
  }
}


//
static inline void clear_after_release_quorum(p_ops_t *p_ops,
                                              uint32_t w_ptr, uint16_t t_id)
{
  uint32_t sess_id = p_ops->w_meta[w_ptr].sess_id;
  if (ENABLE_ASSERTIONS) assert( sess_id < SESSIONS_PER_THREAD);
  sess_info_t *sess_info = &p_ops->sess_info[sess_id];
  if (ENABLE_ASSERTIONS && !sess_info->stalled)
    printf("state %u ptr %u \n", p_ops->w_meta[w_ptr].w_state, w_ptr);
  // Releases, and Acquires/RMW-Acquires that needed a "write" round complete here
  signal_completion_to_client(sess_id, p_ops->w_index_to_req_array[w_ptr], t_id);
  check_sess_info_after_completing_release(sess_info, t_id);
  sess_info->stalled = false;
  p_ops->all_sessions_stalled = false;
}






#endif //CP_RESERVE_STATIONS_UTIL_H
