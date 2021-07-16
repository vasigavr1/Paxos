//
// Created by vasilis on 16/07/2021.
//

#include <cp_core_interface.h>
#include <cp_core_common_util.h>


static inline void fill_com_with_no_val(cp_com_t *com,
                                        loc_entry_t *loc_entry)
{
  assert(ENABLE_COMMITS_WITH_NO_VAL);
  com->opcode = COMMIT_OP_NO_VAL;
  loc_entry->avoid_val_in_com = false;
  com->base_ts.version = loc_entry->log_no;
}

static inline void fill_com_with_val(cp_com_t *com,
                                     loc_entry_t *loc_entry,
                                     uint8_t broadcast_state)
{
  com->opcode = COMMIT_OP;
  com->log_no = loc_entry->log_no;
  com->base_ts.version = loc_entry->base_ts.version;
  if (broadcast_state == MUST_BCAST_COMMITS && !loc_entry->rmw_is_successful) {
    memcpy(com->value, loc_entry->value_to_read, (size_t) RMW_VALUE_SIZE);
  } else {
    memcpy(com->value, loc_entry->value_to_write, (size_t) RMW_VALUE_SIZE);
  }
  check_after_filling_com_with_val(com);
}


inline uint16_t fill_commit_message_from_l_entry(cp_com_t *com,
                                                 void *loc_entry_ptr,
                                                 uint8_t broadcast_state,
                                                 uint16_t t_id)
{
  loc_entry_t *loc_entry = (loc_entry_t *) loc_entry_ptr;
  check_loc_entry_when_filling_com(loc_entry, broadcast_state, t_id);

  memcpy(&com->key, &loc_entry->key, KEY_SIZE);
  com->t_rmw_id = loc_entry->rmw_id.id;
  com->base_ts.m_id = loc_entry->base_ts.m_id;
  if (loc_entry->avoid_val_in_com) {
    fill_com_with_no_val(com, loc_entry);
  }
  else {
    fill_com_with_val(com, loc_entry, broadcast_state);
  }
  return loc_entry->sess_id;
}

inline void cp_fill_acc(cp_acc_t *acc,
                        void *loc_entry_ptr,
                        bool helping,
                        uint16_t t_id)
{
  loc_entry_t *loc_entry = (loc_entry_t *) loc_entry_ptr;
  check_loc_entry_metadata_is_reset(loc_entry, "inserting_accept", t_id);
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

inline void cp_fill_prop(cp_prop_t *prop,
                         void *loc_entry_ptr,
                         uint16_t t_id)
{
  loc_entry_t *loc_entry = (loc_entry_t *) loc_entry_ptr;
  check_loc_entry_metadata_is_reset(loc_entry, "inserting prop", t_id);
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
