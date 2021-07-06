//
// Created by vasilis on 06/07/2021.
//

#include <cp_core_util.h>

static inline void overwrite_help_loc_entry(loc_entry_t* loc_entry,
                                            cp_rmw_rep_t* prop_rep)
{
  loc_entry_t *help_loc_entry = loc_entry->help_loc_entry;
  if (loc_entry->helping_flag == PROPOSE_LOCALLY_ACCEPTED) loc_entry->helping_flag = NOT_HELPING;
  assign_netw_ts_to_ts(&help_loc_entry->new_ts, &prop_rep->ts);
  help_loc_entry->base_ts.version = prop_rep->log_no_or_base_version;
  help_loc_entry->base_ts.m_id = prop_rep->base_m_id;
  help_loc_entry->log_no = loc_entry->log_no;
  help_loc_entry->state = ACCEPTED;
  help_loc_entry->rmw_id.id = prop_rep->rmw_id;
  memcpy(help_loc_entry->value_to_write, prop_rep->value, (size_t) RMW_VALUE_SIZE);
  help_loc_entry->key = loc_entry->key;
}


static inline void store_rmw_rep_in_help_loc_entry(loc_entry_t* loc_entry,
                                                   cp_rmw_rep_t* prop_rep,
                                                   uint16_t t_id)
{
  loc_entry_t *help_loc_entry = loc_entry->help_loc_entry;
  compare_t ts_comp = compare_netw_ts_with_ts(&prop_rep->ts, &help_loc_entry->new_ts);
  check_store_rmw_rep_to_help_loc_entry(loc_entry, prop_rep, ts_comp);

  bool have_not_found_any_ts_to_help = help_loc_entry->state == INVALID_RMW;
  bool found_higher_ts_to_help = ts_comp == GREATER;


  bool must_overwrite_help_loc_entry = have_not_found_any_ts_to_help ||
                                       found_higher_ts_to_help;

  if (must_overwrite_help_loc_entry)
    overwrite_help_loc_entry(loc_entry, prop_rep);
}

static inline void bookkeeping_for_rep_info(rmw_rep_info_t *rep_info,
                                            cp_rmw_rep_t *rep)
{
  rep_info->tot_replies++;
  if (rep_info->tot_replies >= QUORUM_NUM) {
    rep_info->ready_to_inspect = true;
  }
  if (rep->opcode > RMW_ACK_BASE_TS_STALE) rep_info->nacks++;
}

static inline void handle_rmw_rep_ack(cp_rmw_rep_mes_t *rep_mes,
                                      cp_rmw_rep_t *rep,
                                      loc_entry_t *loc_entry,
                                      rmw_rep_info_t *rep_info,
                                      bool is_accept,
                                      const uint16_t t_id)
{
  if (rep->opcode == RMW_ACK_BASE_TS_STALE) {
    write_kv_if_conditional_on_netw_ts(loc_entry->kv_ptr, rep->value,
                                       (size_t) VALUE_SIZE, FROM_BASE_TS_STALE, rep->ts);
    assign_netw_ts_to_ts(&loc_entry->base_ts, &rep->ts);
  }
  loc_entry->base_ts_found = true;
  rep_info->acks++;
  check_handle_prop_or_acc_rep_ack(rep_mes, rep_info, is_accept, t_id);
}

static inline void handle_rmw_rep_rmw_id_committed(loc_entry_t *loc_entry,
                                                   cp_rmw_rep_t *rep,
                                                   rmw_rep_info_t *rep_info,
                                                   const uint16_t t_id)
{
  if (rep->opcode == RMW_ID_COMMITTED)
    rep_info->no_need_to_bcast = true;
  rep_info->ready_to_inspect = true;
  rep_info->rmw_id_commited++;
  commit_rmw(loc_entry->kv_ptr, NULL, loc_entry,
             FROM_ALREADY_COMM_REP, t_id);
}

static inline void handle_rmw_rep_log_too_small(loc_entry_t *loc_entry,
                                                cp_rmw_rep_t *rep,
                                                rmw_rep_info_t *rep_info,
                                                const uint16_t t_id)
{
  rep_info->ready_to_inspect = true;
  rep_info->log_too_small++;
  commit_rmw(loc_entry->kv_ptr, (void*) rep, loc_entry,
             FROM_LOG_TOO_LOW_REP, t_id);
}

static inline void handle_rmw_rep_seen_lower_acc(loc_entry_t *loc_entry,
                                                 cp_rmw_rep_t *rep,
                                                 rmw_rep_info_t *rep_info,
                                                 bool is_accept,
                                                 const uint16_t t_id)
{
  rep_info->already_accepted++;
  check_handle_rmw_rep_seen_lower_acc(loc_entry, rep, is_accept);

  bool no_higher_priority_reps_have_been_seen =
      rep_info->seen_higher_prop_acc +
      rep_info->rmw_id_commited + rep_info->log_too_small == 0;

  if (no_higher_priority_reps_have_been_seen)
    store_rmw_rep_in_help_loc_entry(loc_entry, rep, t_id);

}

static inline void handle_rmw_rep_higher_acc_prop(cp_rmw_rep_t *rep,
                                                  rmw_rep_info_t *rep_info,
                                                  bool is_accept,
                                                  const uint16_t t_id)
{
  if (!is_accept) rep_info->ready_to_inspect = true;
  rep_info->seen_higher_prop_acc++;
  print_handle_rmw_rep_seen_higher(rep, rep_info, is_accept, t_id);
  if (rep->ts.version > rep_info->seen_higher_prop_version) {
    rep_info->seen_higher_prop_version = rep->ts.version;
    print_handle_rmw_rep_higher_ts(rep_info, t_id);
  }
}

static inline void handle_rmw_rep_log_too_high(rmw_rep_info_t *rep_info)
{
  rep_info->log_too_high++;
}

static inline void handle_rmw_rep_based_on_opcode(cp_rmw_rep_mes_t *rep_mes,
                                                  loc_entry_t *loc_entry,
                                                  cp_rmw_rep_t *rep,
                                                  rmw_rep_info_t *rep_info,
                                                  bool is_accept,
                                                  const uint16_t t_id)
{
  switch (rep->opcode) {
    case RMW_ACK_BASE_TS_STALE:
    case RMW_ACK:
      handle_rmw_rep_ack(rep_mes, rep, loc_entry,
                         rep_info, is_accept, t_id);
      break;
    case RMW_ID_COMMITTED:
    case RMW_ID_COMMITTED_SAME_LOG:
      handle_rmw_rep_rmw_id_committed(loc_entry, rep, rep_info, t_id);
      break;
    case LOG_TOO_SMALL:
      handle_rmw_rep_log_too_small(loc_entry, rep, rep_info, t_id);
      break;
    case SEEN_LOWER_ACC:
      handle_rmw_rep_seen_lower_acc(loc_entry, rep, rep_info, is_accept, t_id);
      break;
    case SEEN_HIGHER_ACC:
    case SEEN_HIGHER_PROP:
      handle_rmw_rep_higher_acc_prop(rep, rep_info, is_accept, t_id);
      break;
    case LOG_TOO_HIGH:
      handle_rmw_rep_log_too_high(rep_info);
      break;
    default: my_assert(false, "");
  }
}

// Handle a proposal/accept reply
static inline void handle_prop_or_acc_rep(cp_rmw_rep_mes_t *rep_mes,
                                          cp_rmw_rep_t *rep,
                                          loc_entry_t *loc_entry,
                                          bool is_accept,
                                          const uint16_t t_id)
{
  rmw_rep_info_t *rep_info = &loc_entry->rmw_reps;
  checks_when_handling_prop_acc_rep(loc_entry, rep, is_accept, t_id);
  bookkeeping_for_rep_info(rep_info, rep);
  handle_rmw_rep_based_on_opcode(rep_mes, loc_entry, rep, rep_info, is_accept, t_id);
  check_handle_rmw_rep_end(loc_entry, is_accept);
}


// Handle one accept or propose reply
static inline void find_local_and_handle_rmw_rep(loc_entry_t *loc_entry_array,
                                                 cp_rmw_rep_t *rep,
                                                 cp_rmw_rep_mes_t *rep_mes,
                                                 uint16_t byte_ptr,
                                                 bool is_accept,
                                                 uint16_t r_rep_i,
                                                 uint16_t t_id)
{
  check_find_local_and_handle_rmw_rep(loc_entry_array, rep, rep_mes,
                                      byte_ptr, is_accept, r_rep_i, t_id);

  int entry_i = search_prop_entries_with_l_id(loc_entry_array, (uint8_t) (is_accept ? ACCEPTED : PROPOSED),
                                              rep->l_id);
  if (entry_i == -1) return;
  loc_entry_t *loc_entry = &loc_entry_array[entry_i];
  handle_prop_or_acc_rep(rep_mes, rep, loc_entry, is_accept, t_id);
}

// Handle read replies that refer to RMWs (either replies to accepts or proposes)
inline void handle_rmw_rep_replies(loc_entry_t *loc_entry_array,
                                   cp_rmw_rep_mes_t *r_rep_mes,
                                   bool is_accept, uint16_t t_id)
{
  cp_rmw_rep_mes_t *rep_mes = (cp_rmw_rep_mes_t *) r_rep_mes;
  check_state_with_allowed_flags(4, r_rep_mes->opcode, ACCEPT_REPLY,
                                 PROP_REPLY, ACCEPT_REPLY_NO_CREDITS);
  uint8_t rep_num = rep_mes->coalesce_num;

  uint16_t byte_ptr = RMW_REP_MES_HEADER; // same for both accepts and replies
  for (uint16_t r_rep_i = 0; r_rep_i < rep_num; r_rep_i++) {
    cp_rmw_rep_t *rep = (cp_rmw_rep_t *) (((void *) rep_mes) + byte_ptr);
    find_local_and_handle_rmw_rep(loc_entry_array, rep, rep_mes, byte_ptr, is_accept, r_rep_i, t_id);
    byte_ptr += get_size_from_opcode(rep->opcode);
  }
  r_rep_mes->opcode = INVALID_OPCODE;
}


