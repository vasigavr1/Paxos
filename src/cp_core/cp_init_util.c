//
// Created by vasilis on 16/07/2021.
//

#include <cp_core_interface.h>
#include <cp_core_common_util.h>

FILE* rmw_verify_fp[WORKERS_PER_MACHINE];

void open_rmw_log_files(uint16_t t_id)
{
  if (VERIFY_PAXOS || PRINT_LOGS || COMMIT_LOGS) {
    char fp_name[50];
    my_printf(green, "WILL PRINT LOGS IN THIS RUN \n");
    sprintf(fp_name, "../paxos/src/PaxosVerifier/thread%u.out",  t_id);
    rmw_verify_fp[t_id] = fopen(fp_name, "w+");
    assert(rmw_verify_fp[t_id] != NULL);
  }
}


loc_entry_t *cp_init_loc_entry(uint16_t t_id)
{
  loc_entry_t *rmw_entries = calloc(LOCAL_PROP_NUM, sizeof(loc_entry_t));
  for (uint32_t i = 0; i < LOCAL_PROP_NUM; i++) {
    loc_entry_t *loc_entry = &rmw_entries[i];
    loc_entry->sess_id = (uint16_t) i;
    loc_entry->glob_sess_id = get_glob_sess_id((uint8_t)machine_id, t_id, (uint16_t) i);
    loc_entry->l_id = (uint64_t) loc_entry->sess_id;
    loc_entry->rmw_id.id = (uint64_t) loc_entry->glob_sess_id;
    loc_entry->help_rmw = (struct rmw_help_entry *) calloc(1, sizeof(struct rmw_help_entry));
    loc_entry->help_loc_entry = (loc_entry_t *) calloc(1, sizeof(loc_entry_t));
    loc_entry->help_loc_entry->sess_id = (uint16_t) i;
    loc_entry->help_loc_entry->helping_flag = IS_HELPER;
    loc_entry->help_loc_entry->glob_sess_id = loc_entry->glob_sess_id;
    loc_entry->state = INVALID_RMW;
  }
  return rmw_entries;
}

cp_core_ctx_t* cp_init_cp_core_ctx(void *cp_ctx,
                                   sess_stall_t *stall_info,
                                   void *ctx,
                                   uint16_t t_id)
{
  open_rmw_log_files(t_id);
  cp_core_ctx_t *cp_core_ctx = calloc(1, sizeof(cp_core_ctx_t));
  cp_core_ctx->rmw_entries = cp_init_loc_entry(t_id);
  cp_core_ctx->appl_ctx = (void *) cp_ctx;
  cp_core_ctx->stall_info = stall_info;
  cp_core_ctx->netw_ctx = (void *) ctx;
  cp_core_ctx->t_id = t_id;
  return cp_core_ctx;
}
