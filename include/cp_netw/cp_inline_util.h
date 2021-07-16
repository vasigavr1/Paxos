#ifndef CP_INLINE_UTIL_H
#define CP_INLINE_UTIL_H



#include "od_network_context.h"



void cp_send_ack_debug(context_t *ctx,
                       void* ack_ptr,
                       uint32_t m_i);
void send_props_helper(context_t *ctx);
void send_accs_helper(context_t *ctx);
void cp_send_coms_helper(context_t *ctx);
void rmw_prop_rep_helper(context_t *ctx);
void cp_send_ack_helper(context_t *ctx);
bool prop_recv_handler(context_t* ctx);
bool acc_recv_handler(context_t* ctx);
bool cp_rmw_rep_recv_handler(context_t* ctx);
bool cp_com_recv_handler(context_t* ctx);
bool cp_ack_recv_handler(context_t *ctx);


void cp_insert_com_help(context_t *ctx,
                        void* com_ptr,
                        void *source,
                        uint32_t source_flag);


void cp_insert_prop_help(context_t *ctx, void* prop_ptr,
                         void *source, uint32_t source_flag);

void cp_insert_rmw_rep_helper(context_t *ctx,
                              void* prop_rep_ptr,
                              void *kv_ptr, uint32_t source_flag);

void cp_insert_acc_help(context_t *ctx, void* acc_ptr,
                        void *source, uint32_t source_flag);

_Noreturn void cp_main_loop(context_t *ctx);
#endif /* CP_INLINE_UTIL_H */
