#ifndef CP_INLINE_UTIL_H
#define CP_INLINE_UTIL_H


#include <stdint-gcc.h>
#include <stdbool.h>

typedef struct context context_t;

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




_Noreturn void cp_main_loop(context_t *ctx);
#endif /* CP_INLINE_UTIL_H */
