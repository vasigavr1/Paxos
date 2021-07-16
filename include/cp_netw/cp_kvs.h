//
// Created by vasilis on 11/05/20.
//

#ifndef CP_KVS_UTILITY_H
#define CP_KVS_UTILITY_H


#include <stdint-gcc.h>

typedef struct context context_t;
typedef struct cp_ctx cp_ctx_t;
typedef struct trace_op trace_op_t;

void cp_KVS_batch_op_trace(uint16_t op_num,
                           trace_op_t *op,
                           cp_ctx_t *cp_ctx,
                           uint16_t t_id);

void cp_KVS_batch_op_props(context_t *ctx);
void cp_KVS_batch_op_accs(context_t *ctx);
void cp_KVS_batch_op_coms(context_t *ctx);

#endif //CP_KVS_UTILITY_H
