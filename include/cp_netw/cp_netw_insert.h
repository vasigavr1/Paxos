//
// Created by vasilis on 16/07/2021.
//

#ifndef ODYSSEY_CP_NETW_INSERT_H
#define ODYSSEY_CP_NETW_INSERT_H

#include <stdint-gcc.h>


typedef struct context context_t;


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

#endif //ODYSSEY_CP_NETW_INSERT_H
