//
// Created by vasilis on 16/07/2021.
//

#ifndef ODYSSEY_CP_NETW_INTERFACE_H
#define ODYSSEY_CP_NETW_INTERFACE_H

#include <stdbool.h>
#include <stdint-gcc.h>
typedef struct mica_op mica_op_t;

void cp_rmw_rep_insert(void *ctx,
                       mica_op_t **kv_ptr,
                       uint32_t op_i,
                       bool is_accept);


void cp_prop_insert(void *ctx,
                    void *loc_entry);

void cp_acc_insert(void *ctx,
                   void *loc_entry,
                   bool helping);

bool cp_com_insert(void *ctx,
                   void *loc_entry,
                   uint32_t state);



#endif //ODYSSEY_CP_NETW_INTERFACE_H
