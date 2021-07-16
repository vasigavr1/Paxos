#ifndef CP_UTILS_H
#define CP_UTILS_H


#include "cp_main.h"


typedef struct cp_ctx cp_ctx_t;
typedef struct trace_op trace_op_t;
typedef struct context context_t;

extern uint64_t seed;
void cp_static_assert_compile_parameters();
void cp_print_parameters_in_the_start();
void cp_init_globals();


void cp_init_qp_meta(context_t *ctx);
// Initialize the struct that holds all pending ops
cp_ctx_t* cp_set_up_pending_ops(context_t *ctx);

void randomize_op_values(trace_op_t *ops, uint16_t t_id);


/* ---------------------------------------------------------------------------
------------------------------UTILITY --------------------------------------
---------------------------------------------------------------------------*/
void print_latency_stats(void);

void cp_init_functionality(int argc, char *argv[]);


#endif /* CP_UTILS_H */
