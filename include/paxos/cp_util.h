#ifndef CP_UTILS_H
#define CP_UTILS_H


//#include "multicast.h"
#include "od_kvs.h"
#include "od_hrd.h"
#include "cp_main.h"
#include "cp_inline_util.h"
#include "od_network_context.h"
#include <od_init_func.h>

extern uint64_t seed;
void cp_static_assert_compile_parameters();
void cp_print_parameters_in_the_start();
void cp_init_globals();


/* ---------------------------------------------------------------------------
------------------------------STATS --------------------------------------
---------------------------------------------------------------------------*/
struct stats {
  double r_batch_size[WORKERS_PER_MACHINE];
  double r_rep_batch_size[WORKERS_PER_MACHINE];
  double ack_batch_size[WORKERS_PER_MACHINE];
  double write_batch_size[WORKERS_PER_MACHINE];
  double stalled_ack[WORKERS_PER_MACHINE];
  double stalled_r_rep[WORKERS_PER_MACHINE];
	double failed_rem_write[WORKERS_PER_MACHINE];
  double quorum_reads_per_thread[WORKERS_PER_MACHINE];

	double cache_hits_per_thread[WORKERS_PER_MACHINE];

	double writes_sent[WORKERS_PER_MACHINE];
	double reads_sent[WORKERS_PER_MACHINE];
	double acks_sent[WORKERS_PER_MACHINE];
	double proposes_sent[WORKERS_PER_MACHINE];
	double rmws_completed[WORKERS_PER_MACHINE];
	double accepts_sent[WORKERS_PER_MACHINE];
	double commits_sent[WORKERS_PER_MACHINE];

	double r_reps_sent[WORKERS_PER_MACHINE];
	double received_writes[WORKERS_PER_MACHINE];
	double received_reads[WORKERS_PER_MACHINE];
	double received_acks[WORKERS_PER_MACHINE];
	double received_r_reps[WORKERS_PER_MACHINE];
  double cancelled_rmws[WORKERS_PER_MACHINE];
	double all_aboard_rmws[WORKERS_PER_MACHINE];
	double reads_that_become_writes[WORKERS_PER_MACHINE];
  //double zookeeper_writes[WORKERS_PER_MACHINE];
};
void dump_stats_2_file(struct stats* st);
void print_latency_stats(void);


/* ---------------------------------------------------------------------------
-----------------------------------------------------------------------------
---------------------------------------------------------------------------*/


void cp_init_qp_meta(context_t *ctx);
// Initialize the struct that holds all pending ops
p_ops_t* cp_set_up_pending_ops(context_t *ctx);

void randomize_op_values(trace_op_t *ops, uint16_t t_id);


/* ---------------------------------------------------------------------------
------------------------------UTILITY --------------------------------------
---------------------------------------------------------------------------*/
void print_latency_stats(void);

static void cp_init_functionality(int argc, char *argv[])
{
  cp_print_parameters_in_the_start();
	od_generic_static_assert_compile_parameters();
  cp_static_assert_compile_parameters();
  od_generic_init_globals(QP_NUM);
  cp_init_globals();
  od_handle_program_inputs(argc, argv);
}


#endif /* CP_UTILS_H */
