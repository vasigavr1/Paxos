#include "cp_util.h"

void print_latency_stats(void);


static inline void get_all_wrkr_stats(stats_ctx_t *ctx)
{
  t_stats_t *curr_w_stats = ctx->curr_w_stats;
  t_stats_t *prev_w_stats = ctx->prev_w_stats;
  t_stats_t *all_per_t = ctx->all_per_t;
  for (int i = 0; i < WORKERS_PER_MACHINE; i++) {
    stats_per_thread((uint64_t *) &curr_w_stats[i],
                     (uint64_t *) &prev_w_stats[i],
                     (uint64_t *) ctx->all_aggreg,
                     (uint64_t *) &all_per_t[i],
                     sizeof(t_stats_t));
  }
  memcpy(prev_w_stats, curr_w_stats, WORKERS_PER_MACHINE * (sizeof(struct thread_stats)));
}

static inline void show_aggregate_stats(stats_ctx_t *ctx)
{
  t_stats_t *all_aggreg = ctx->all_aggreg;
  my_printf(green, "%u %.2f, canc: %.2f, all-aboard: %.2f \n",
            ctx->print_count,
            per_sec(ctx, all_aggreg->total_reqs),
            (double) all_aggreg->cancelled_rmws / (double) all_aggreg->total_reqs,
            per_sec(ctx, all_aggreg->all_aboard_rmws));
}

static inline void show_per_thread_stats(stats_ctx_t *ctx)
{
  t_stats_t *all_per_t = ctx->all_per_t;
  uint64_t total_reqs = ctx->all_aggreg->total_reqs;
  printf("---------------PRINT %d time elapsed %.2f---------------\n",
         ctx->print_count, ctx->seconds);
  my_printf(green, "SYSTEM MIOPS: %.2f \n",
            per_sec(ctx, total_reqs));

  for (int i = 0; i < WORKERS_PER_MACHINE; i++) {
    my_printf(cyan, "T%d: ", i);
    my_printf(yellow, "%.2f MIOPS, Ab %.2f/s, "
                      "P/S %.2f/s, A/S %.2f/s, C/S %.2f/s",
              per_sec(ctx, all_per_t[i].total_reqs),
              per_sec(ctx, all_per_t[i].all_aboard_rmws),
              per_sec(ctx, all_per_t[i].qp_stats[PROP_QP_ID].sent),
              per_sec(ctx, all_per_t[i].qp_stats[ACC_QP_ID].sent),
              per_sec(ctx, all_per_t[i].qp_stats[COM_QP_ID].sent));
    my_printf(yellow, ", BATCHES: "
                      "Acks %.2f, Props %.2f, Accs %.2f, Coms %.2f",
              get_batch(ctx, &all_per_t[i].qp_stats[ACK_QP_ID]),
              get_batch(ctx, &all_per_t[i].qp_stats[PROP_QP_ID]),
              get_batch(ctx, &all_per_t[i].qp_stats[ACC_QP_ID]),
              get_batch(ctx, &all_per_t[i].qp_stats[COM_QP_ID]));
    printf("\n");
  }
  printf("\n");
  printf("---------------------------------------\n");
  my_printf(green, "SYSTEM MIOPS: %.2f \n",
            per_sec(ctx, total_reqs));
}


void cp_stats(stats_ctx_t *ctx)
{
  get_all_wrkr_stats(ctx);

  if (SHOW_AGGREGATE_STATS)
    show_aggregate_stats(ctx);
  else {
    show_per_thread_stats(ctx);
  }
  memset(ctx->all_aggreg, 0, sizeof(t_stats_t));

}



