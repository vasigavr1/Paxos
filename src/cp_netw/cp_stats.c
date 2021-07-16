//#include "cp_util.h"
#include "cp_stats.h"
#include "cp_main.h"

void print_latency_stats(void);



static inline void show_aggregate_stats(stats_ctx_t *ctx)
{
  t_stats_t *all_aggreg = ctx->all_aggreg;
  my_printf(green, "%u %.2f, all-aboard: %.2f, TOT/AB: %lu/%lu, "
                   "props %lu, canc: %.2f \n",
            ctx->print_count,
            per_sec(ctx, all_aggreg->total_reqs),
            per_sec(ctx, all_aggreg->all_aboard_rmws),
            all_aggreg->total_reqs, all_aggreg->all_aboard_rmws,
            all_aggreg->qp_stats[PROP_QP_ID].sent,
            (double) all_aggreg->cancelled_rmws /
            (double) all_aggreg->total_reqs);
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
                      "Props %.2f, Accs %.2f, Coms %.2f,"
                      " Reps %.2f, Acks %.2f",
              get_batch(ctx, &all_per_t[i].qp_stats[PROP_QP_ID]),
              get_batch(ctx, &all_per_t[i].qp_stats[ACC_QP_ID]),
              get_batch(ctx, &all_per_t[i].qp_stats[COM_QP_ID]),
              get_batch(ctx, &all_per_t[i].qp_stats[RMW_REP_QP_ID]),
              get_batch(ctx, &all_per_t[i].qp_stats[ACK_QP_ID]));
    printf("\n");
  }
  printf("\n");
  printf("---------------------------------------\n");
  my_printf(green, "SYSTEM MIOPS: %.2f \n",
            per_sec(ctx, total_reqs));
}


void cp_stats(stats_ctx_t *ctx)
{
  get_all_wrkr_stats(ctx, WORKERS_PER_MACHINE, sizeof(t_stats_t));
  memcpy(ctx->prev_w_stats, ctx->curr_w_stats, WORKERS_PER_MACHINE * (sizeof(t_stats_t)));

  if (SHOW_AGGREGATE_STATS)
    show_aggregate_stats(ctx);
  else {
    show_per_thread_stats(ctx);
  }
  memset(ctx->all_aggreg, 0, sizeof(t_stats_t));

}



void cp_dump_stats_2_file(t_stats_t* st){
  uint8_t typeNo = 0;
  assert(typeNo >=0 && typeNo <=3);
  int i = 0;
  char filename[128];
  FILE *fp;
  double total_MIOPS;
  char* path = "../../results/scattered-results";

  sprintf(filename, "%s/%s_s_%d_v_%d_m_%d_w_%d_r_%d-%d.csv", path,
          "CP",
          SESSIONS_PER_THREAD,
          USE_BIG_OBJECTS == 1 ? ((EXTRA_CACHE_LINES * 64) + BASE_VALUE_SIZE): BASE_VALUE_SIZE,
          MACHINE_NUM, WORKERS_PER_MACHINE,
          WRITE_RATIO,
          machine_id);
  printf("%s\n", filename);
  fp = fopen(filename, "w"); // "w" means that we are going to write on this file
  fprintf(fp, "machine_id: %d\n", machine_id);

  fprintf(fp, "comment: thread ID, total MIOPS,"
              "reads sent, read-replies sent, acks sent, "
              "received read-replies, received reads, received acks\n");
  //for(i = 0; i < WORKERS_PER_MACHINE; ++i){
  //    total_MIOPS = st->total_reqs[i];
  //    fprintf(fp, "client: %d, %.2f, %.2f, %.2f, %.2f, %.2f, %.2f, %.2f, %.2f\n",
  //            i, total_MIOPS, st->total_reqs[i], st->reads_sent[i],
  //            st->r_reps_sent[i], st->acks_sent[i],
  //            st->received_r_reps[i],st->received_reads[i],
  //            st->received_acks[i]);
  //}

  fclose(fp);
}