//
// Created by vasilis on 16/07/2021.
//

#ifndef ODYSSEY_CP_STATS_H
#define ODYSSEY_CP_STATS_H

typedef struct thread_stats t_stats_t;

void cp_dump_stats_2_file(t_stats_t* st);
void print_latency_stats(void);

#endif //ODYSSEY_CP_STATS_H
