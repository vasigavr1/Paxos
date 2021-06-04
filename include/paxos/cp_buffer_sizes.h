//
// Created by vasilis on 22/05/20.
//

#ifndef CP_BUFFER_SIZES_H
#define CP_BUFFER_SIZES_H


// BUFFER SIZES
/*For reads and writes we want more slots than RECV_WRS because of multicasts:
 * if the machine sleeps, it will keep "receiving" messages. If the buffer space is smaller
 * than the receives_wrs, then the buffer will get corrupted!*/
#define PROP_BUF_SLOTS MAX((REM_MACH_NUM * PROP_CREDITS), (MAX_RECV_PROP_WRS + 1))
#define PROP_BUF_SIZE (PROP_RECV_SIZE * R_BUF_SLOTS)

#define PROP_REP_BUF_SLOTS ((REM_MACH_NUM * PROP_CREDITS))
#define PROP_REP_BUF_SIZE (PROP_REP_RECV_SIZE * PROP_REP_BUF_SLOTS)

#define W_BUF_SLOTS MAX((REM_MACH_NUM * W_CREDITS), (MAX_RECV_W_WRS + 1))
#define W_BUF_SIZE (W_RECV_SIZE * W_BUF_SLOTS)

#define ACK_BUF_SLOTS (REM_MACH_NUM * W_CREDITS)





#endif //CP_BUFFER_SIZES_H
