//
// Created by vasilis on 11/05/20.
//

#ifndef CP_GENERIC_UTILITY_H
#define CP_GENERIC_UTILITY_H


#include <stdint.h>
#include <od_netw_func.h>
#include <od_generic_inline_util.h>
#include <cp_netw_structs.h>
#include <cp_messages.h>





// Generic CAS
static inline bool cas_a_state(atomic_uint_fast8_t * state, uint8_t old_state, uint8_t new_state, uint16_t t_id)
{
  return atomic_compare_exchange_strong(state, (atomic_uint_fast8_t *) &old_state,
                                        (atomic_uint_fast8_t) new_state);
}


/* ---------------------------------------------------------------------------
//------------------------------ OPCODE HANDLING----------------------------
//---------------------------------------------------------------------------*/

static inline bool opcode_is_rmw(uint8_t opcode)
{
  return opcode == FETCH_AND_ADD || opcode == COMPARE_AND_SWAP_WEAK ||
         opcode == COMPARE_AND_SWAP_STRONG || opcode == RMW_PLAIN_WRITE;
}






// Returns the capacity of a write request given an opcode -- Accepts, commits, writes, releases
static inline uint16_t get_write_size_from_opcode(uint8_t opcode) {
  switch(opcode) {
    case OP_RELEASE:
    case OP_ACQUIRE:
    case KVS_OP_PUT:
    case OP_RELEASE_BIT_VECTOR:
    case OP_RELEASE_SECOND_ROUND:
    case NO_OP_RELEASE:
      assert(false);
    case ACCEPT_OP:
    case ACCEPT_OP_BIT_VECTOR:
      return ACC_SIZE;
    case COMMIT_OP:
    case RMW_ACQ_COMMIT_OP:
      return COM_SIZE;
    case COMMIT_OP_NO_VAL:
      return COMMIT_NO_VAL_SIZE;
    default: if (ENABLE_ASSERTIONS) assert(false);
  }
}
/* ---------------------------------------------------------------------------
//------------------------------ PRINTS---------------------------------------
//---------------------------------------------------------------------------*/






/* ---------------------------------------------------------------------------
//------------------------------ KV-PTR writes---------------------------------------
//---------------------------------------------------------------------------*/

static inline mica_key_t* key_ptr_of_rmw_op(void **ops,
                                            uint16_t op_i,
                                            bool is_accept)
{
  return is_accept?
         &(((cp_acc_t **) ops)[op_i]->key) :
         &(((cp_prop_t **) ops)[op_i]->key);
}

static inline mica_key_t key_of_rmw_op(void **ops,
                                       uint16_t op_i,
                                       bool is_accept)
{
  return is_accept?
         ((cp_acc_t **) ops)[op_i]->key :
         ((cp_prop_t **) ops)[op_i]->key;
}

static inline void fill_ptr_to_ops_for_reps(cp_ptrs_to_ops_t *polled_messages,
                                            void* op,
                                            void *op_mes,
                                            uint16_t i)
{
  polled_messages->ptr_to_ops[polled_messages->polled_ops] = op;
  polled_messages->ptr_to_mes[polled_messages->polled_ops] = op_mes;
  polled_messages->break_message[polled_messages->polled_ops] = i == 0;
  polled_messages->polled_ops++;
}





#endif //CP_GENERIC_UTILITY_H
