#include <stdio.h>

int main() {
#ifdef USE_SHARD_PQUEUE
  printf("USE_SHARD_PQUEUE\n");
  printf("NUM_PQUEUE_SHARDS: %d\n", NUM_PQUEUE_SHARDS);
#endif
#ifdef USE_LOCK_BACKOFF
  printf("USE_LOCK_BACKOFF\n");
#endif
#ifdef ELA_MEM_TPT
  printf("ELA_MEM_TPT\n");
#endif
#ifdef USE_FIBER
  printf("USE_FIBER\n");
#endif
#ifdef USE_PENALTY
  printf("USE_PENALTY\n");
#endif
#ifdef MULTI_POLICY
  printf("MULTI_POLICY\n");
#endif
  printf("HASH_BUCKET_ASSOC_NUM %d\n", HASH_BUCKET_ASSOC_NUM);
  printf("HASH_NUM_BUCKETS %d\n", HASH_NUM_BUCKETS);
}