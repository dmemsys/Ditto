cmake_minimum_required(VERSION 3.16 FATAL_ERROR)

project(DMC LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 17)

find_package(Boost REQUIRED)
include_directories(src)

set(CMAKE_BUILD_TYPE Debug)

set(use_shard_pqueue OFF CACHE BOOL "set USE_SHARD_PQUEUE")
set(num_pqueue_shards 32 CACHE STRING "set NUM_PQUEUE_SHARDS")
set(use_lock_backoff ON CACHE BOOL "set USE_LOCK_BACKOFF")
set(ela_mem_tpt OFF CACHE BOOL "set ELA_MEM_TPT")
set(use_fiber OFF CACHE BOOL "set USE_FIBER")
set(use_penalty ON CACHE BOOL "set USE_PENALTY")
set(multi_policy OFF CACHE BOOL "set MULTI_POLICY")
set(hash_bucket_assoc 8 CACHE STRING "set HASH_BUCKET_ASSOC_NUM")
set(hash_bucket_num 17170432 CACHE STRING "set HASH_NUM_BUCKETS")

if(use_shard_pqueue)
    message("set USE_SHARD_PQUEUE")
    message("num_pqueue_shards=${num_pqueue_shards}")
else()
    message("unset USE_SHARD_PQUEUE")
endif()

if(use_lock_backoff)
    message("set USE_LOCK_BACKOFF")
else()
    message("unset USE_LOCK_BACKOFF")
endif()

if(ela_mem_tpt)
    message("set ELA_MEM_TPT")
else()
    message("unset ELA_MEM_TPT")
endif()

if(use_fiber)
    message("set USE_FIBER")
else()
    message("unset USE_FIBER")
endif()

if(use_penalty)
    message("set USE_PENALTY")
else()
    message("unset USE_PENALTY")
endif()

if(multi_policy)
    message("set MULTI_POLICY")
else()
    message("unset MULTI_POLICY")
endif()

message("hash_bucket_assoc=${hash_bucket_assoc}")
message("hash_bucket_num=${hash_bucket_num}")

add_subdirectory(src)
add_subdirectory(experiments)
enable_testing()
add_subdirectory(tests)