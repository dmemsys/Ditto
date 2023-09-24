list(APPEND source_dmc
    dmc_utils.cc
    dmc_table.cc
    ib.cc
    nm.cc
    server_mm.cc
    client_mm.cc
    server.cc
    client.cc
    priority.cc
    rlist.cc
    fifo_history.cc
)

add_library(libdmc SHARED ${source_dmc})

target_compile_definitions(libdmc PUBLIC
    HASH_BUCKET_ASSOC_NUM=${hash_bucket_assoc}LL
    HASH_NUM_BUCKETS=${hash_bucket_num}LL)

if(use_shard_pqueue)
    target_compile_definitions(libdmc PUBLIC USE_SHARD_PQUEUE NUM_PQUEUE_SHARDS=${num_pqueue_shards})
endif()

if(use_lock_backoff)
    target_compile_definitions(libdmc PUBLIC USE_LOCK_BACKOFF)
endif()

if(ela_mem_tpt)
    target_compile_definitions(libdmc PUBLIC ELA_MEM_TPT)
endif()

if(use_fiber)
    target_compile_definitions(libdmc PUBLIC USE_FIBER)
endif()

if(use_penalty)
    target_compile_definitions(libdmc PUBLIC USE_PENALTY)
endif()

if(multi_policy)
    target_compile_definitions(libdmc PUBLIC MULTI_POLICY)
endif()

target_compile_options(
    libdmc

    # PRIVATE
    PUBLIC

    # ${CMAKE_CXX_FLAGS_DEBUG}
    # "-g"
    # "-D_DEBUG"
    "-O2"
)

target_link_libraries(libdmc PUBLIC ${Boost_LIBRARIES} boost_context boost_fiber ibverbs)
