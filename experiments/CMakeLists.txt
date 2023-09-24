add_executable(init init.cc run_client.cc memcached.cc workload.cc)
add_executable(simulator simulator.cc workload.cc)

# add_executable(ada_simulator adaptive_simulator.cc workload.cc)
add_executable(compile_opt_checker compile_opt_checker.cc)
add_executable(redis_perf redis_test/redis_perf.cc memcached.cc)
add_executable(redis_perf_tpt redis_test/redis_perf_tpt.cc memcached.cc)

target_link_libraries(
    init
    libdmc
    pthread
    memcached
)

target_link_libraries(
    simulator
    libdmc
    pthread
    memcached
)

target_link_libraries(
    redis_perf
    redis++
    hiredis
    pthread
    memcached
)

target_link_libraries(
    redis_perf_tpt
    redis++
    hiredis
    pthread
    memcached
)

target_link_libraries(
    compile_opt_checker
    libdmc
)

# target_link_libraries(
# ada_simulator
# libdmc
# pthread
# memcached
# )
configure_file(
    ${CMAKE_SOURCE_DIR}/experiments/controller.py
    ${CMAKE_CURRENT_BINARY_DIR}/controller.py
)