find_package(GTest REQUIRED)

include_directories(${GTEST_INCLUDE_DIRS})

add_library(dmctest dmc_test.cc)

add_executable(test_dmc_utils test_dmc_utils.cc)
add_executable(test_nm test_nm.cc)
add_executable(test_server_mm test_server_mm.cc)
add_executable(test_server test_server.cc)
add_executable(test_client_mm test_client_mm.cc)
add_executable(test_client test_client.cc)
add_executable(test_cms test_cms.cc)
add_executable(test_rlist test_rlist.cc test_nm.cc)

target_link_libraries(test_dmc_utils
    ${GTEST_BOTH_LIBRARIES}
    libdmc
    pthread
    dmctest
)

target_link_libraries(test_nm
    ${GTEST_BOTH_LIBRARIES}
    libdmc
    pthread
    dmctest
)

target_link_libraries(test_server_mm
    ${GTEST_BOTH_LIBRARIES}
    libdmc
    pthread
    dmctest
)

target_link_libraries(test_server
    ${GTEST_BOTH_LIBRARIES}
    libdmc
    pthread
    dmctest
)

target_link_libraries(test_client_mm
    ${GTEST_BOTH_LIBRARIES}
    libdmc
    pthread
    dmctest
)

target_link_libraries(test_client
    ${GTEST_BOTH_LIBRARIES}
    libdmc
    pthread
    dmctest
)

target_link_libraries(test_cms
    ${GTEST_BOTH_LIBRARIES}
    libdmc
    pthread
)

target_link_libraries(test_rlist
    ${GTEST_BOTH_LIBRARIES}
    libdmc
    dmctest
    pthread
)

configure_file(
    ${CMAKE_SOURCE_DIR}/tests/test_conf.json
    ${CMAKE_CURRENT_BINARY_DIR}/test_conf.json
    COPYONLY
)

configure_file(
    ${CMAKE_SOURCE_DIR}/tests/test_conf_client.json
    ${CMAKE_CURRENT_BINARY_DIR}/test_conf_client.json
    COPYONLY
)

configure_file(
    ${CMAKE_SOURCE_DIR}/tests/test_conf_server.json
    ${CMAKE_CURRENT_BINARY_DIR}/test_conf_server.json
    COPYONLY
)

target_compile_options(
    test_client
    # PRIVATE
    PUBLIC
    ${CMAKE_CXX_FLAGS_DEBUG}
    "-g"
    "-D_DEBUG"
)

gtest_discover_tests(
    test_dmc_utils
    test_nm
)