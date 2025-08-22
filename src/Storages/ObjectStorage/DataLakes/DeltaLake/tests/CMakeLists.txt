# ClickHouse Delta Lake S3 Optimization Tests

# Only build tests if Delta Lake and S3 support are enabled
if (USE_DELTA_KERNEL_RS AND USE_AWS_S3)
    # Find required dependencies
    find_package(GTest REQUIRED)
    
    # Define test sources
    set(DELTALAKE_S3_TEST_SOURCES
        gtest_s3_configuration_optimizer.cpp
        gtest_table_snapshot_optimized.cpp
        gtest_s3_iterator_optimized.cpp
        gtest_deltalake_s3_integration.cpp
    )
    
    # Create test executable
    add_executable(clickhouse_deltalake_s3_tests ${DELTALAKE_S3_TEST_SOURCES})
    
    # Link required libraries
    target_link_libraries(clickhouse_deltalake_s3_tests
        PRIVATE
            clickhouse_storages_deltalake
            clickhouse_common_io
            clickhouse_common_config
            clickhouse_interpreters
            GTest::gtest
            GTest::gtest_main
            ${AWS_SDK_LINK_LIBRARIES}
    )
    
    # Include directories
    target_include_directories(clickhouse_deltalake_s3_tests
        PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/../..
            ${CMAKE_SOURCE_DIR}/src
            ${CMAKE_SOURCE_DIR}/base
    )
    
    # Set compile definitions
    target_compile_definitions(clickhouse_deltalake_s3_tests
        PRIVATE
            USE_DELTA_KERNEL_RS=1
            USE_AWS_S3=1
    )
    
    # Add test to CTest
    add_test(
        NAME clickhouse_deltalake_s3_optimization_tests
        COMMAND clickhouse_deltalake_s3_tests
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    )
    
    # Set test properties
    set_tests_properties(clickhouse_deltalake_s3_optimization_tests
        PROPERTIES
            LABELS "deltalake;s3;optimization"
            TIMEOUT 300  # 5 minutes timeout
    )
    
    # Performance benchmark executable (optional)
    add_executable(clickhouse_deltalake_s3_benchmark
        benchmark_s3_optimization.cpp
    )
    
    target_link_libraries(clickhouse_deltalake_s3_benchmark
        PRIVATE
            clickhouse_storages_deltalake
            clickhouse_common_io
            clickhouse_common_config
            clickhouse_interpreters
            ${AWS_SDK_LINK_LIBRARIES}
    )
    
    target_include_directories(clickhouse_deltalake_s3_benchmark
        PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/../..
            ${CMAKE_SOURCE_DIR}/src
            ${CMAKE_SOURCE_DIR}/base
    )
    
    target_compile_definitions(clickhouse_deltalake_s3_benchmark
        PRIVATE
            USE_DELTA_KERNEL_RS=1
            USE_AWS_S3=1
    )

endif()

# Install test data if needed
if (EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/test_data)
    install(DIRECTORY test_data/
        DESTINATION ${CMAKE_INSTALL_DATADIR}/clickhouse-test/deltalake_s3_optimization/
        COMPONENT clickhouse)
endif()
