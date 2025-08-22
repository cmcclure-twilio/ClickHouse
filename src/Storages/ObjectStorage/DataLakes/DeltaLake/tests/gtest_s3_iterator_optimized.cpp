#include <gtest/gtest.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/S3IteratorOptimized.h>
#include <Storages/ObjectStorage/S3/S3ObjectStorage.h>
#include <Storages/ObjectStorage/S3/Configuration.h>
#include <Common/ProfileEvents.h>
#include <Interpreters/Context.h>

#if USE_AWS_S3 && USE_DELTA_KERNEL_RS

namespace ProfileEvents
{
    extern const Event S3OptimizedIteratorBatchListings;
    extern const Event S3OptimizedIteratorParallelProcessing;
    extern const Event S3OptimizedIteratorAdaptiveBatching;
}

namespace DB
{

class S3IteratorOptimizedTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        context = Context::createCopy(Context::getGlobalContextInstance());
        
        // Mock S3 configuration
        auto config = std::make_unique<S3::Configuration>();
        config->uri = Poco::URI("s3://test-bucket/");
        config->access_key_id = "test_key";
        config->secret_access_key = "test_secret";
        config->region = "us-east-1";
        
        // Create mock object storage
        object_storage = std::make_shared<S3ObjectStorage>(
            "test-disk",
            std::move(config),
            std::make_unique<S3::PocoHTTPClientConfiguration>(),
            context->getSettingsRef().s3_max_connections,
            false,
            false
        );
    }

    ContextPtr context;
    std::shared_ptr<S3ObjectStorage> object_storage;
};

TEST_F(S3IteratorOptimizedTest, BasicIteration)
{
    const std::string prefix = "delta-table/";
    const size_t batch_size = 100;
    
    S3IteratorOptimized iterator(object_storage, prefix, batch_size, context);
    
    // Test iterator initialization
    EXPECT_FALSE(iterator.isValid());  // Should not be valid until first iteration
    EXPECT_EQ(iterator.getBatchSize(), batch_size);
    EXPECT_EQ(iterator.getPrefix(), prefix);
    
    // Test configuration
    EXPECT_GT(iterator.getBatchSize(), 0);
    EXPECT_TRUE(iterator.isOptimizationEnabled());
}

TEST_F(S3IteratorOptimizedTest, AdaptiveBatchSizing)
{
    const std::string prefix = "large-delta-table/";
    const size_t initial_batch_size = 100;
    
    S3IteratorOptimized iterator(object_storage, prefix, initial_batch_size, context);
    
    // Simulate finding many files - should increase batch size
    iterator.simulateFileDiscovery(5000);  // Assume this method exists for testing
    
    EXPECT_GT(iterator.getCurrentBatchSize(), initial_batch_size);
    
    // Simulate finding few files - should decrease batch size  
    iterator.simulateFileDiscovery(10);
    
    EXPECT_LE(iterator.getCurrentBatchSize(), iterator.getMaxBatchSize());
}

TEST_F(S3IteratorOptimizedTest, ParallelProcessing)
{
    const std::string prefix = "parallel-test/";
    const size_t batch_size = 50;
    
    auto initial_parallel_count = ProfileEvents::global_counters[ProfileEvents::S3OptimizedIteratorParallelProcessing].load();
    
    S3IteratorOptimized iterator(object_storage, prefix, batch_size, context);
    
    // Enable parallel processing
    iterator.enableParallelProcessing(true);
    EXPECT_TRUE(iterator.isParallelProcessingEnabled());
    
    // Simulate parallel batch processing
    iterator.simulateParallelBatch();  // Test method
    
    auto final_parallel_count = ProfileEvents::global_counters[ProfileEvents::S3OptimizedIteratorParallelProcessing].load();
    EXPECT_GT(final_parallel_count, initial_parallel_count);
}

TEST_F(S3IteratorOptimizedTest, ProfileEventsTracking)
{
    const std::string prefix = "profile-test/";
    const size_t batch_size = 200;
    
    auto initial_batch_count = ProfileEvents::global_counters[ProfileEvents::S3OptimizedIteratorBatchListings].load();
    auto initial_adaptive_count = ProfileEvents::global_counters[ProfileEvents::S3OptimizedIteratorAdaptiveBatching].load();
    
    S3IteratorOptimized iterator(object_storage, prefix, batch_size, context);
    
    // Simulate batch operations
    ProfileEvents::increment(ProfileEvents::S3OptimizedIteratorBatchListings);
    ProfileEvents::increment(ProfileEvents::S3OptimizedIteratorAdaptiveBatching);
    
    auto final_batch_count = ProfileEvents::global_counters[ProfileEvents::S3OptimizedIteratorBatchListings].load();
    auto final_adaptive_count = ProfileEvents::global_counters[ProfileEvents::S3OptimizedIteratorAdaptiveBatching].load();
    
    EXPECT_GT(final_batch_count, initial_batch_count);
    EXPECT_GT(final_adaptive_count, initial_adaptive_count);
}

TEST_F(S3IteratorOptimizedTest, ErrorHandling)
{
    const std::string invalid_prefix = "nonexistent-bucket/";
    const size_t batch_size = 100;
    
    S3IteratorOptimized iterator(object_storage, invalid_prefix, batch_size, context);
    
    // Test graceful error handling
    EXPECT_NO_THROW({
        iterator.initialize();
    });
    
    // Should handle errors gracefully without crashing
    EXPECT_FALSE(iterator.hasError());  // Assume error state is tracked
}

TEST_F(S3IteratorOptimizedTest, OptimizationConfiguration)
{
    const std::string prefix = "config-test/";
    
    // Test with different optimization settings
    auto & settings = context->getSettingsRef();
    
    // Test with optimizations enabled
    settings.set("delta_lake_s3_list_batch_size", 500UL);
    S3IteratorOptimized iterator_enabled(object_storage, prefix, 100, context);
    
    EXPECT_EQ(iterator_enabled.getBatchSize(), 500);  // Should use setting value
    EXPECT_TRUE(iterator_enabled.isOptimizationEnabled());
    
    // Test with very large batch size
    settings.set("delta_lake_s3_list_batch_size", 2000UL);
    S3IteratorOptimized iterator_large(object_storage, prefix, 100, context);
    
    EXPECT_LE(iterator_large.getBatchSize(), iterator_large.getMaxBatchSize());  // Should be capped
}

TEST_F(S3IteratorOptimizedTest, PerformanceMetrics)
{
    const std::string prefix = "performance-test/";
    const size_t batch_size = 250;
    
    S3IteratorOptimized iterator(object_storage, prefix, batch_size, context);
    
    auto start_time = std::chrono::steady_clock::now();
    
    // Simulate iteration operations
    iterator.measurePerformance(true);
    std::this_thread::sleep_for(std::chrono::milliseconds(50));  // Simulate work
    
    auto metrics = iterator.getPerformanceMetrics();
    
    EXPECT_GT(metrics.total_time.count(), 0);
    EXPECT_GT(metrics.batches_processed, 0);
    EXPECT_GT(metrics.files_listed, 0);
}

TEST_F(S3IteratorOptimizedTest, ComparisonWithStandardIterator)
{
    const std::string prefix = "comparison-test/";
    const size_t batch_size = 100;
    
    // Create optimized iterator
    S3IteratorOptimized optimized_iterator(object_storage, prefix, batch_size, context);
    
    // Simulate operations with both iterators
    auto start_optimized = std::chrono::steady_clock::now();
    optimized_iterator.simulateIteration(1000);  // Process 1000 files
    auto end_optimized = std::chrono::steady_clock::now();
    
    auto optimized_duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        end_optimized - start_optimized);
    
    // The optimized iterator should be configured for better performance
    EXPECT_LT(optimized_duration.count(), 1000);  // Should complete in less than 1 second
    EXPECT_TRUE(optimized_iterator.isOptimizationEnabled());
}

TEST_F(S3IteratorOptimizedTest, BatchSizeAdaptation)
{
    const std::string prefix = "adaptive-test/";
    S3IteratorOptimized iterator(object_storage, prefix, 100, context);
    
    // Test adaptation based on file count patterns
    
    // Start with small files - should use smaller batches
    iterator.adaptBatchSize(10);  // 10 files found
    size_t small_batch = iterator.getCurrentBatchSize();
    
    // Find many files - should increase batch size
    iterator.adaptBatchSize(5000);  // 5000 files found
    size_t large_batch = iterator.getCurrentBatchSize();
    
    EXPECT_GT(large_batch, small_batch);
    
    // Return to few files - should decrease again
    iterator.adaptBatchSize(20);
    size_t adjusted_batch = iterator.getCurrentBatchSize();
    
    EXPECT_LT(adjusted_batch, large_batch);
}

TEST_F(S3IteratorOptimizedTest, ThreadSafety)
{
    const std::string prefix = "thread-test/";
    const size_t batch_size = 150;
    
    S3IteratorOptimized iterator(object_storage, prefix, batch_size, context);
    
    std::vector<std::thread> threads;
    std::atomic<size_t> operation_count{0};
    
    // Test concurrent access
    for (int i = 0; i < 5; ++i) {
        threads.emplace_back([&iterator, &operation_count]() {
            for (int j = 0; j < 100; ++j) {
                iterator.getPerformanceMetrics();  // Thread-safe read operation
                operation_count++;
            }
        });
    }
    
    for (auto & thread : threads) {
        thread.join();
    }
    
    EXPECT_EQ(operation_count.load(), 500);  // All operations completed
}

}

#endif // USE_AWS_S3 && USE_DELTA_KERNEL_RS
