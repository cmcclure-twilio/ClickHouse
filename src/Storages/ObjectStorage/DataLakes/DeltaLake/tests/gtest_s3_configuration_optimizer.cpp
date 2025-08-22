#include <gtest/gtest.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/S3ConfigurationOptimizer.h>
#include <Interpreters/Context.h>
#include <Core/Settings.h>

#if USE_AWS_S3 && USE_DELTA_KERNEL_RS

#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/auth/AWSCredentials.h>

namespace DB
{

class S3ConfigurationOptimizerTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        base_config = Aws::Client::ClientConfiguration();
        base_config.maxConnections = 25;  // Default AWS setting
        base_config.connectTimeoutMs = 3000;
        base_config.requestTimeoutMs = 30000;
    }

    Aws::Client::ClientConfiguration base_config;
};

TEST_F(S3ConfigurationOptimizerTest, OptimizeForDeltaLake_DefaultSettings)
{
    auto optimized = S3ConfigurationOptimizer::optimizeForDeltaLake(base_config, nullptr);
    
    // Should increase connection pool size
    EXPECT_GT(optimized.maxConnections, base_config.maxConnections);
    EXPECT_EQ(optimized.maxConnections, 50U);  // Default optimized setting
    
    // Should optimize timeouts
    EXPECT_LE(optimized.connectTimeoutMs, 5000);  // Faster connect timeout
    EXPECT_TRUE(optimized.enableTcpKeepAlive);
    EXPECT_EQ(optimized.tcpKeepAliveIntervalMs, 30000);
}

TEST_F(S3ConfigurationOptimizerTest, OptimizeForDeltaLake_DisabledOptimizations)
{
    // Create a mock context with optimizations disabled
    auto context = Context::createCopy(Context::getGlobalContextInstance());
    auto & settings = context->getSettingsRef();
    settings.set("delta_lake_enable_optimized_s3_client", false);
    
    auto optimized = S3ConfigurationOptimizer::optimizeForDeltaLake(base_config, context);
    
    // Should return unchanged configuration
    EXPECT_EQ(optimized.maxConnections, base_config.maxConnections);
    EXPECT_EQ(optimized.connectTimeoutMs, base_config.connectTimeoutMs);
}

TEST_F(S3ConfigurationOptimizerTest, OptimizeForDeltaLake_CustomSettings)
{
    auto context = Context::createCopy(Context::getGlobalContextInstance());
    auto & settings = context->getSettingsRef();
    
    // Set custom optimization values
    settings.set("delta_lake_enable_optimized_s3_client", true);
    settings.set("delta_lake_s3_max_connections", 75UL);
    settings.set("delta_lake_s3_request_timeout_ms", 20000UL);
    
    auto optimized = S3ConfigurationOptimizer::optimizeForDeltaLake(base_config, context);
    
    EXPECT_EQ(optimized.maxConnections, 75U);
    EXPECT_EQ(optimized.requestTimeoutMs, 20000L);
}

TEST_F(S3ConfigurationOptimizerTest, GetOptimalBatchSize)
{
    // Test default batch size
    auto default_size = S3ConfigurationOptimizer::getOptimalBatchSize(nullptr);
    EXPECT_EQ(default_size, 1000U);
    
    // Test custom batch size
    auto context = Context::createCopy(Context::getGlobalContextInstance());
    auto & settings = context->getSettingsRef();
    settings.set("delta_lake_s3_list_batch_size", 2000UL);
    
    auto custom_size = S3ConfigurationOptimizer::getOptimalBatchSize(context);
    EXPECT_EQ(custom_size, 2000U);
    
    // Test batch size limit
    settings.set("delta_lake_s3_list_batch_size", 5000UL);  // Above limit
    auto limited_size = S3ConfigurationOptimizer::getOptimalBatchSize(context);
    EXPECT_LE(limited_size, 2000U);  // Should be clamped to max
}

TEST_F(S3ConfigurationOptimizerTest, GetOptimalConnectionPoolSize)
{
    auto default_size = S3ConfigurationOptimizer::getOptimalConnectionPoolSize(nullptr);
    EXPECT_EQ(default_size, 50U);
    
    auto context = Context::createCopy(Context::getGlobalContextInstance());
    auto & settings = context->getSettingsRef();
    settings.set("delta_lake_s3_max_connections", 80UL);
    
    auto custom_size = S3ConfigurationOptimizer::getOptimalConnectionPoolSize(context);
    EXPECT_EQ(custom_size, 80U);
}

TEST_F(S3ConfigurationOptimizerTest, CreateOptimizedS3Client)
{
    Aws::Auth::AWSCredentials credentials("test_access_key", "test_secret_key");
    
    auto client = S3ConfigurationOptimizer::createOptimizedS3Client(
        base_config, credentials, nullptr);
    
    EXPECT_NE(client, nullptr);
    // Additional tests would require AWS SDK mocking which is complex
}

TEST_F(S3ConfigurationOptimizerTest, RetryStrategyOptimization)
{
    auto optimized = S3ConfigurationOptimizer::optimizeForDeltaLake(base_config, nullptr);
    
    // Should have configured retry strategy
    EXPECT_NE(optimized.retryStrategy, nullptr);
    
    // Retry strategy should be configured for fast metadata operations
    // This would require more detailed testing of the retry strategy implementation
}

TEST_F(S3ConfigurationOptimizerTest, TimeoutOptimization)
{
    auto default_connect = S3ConfigurationOptimizer::getOptimalConnectTimeout(nullptr);
    auto default_request = S3ConfigurationOptimizer::getOptimalRequestTimeout(nullptr);
    
    EXPECT_LE(default_connect.count(), 5000);  // Should be <= 5 seconds
    EXPECT_LE(default_request.count(), 30000); // Should be <= 30 seconds
    
    // Test custom timeouts
    auto context = Context::createCopy(Context::getGlobalContextInstance());
    auto & settings = context->getSettingsRef();
    settings.set("delta_lake_s3_connect_timeout_ms", 3000UL);
    settings.set("delta_lake_s3_request_timeout_ms", 15000UL);
    
    auto custom_connect = S3ConfigurationOptimizer::getOptimalConnectTimeout(context);
    auto custom_request = S3ConfigurationOptimizer::getOptimalRequestTimeout(context);
    
    EXPECT_EQ(custom_connect.count(), 3000);
    EXPECT_EQ(custom_request.count(), 15000);
}

}

#endif // USE_AWS_S3 && USE_DELTA_KERNEL_RS
