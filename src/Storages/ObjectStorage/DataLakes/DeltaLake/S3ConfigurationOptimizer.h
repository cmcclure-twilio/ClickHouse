#pragma once

#include <config.h>

#if USE_AWS_S3 && USE_DELTA_KERNEL_RS

#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/core/client/ClientConfiguration.h>
#include <Interpreters/Context_fwd.h>
#include <Core/Settings.h>

namespace DB
{

class S3ConfigurationOptimizer
{
public:
    /// Optimize S3 client configuration for Delta Lake operations
    static Aws::Client::ClientConfiguration optimizeForDeltaLake(
        const Aws::Client::ClientConfiguration & base_config,
        ContextPtr context = nullptr);

    /// Create optimized S3 client specifically for Delta Lake metadata operations
    static std::shared_ptr<Aws::S3::S3Client> createOptimizedS3Client(
        const Aws::Client::ClientConfiguration & base_config,
        const Aws::Auth::AWSCredentials & credentials,
        ContextPtr context = nullptr);

    /// Get optimal batch size for S3 list operations based on context
    static size_t getOptimalBatchSize(ContextPtr context = nullptr);

    /// Get optimal connection pool size for S3 operations
    static size_t getOptimalConnectionPoolSize(ContextPtr context = nullptr);

    /// Get optimal timeout settings for Delta Lake operations
    static std::chrono::milliseconds getOptimalConnectTimeout(ContextPtr context = nullptr);
    static std::chrono::milliseconds getOptimalRequestTimeout(ContextPtr context = nullptr);

private:
    static constexpr size_t DEFAULT_BATCH_SIZE = 1000;
    static constexpr size_t DEFAULT_CONNECTION_POOL_SIZE = 50;
    static constexpr std::chrono::milliseconds DEFAULT_CONNECT_TIMEOUT{5000};
    static constexpr std::chrono::milliseconds DEFAULT_REQUEST_TIMEOUT{30000};

    /// Performance tuning constants for Delta Lake workloads
    static constexpr size_t DELTA_LAKE_MAX_CONNECTIONS = 100;
    static constexpr size_t DELTA_LAKE_MAX_BATCH_SIZE = 2000;
    static constexpr std::chrono::milliseconds DELTA_LAKE_FAST_TIMEOUT{3000};
    static constexpr std::chrono::milliseconds DELTA_LAKE_STANDARD_TIMEOUT{15000};
};

}

#endif
