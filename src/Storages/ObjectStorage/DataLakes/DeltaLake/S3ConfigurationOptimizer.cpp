#include "S3ConfigurationOptimizer.h"

#if USE_AWS_S3 && USE_DELTA_KERNEL_RS

#include <aws/core/utils/memory/stl/AWSStringStream.h>
#include <aws/s3/S3ClientConfiguration.h>
#include <Common/logger_useful.h>
#include <Common/ProfileEvents.h>

namespace ProfileEvents
{
    extern const Event S3OptimizedClientCreated;
    extern const Event S3OptimizedConfigurationApplied;
    extern const Event S3DeltaLakeClientOptimizations;
}

namespace DB
{

Aws::Client::ClientConfiguration S3ConfigurationOptimizer::optimizeForDeltaLake(
    const Aws::Client::ClientConfiguration & base_config,
    ContextPtr context)
{
    auto optimized_config = base_config;
    
    /// Get user settings if available
    bool enable_optimizations = true;
    size_t custom_max_connections = 0;
    size_t custom_timeout = 0;
    
    if (context)
    {
        const auto & settings = context->getSettingsRef();
        enable_optimizations = settings.get("delta_lake_enable_optimized_s3_client").safeGet<bool>();
        custom_max_connections = settings.get("delta_lake_s3_max_connections").safeGet<UInt64>();
        custom_timeout = settings.get("delta_lake_s3_request_timeout_ms").safeGet<UInt64>();
    }
    
    if (!enable_optimizations)
    {
        return optimized_config;
    }
    
    /// Connection pool optimization
    size_t max_connections = custom_max_connections > 0 ? 
        custom_max_connections : getOptimalConnectionPoolSize(context);
    optimized_config.maxConnections = static_cast<unsigned>(max_connections);
    
    /// Timeout optimizations
    auto connect_timeout = getOptimalConnectTimeout(context);
    auto request_timeout = custom_timeout > 0 ? 
        std::chrono::milliseconds(custom_timeout) : getOptimalRequestTimeout(context);
        
    optimized_config.connectTimeoutMs = static_cast<long>(connect_timeout.count());
    optimized_config.requestTimeoutMs = static_cast<long>(request_timeout.count());
    
    /// HTTP optimizations for metadata-heavy workloads
    optimized_config.httpRequestTimeoutMs = static_cast<long>(request_timeout.count());
    optimized_config.enableTcpKeepAlive = true;
    optimized_config.tcpKeepAliveIntervalMs = 30000; // 30 seconds
    
    /// Connection reuse optimization
    optimized_config.enableHttpClientTrace = false; // Reduce overhead
    optimized_config.lowSpeedLimit = 1; // Bytes per second - very permissive for metadata
    
    /// Retry configuration for Delta Lake metadata operations
    optimized_config.retryStrategy = std::make_shared<Aws::Client::DefaultRetryStrategy>(
        3, // max retries - aggressive for metadata
        25 // base delay ms - quick retry for fast metadata operations
    );
    
    /// DNS optimization
    optimized_config.enableClockSkewAdjustment = true;
    
    LOG_DEBUG(getLogger("S3ConfigurationOptimizer"),
        "Applied Delta Lake optimizations: maxConnections={}, connectTimeout={}ms, requestTimeout={}ms",
        max_connections, connect_timeout.count(), request_timeout.count());
        
    ProfileEvents::increment(ProfileEvents::S3OptimizedConfigurationApplied);
    ProfileEvents::increment(ProfileEvents::S3DeltaLakeClientOptimizations);
    
    return optimized_config;
}

std::shared_ptr<Aws::S3::S3Client> S3ConfigurationOptimizer::createOptimizedS3Client(
    const Aws::Client::ClientConfiguration & base_config,
    const Aws::Auth::AWSCredentials & credentials,
    ContextPtr context)
{
    auto optimized_config = optimizeForDeltaLake(base_config, context);
    
    auto client = std::make_shared<Aws::S3::S3Client>(
        credentials,
        optimized_config,
        Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
        true // use virtual addressing
    );
    
    ProfileEvents::increment(ProfileEvents::S3OptimizedClientCreated);
    
    LOG_DEBUG(getLogger("S3ConfigurationOptimizer"), 
        "Created optimized S3 client for Delta Lake operations");
    
    return client;
}

size_t S3ConfigurationOptimizer::getOptimalBatchSize(ContextPtr context)
{
    if (context)
    {
        const auto & settings = context->getSettingsRef();
        auto custom_batch_size = settings.get("delta_lake_s3_list_batch_size").safeGet<UInt64>();
        if (custom_batch_size > 0)
        {
            return std::min(custom_batch_size, static_cast<UInt64>(DELTA_LAKE_MAX_BATCH_SIZE));
        }
    }
    
    return DEFAULT_BATCH_SIZE;
}

size_t S3ConfigurationOptimizer::getOptimalConnectionPoolSize(ContextPtr context)
{
    if (context)
    {
        const auto & settings = context->getSettingsRef();
        auto custom_connections = settings.get("delta_lake_s3_max_connections").safeGet<UInt64>();
        if (custom_connections > 0)
        {
            return std::min(custom_connections, static_cast<UInt64>(DELTA_LAKE_MAX_CONNECTIONS));
        }
    }
    
    return DEFAULT_CONNECTION_POOL_SIZE;
}

std::chrono::milliseconds S3ConfigurationOptimizer::getOptimalConnectTimeout(ContextPtr context)
{
    if (context)
    {
        const auto & settings = context->getSettingsRef();
        auto custom_timeout = settings.get("delta_lake_s3_connect_timeout_ms").safeGet<UInt64>();
        if (custom_timeout > 0)
        {
            return std::chrono::milliseconds(custom_timeout);
        }
    }
    
    return DEFAULT_CONNECT_TIMEOUT;
}

std::chrono::milliseconds S3ConfigurationOptimizer::getOptimalRequestTimeout(ContextPtr context)
{
    if (context)
    {
        const auto & settings = context->getSettingsRef();
        auto custom_timeout = settings.get("delta_lake_s3_request_timeout_ms").safeGet<UInt64>();
        if (custom_timeout > 0)
        {
            return std::chrono::milliseconds(custom_timeout);
        }
    }
    
    return DEFAULT_REQUEST_TIMEOUT;
}

}

#endif
