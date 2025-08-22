#include "DeltaLakePerformanceMonitor.h"

#if USE_DELTA_KERNEL_RS

#include <Common/logger_useful.h>
#include <Common/formatReadable.h>
#include <base/FnTraits.h>

namespace ProfileEvents
{
    extern const Event S3DeltaLakeSnapshotCacheHits;
    extern const Event S3DeltaLakeSnapshotCacheMisses;
    extern const Event S3DeltaLakeAsyncSnapshotInit;
    extern const Event S3DeltaLakeMetadataProcessingOptimized;
}

namespace DB
{

DeltaLakePerformanceMonitor & DeltaLakePerformanceMonitor::instance()
{
    static DeltaLakePerformanceMonitor instance;
    return instance;
}

void DeltaLakePerformanceMonitor::startOperation(const String & operation_id, const String & table_path)
{
    std::lock_guard lock(metrics_mutex_);
    operation_start_times_[operation_id] = std::chrono::steady_clock::now();
    
    LOG_TRACE(getLogger("DeltaLakePerformanceMonitor"),
             "Started performance tracking for operation {} on table {}", operation_id, table_path);
}

void DeltaLakePerformanceMonitor::endOperation(const String & operation_id, const PerformanceMetrics & metrics)
{
    std::lock_guard lock(metrics_mutex_);
    
    auto it = operation_start_times_.find(operation_id);
    if (it == operation_start_times_.end())
    {
        LOG_WARNING(getLogger("DeltaLakePerformanceMonitor"),
                   "No start time found for operation {}", operation_id);
        return;
    }
    
    auto total_time = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - it->second);
    
    operation_start_times_.erase(it);
    
    /// Update global metrics
    global_metrics_.snapshot_init_time = 
        std::max(global_metrics_.snapshot_init_time, metrics.snapshot_init_time);
    global_metrics_.metadata_scan_time = 
        std::max(global_metrics_.metadata_scan_time, metrics.metadata_scan_time);
    global_metrics_.s3_list_time = 
        std::max(global_metrics_.s3_list_time, metrics.s3_list_time);
    global_metrics_.files_processed += metrics.files_processed;
    global_metrics_.cache_hits += metrics.cache_hits;
    global_metrics_.cache_misses += metrics.cache_misses;
    global_metrics_.used_optimizations = global_metrics_.used_optimizations || metrics.used_optimizations;
    
    LOG_DEBUG(getLogger("DeltaLakePerformanceMonitor"),
             "Operation {} completed in {}ms. Snapshot: {}ms, Metadata: {}ms, S3 List: {}ms, "
             "Files: {}, Cache hits: {}, Cache misses: {}, Optimizations: {}",
             operation_id, total_time.count(),
             metrics.snapshot_init_time.count(), metrics.metadata_scan_time.count(),
             metrics.s3_list_time.count(), metrics.files_processed,
             metrics.cache_hits, metrics.cache_misses,
             metrics.used_optimizations ? "enabled" : "disabled");
}

DeltaLakePerformanceMonitor::PerformanceMetrics 
DeltaLakePerformanceMonitor::getTableMetrics(const String & table_path) const
{
    std::lock_guard lock(metrics_mutex_);
    auto it = table_metrics_.find(table_path);
    return it != table_metrics_.end() ? it->second : PerformanceMetrics{};
}

DeltaLakePerformanceMonitor::PerformanceMetrics 
DeltaLakePerformanceMonitor::getGlobalMetrics() const
{
    std::lock_guard lock(metrics_mutex_);
    return global_metrics_;
}

bool DeltaLakePerformanceMonitor::isPerformanceAcceptable(const String & table_path) const
{
    auto metrics = getTableMetrics(table_path);
    
    return metrics.snapshot_init_time <= ACCEPTABLE_SNAPSHOT_TIME &&
           metrics.metadata_scan_time <= ACCEPTABLE_METADATA_TIME &&
           metrics.s3_list_time <= ACCEPTABLE_S3_LIST_TIME;
}

std::vector<String> DeltaLakePerformanceMonitor::getOptimizationRecommendations(const String & table_path) const
{
    auto metrics = getTableMetrics(table_path);
    std::vector<String> recommendations;
    
    if (!metrics.used_optimizations)
    {
        recommendations.push_back("Enable delta_lake_enable_optimized_s3_client setting");
    }
    
    if (metrics.snapshot_init_time > ACCEPTABLE_SNAPSHOT_TIME)
    {
        recommendations.push_back("Consider increasing delta_lake_s3_max_connections");
        recommendations.push_back("Consider reducing delta_lake_s3_request_timeout_ms for faster failures");
    }
    
    if (metrics.cache_misses > metrics.cache_hits)
    {
        recommendations.push_back("Consider increasing delta_lake_snapshot_cache_size");
        recommendations.push_back("Consider increasing delta_lake_snapshot_cache_ttl_seconds");
    }
    
    if (metrics.s3_list_time > ACCEPTABLE_S3_LIST_TIME)
    {
        recommendations.push_back("Consider increasing delta_lake_s3_list_batch_size");
        recommendations.push_back("Consider enabling S3 transfer acceleration");
    }
    
    if (metrics.metadata_scan_time > ACCEPTABLE_METADATA_TIME)
    {
        recommendations.push_back("Consider increasing parallelism settings");
        recommendations.push_back("Consider using more specific partition filters");
    }
    
    return recommendations;
}

void DeltaLakePerformanceMonitor::logPerformanceSummary(const String & table_path, ContextPtr context) const
{
    auto metrics = getTableMetrics(table_path);
    auto recommendations = getOptimizationRecommendations(table_path);
    
    auto logger = getLogger("DeltaLakePerformanceMonitor");
    
    LOG_INFO(logger, "Performance Summary for Delta Lake table: {}", table_path);
    LOG_INFO(logger, "  Snapshot initialization: {}ms (target: <{}ms)", 
             metrics.snapshot_init_time.count(), ACCEPTABLE_SNAPSHOT_TIME.count());
    LOG_INFO(logger, "  Metadata scanning: {}ms (target: <{}ms)", 
             metrics.metadata_scan_time.count(), ACCEPTABLE_METADATA_TIME.count());
    LOG_INFO(logger, "  S3 listing operations: {}ms (target: <{}ms)", 
             metrics.s3_list_time.count(), ACCEPTABLE_S3_LIST_TIME.count());
    LOG_INFO(logger, "  Files processed: {}", metrics.files_processed);
    LOG_INFO(logger, "  Cache efficiency: {:.1f}% ({} hits, {} misses)",
             metrics.cache_hits + metrics.cache_misses > 0 ? 
                 100.0 * metrics.cache_hits / (metrics.cache_hits + metrics.cache_misses) : 0.0,
             metrics.cache_hits, metrics.cache_misses);
    LOG_INFO(logger, "  Optimizations enabled: {}", metrics.used_optimizations ? "Yes" : "No");
    
    bool acceptable = isPerformanceAcceptable(table_path);
    LOG_INFO(logger, "  Performance status: {}", acceptable ? "ACCEPTABLE" : "NEEDS IMPROVEMENT");
    
    if (!recommendations.empty())
    {
        LOG_INFO(logger, "  Optimization recommendations:");
        for (const auto & rec : recommendations)
        {
            LOG_INFO(logger, "    - {}", rec);
        }
    }
    
    /// Update ProfileEvents
    if (metrics.used_optimizations)
    {
        ProfileEvents::increment(ProfileEvents::S3DeltaLakeMetadataProcessingOptimized);
    }
}

// PerformanceTracker implementation

PerformanceTracker::PerformanceTracker(const String & operation_id, const String & table_path)
    : operation_id_(operation_id)
    , table_path_(table_path)
    , start_time_(std::chrono::steady_clock::now())
{
    DeltaLakePerformanceMonitor::instance().startOperation(operation_id_, table_path_);
}

PerformanceTracker::~PerformanceTracker()
{
    DeltaLakePerformanceMonitor::instance().endOperation(operation_id_, metrics_);
}

void PerformanceTracker::recordSnapshotInitTime(std::chrono::milliseconds time)
{
    metrics_.snapshot_init_time = time;
}

void PerformanceTracker::recordMetadataScanTime(std::chrono::milliseconds time)
{
    metrics_.metadata_scan_time = time;
}

void PerformanceTracker::recordS3ListTime(std::chrono::milliseconds time)
{
    metrics_.s3_list_time = time;
}

void PerformanceTracker::recordFilesProcessed(size_t count)
{
    metrics_.files_processed = count;
}

void PerformanceTracker::recordCacheHit()
{
    metrics_.cache_hits++;
    ProfileEvents::increment(ProfileEvents::S3DeltaLakeSnapshotCacheHits);
}

void PerformanceTracker::recordCacheMiss()
{
    metrics_.cache_misses++;
    ProfileEvents::increment(ProfileEvents::S3DeltaLakeSnapshotCacheMisses);
}

void PerformanceTracker::recordOptimizationUsed()
{
    metrics_.used_optimizations = true;
}

}

#endif
