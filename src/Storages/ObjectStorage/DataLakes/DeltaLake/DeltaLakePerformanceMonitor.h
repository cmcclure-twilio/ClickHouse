#pragma once

#include <Common/config.h>

#if USE_DELTA_KERNEL_RS

#include <Core/Types.h>
#include <Interpreters/Context_fwd.h>
#include <Common/ProfileEvents.h>
#include <chrono>
#include <unordered_map>
#include <mutex>

namespace DB
{

/// Performance monitoring and optimization manager for Delta Lake S3 operations
class DeltaLakePerformanceMonitor
{
public:
    struct PerformanceMetrics
    {
        std::chrono::milliseconds snapshot_init_time{0};
        std::chrono::milliseconds metadata_scan_time{0};
        std::chrono::milliseconds s3_list_time{0};
        size_t files_processed{0};
        size_t cache_hits{0};
        size_t cache_misses{0};
        bool used_optimizations{false};
    };
    
    static DeltaLakePerformanceMonitor & instance();
    
    /// Start tracking performance for a specific operation
    void startOperation(const String & operation_id, const String & table_path);
    
    /// End tracking and record metrics
    void endOperation(const String & operation_id, const PerformanceMetrics & metrics);
    
    /// Get performance summary for a table
    PerformanceMetrics getTableMetrics(const String & table_path) const;
    
    /// Get global performance statistics
    PerformanceMetrics getGlobalMetrics() const;
    
    /// Check if performance is acceptable (sub-second goal)
    bool isPerformanceAcceptable(const String & table_path) const;
    
    /// Get optimization recommendations
    std::vector<String> getOptimizationRecommendations(const String & table_path) const;
    
    /// Log performance summary
    void logPerformanceSummary(const String & table_path, ContextPtr context) const;
    
private:
    mutable std::mutex metrics_mutex_;
    std::unordered_map<String, PerformanceMetrics> table_metrics_;
    std::unordered_map<String, std::chrono::steady_clock::time_point> operation_start_times_;
    PerformanceMetrics global_metrics_;
    
    static constexpr std::chrono::milliseconds ACCEPTABLE_SNAPSHOT_TIME{1000}; // 1 second goal
    static constexpr std::chrono::milliseconds ACCEPTABLE_METADATA_TIME{500};  // 0.5 second goal
    static constexpr std::chrono::milliseconds ACCEPTABLE_S3_LIST_TIME{300};   // 0.3 second goal
};

/// RAII helper for automatic performance tracking
class PerformanceTracker
{
public:
    PerformanceTracker(const String & operation_id, const String & table_path);
    ~PerformanceTracker();
    
    void recordSnapshotInitTime(std::chrono::milliseconds time);
    void recordMetadataScanTime(std::chrono::milliseconds time);
    void recordS3ListTime(std::chrono::milliseconds time);
    void recordFilesProcessed(size_t count);
    void recordCacheHit();
    void recordCacheMiss();
    void recordOptimizationUsed();
    
private:
    String operation_id_;
    String table_path_;
    DeltaLakePerformanceMonitor::PerformanceMetrics metrics_;
    std::chrono::steady_clock::time_point start_time_;
};

}

#endif
