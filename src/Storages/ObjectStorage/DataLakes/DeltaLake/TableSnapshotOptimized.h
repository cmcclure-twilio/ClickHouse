#pragma once

#include "config.h"

#if USE_DELTA_KERNEL_RS

#include <Storages/ObjectStorage/DataLakes/DeltaLake/TableSnapshot.h>
#include <Common/ThreadPool.h>
#include <future>
#include <memory>

namespace DeltaLake
{

/**
 * An optimized version of TableSnapshot that performs expensive S3 operations
 * asynchronously to avoid blocking the main query thread
 */
class TableSnapshotOptimized : public TableSnapshot
{
public:
    TableSnapshotOptimized(
        KernelHelperPtr helper_,
        DB::ObjectStoragePtr object_storage_,
        DB::ContextPtr context_,
        LoggerPtr log_);

    /// Override to provide async initialization
    void initSnapshot() const override;

    /// Pre-warm the snapshot asynchronously in background
    void preWarmSnapshot() const;

    /// Check if snapshot is ready (non-blocking)
    bool isSnapshotReady() const;

private:
    mutable std::future<void> snapshot_future;
    mutable std::mutex snapshot_future_mutex;
    mutable std::atomic<bool> snapshot_initialized{false};
    mutable std::atomic<bool> snapshot_warming{false};

    /// Background snapshot initialization
    void initSnapshotAsync() const;

    /// Thread pool for async operations
    static ThreadPool & getAsyncPool();
};

}

#endif
