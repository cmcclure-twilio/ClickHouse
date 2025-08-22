#include "config.h"

#if USE_DELTA_KERNEL_RS

#include <Storages/ObjectStorage/DataLakes/DeltaLake/TableSnapshotOptimized.h>
#include <Common/ThreadPool.h>
#include <Common/logger_useful.h>
#include <Common/Exception.h>

namespace DeltaLake
{

TableSnapshotOptimized::TableSnapshotOptimized(
    KernelHelperPtr helper_,
    DB::ObjectStoragePtr object_storage_,
    DB::ContextPtr context_,
    LoggerPtr log_)
    : TableSnapshot(helper_, object_storage_, context_, log_)
{
    /// Start pre-warming the snapshot immediately
    preWarmSnapshot();
}

ThreadPool & TableSnapshotOptimized::getAsyncPool()
{
    /// Use a dedicated thread pool for Delta Lake async operations
    /// with a reasonable number of threads to handle concurrent snapshot initializations
    static ThreadPool pool(10, 50, 100);
    return pool;
}

void TableSnapshotOptimized::preWarmSnapshot() const
{
    std::lock_guard lock(snapshot_future_mutex);

    if (snapshot_warming.load() || snapshot_initialized.load())
        return;

    snapshot_warming = true;

    LOG_TRACE(log, "Starting async snapshot pre-warming");

    /// Start the async initialization
    snapshot_future = getAsyncPool().scheduleOrThrow([this]()
    {
        try
        {
            initSnapshotAsync();
            snapshot_initialized = true;
            LOG_TRACE(log, "Async snapshot initialization completed successfully");
        }
        catch (...)
        {
            snapshot_initialized = false;
            LOG_ERROR(log, "Async snapshot initialization failed: {}", DB::getCurrentExceptionMessage(true));
            /// Don't re-throw here as it would terminate the thread
        }
        snapshot_warming = false;
    });
}

void TableSnapshotOptimized::initSnapshotAsync() const
{
    LOG_TRACE(log, "Starting async snapshot initialization");

    /// Use the original implementation but run it in a background thread
    TableSnapshot::initSnapshotImpl();

    LOG_TRACE(log, "Async snapshot initialization completed");
}

void TableSnapshotOptimized::initSnapshot() const
{
    /// Fast path: if already initialized, return immediately
    if (snapshot_initialized.load() && kernel_snapshot_state)
        return;

    /// Check if async initialization is in progress
    {
        std::lock_guard lock(snapshot_future_mutex);
        if (snapshot_future.valid())
        {
            LOG_TRACE(log, "Waiting for async snapshot initialization to complete");

            /// Wait for the async initialization to complete
            /// This blocks the main thread, but only after async work has started
            try
            {
                snapshot_future.get();
            }
            catch (...)
            {
                LOG_ERROR(log, "Async snapshot initialization failed, falling back to sync");
                /// Fall through to sync initialization
            }
        }
    }

    /// If async initialization completed successfully, we're done
    if (snapshot_initialized.load() && kernel_snapshot_state)
        return;

    /// Fallback to synchronous initialization
    LOG_TRACE(log, "Falling back to synchronous snapshot initialization");
    TableSnapshot::initSnapshotImpl();
    snapshot_initialized = true;
}

bool TableSnapshotOptimized::isSnapshotReady() const
{
    return snapshot_initialized.load() && kernel_snapshot_state != nullptr;
}

}

#endif
