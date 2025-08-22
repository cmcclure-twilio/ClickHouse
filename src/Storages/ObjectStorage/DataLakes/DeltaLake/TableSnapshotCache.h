#pragma once

#include "config.h"

#if USE_DELTA_KERNEL_RS

#include <Storages/ObjectStorage/DataLakes/DeltaLake/TableSnapshot.h>
#include <Common/LRUCache.h>
#include <memory>
#include <string>

namespace DeltaLake
{

/**
 * Cache for TableSnapshot objects to avoid expensive re-initialization
 * of the same Delta Lake tables
 */
class TableSnapshotCache
{
public:
    using SnapshotPtr = std::shared_ptr<TableSnapshot>;
    using CacheKey = std::string;

    static TableSnapshotCache & instance();

    /// Get cached snapshot or create new one
    SnapshotPtr getOrCreate(
        const CacheKey & key,
        KernelHelperPtr helper,
        DB::ObjectStoragePtr object_storage,
        DB::ContextPtr context,
        LoggerPtr log);

    /// Invalidate cache entry
    void invalidate(const CacheKey & key);

    /// Clear entire cache
    void clear();

    /// Get cache statistics
    size_t size() const;

private:
    TableSnapshotCache();

    mutable std::mutex cache_mutex;
    DB::LRUCache<CacheKey, SnapshotPtr> cache;

    /// Generate cache key from helper and settings
    static CacheKey generateKey(
        const IKernelHelper & helper,
        const DB::ContextPtr & context);
};

}

#endif
