#include "config.h"

#if USE_DELTA_KERNEL_RS

#include <Storages/ObjectStorage/DataLakes/DeltaLake/TableSnapshotCache.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/TableSnapshotOptimized.h>
#include <Common/logger_useful.h>
#include <Common/hex.h>
#include <city.h>
#include <Core/Settings.h>

namespace DB::Setting
{
    extern const SettingsInt64 delta_lake_snapshot_version;
    extern const SettingsBool delta_lake_enable_engine_predicate;
    extern const SettingsBool delta_lake_enable_optimized_s3_client;
}

namespace DeltaLake
{

TableSnapshotCache::TableSnapshotCache()
    : cache(1000) // Cache up to 1000 snapshots
{
}

TableSnapshotCache & TableSnapshotCache::instance()
{
    static TableSnapshotCache instance;
    return instance;
}

TableSnapshotCache::CacheKey TableSnapshotCache::generateKey(
    const IKernelHelper & helper,
    const DB::ContextPtr & context)
{
    /// Create a cache key based on table location and relevant settings
    const auto & settings = context->getSettingsRef();
    
    std::string key_components = helper.getTableLocation();
    key_components += "|";
    key_components += std::to_string(settings[DB::Setting::delta_lake_snapshot_version].value);
    key_components += "|";
    key_components += std::to_string(settings[DB::Setting::delta_lake_enable_engine_predicate].value);
    key_components += "|";
    key_components += std::to_string(settings[DB::Setting::delta_lake_enable_optimized_s3_client].value);
    
    /// Use CityHash for fast, deterministic hashing
    auto hash = CityHash_v1_0_2::CityHash64(key_components.data(), key_components.size());
    return DB::getHexUIntLowercase(hash);
}

TableSnapshotCache::SnapshotPtr TableSnapshotCache::getOrCreate(
    const CacheKey & key,
    KernelHelperPtr helper,
    DB::ObjectStoragePtr object_storage,
    DB::ContextPtr context,
    LoggerPtr log)
{
    std::lock_guard lock(cache_mutex);
    
    /// Try to get from cache first
    auto cached = cache.get(key);
    if (cached)
    {
        LOG_TRACE(log, "Using cached TableSnapshot for key: {}", key);
        return *cached;
    }
    
    /// Create new snapshot
    LOG_TRACE(log, "Creating new TableSnapshot for key: {}", key);
    
    const auto & settings = context->getSettingsRef();
    bool use_optimized = settings[DB::Setting::delta_lake_enable_optimized_s3_client];
    
    SnapshotPtr snapshot;
    if (use_optimized)
    {
        snapshot = std::make_shared<TableSnapshotOptimized>(helper, object_storage, context, log);
    }
    else
    {
        snapshot = std::make_shared<TableSnapshot>(helper, object_storage, context, log);
    }
    
    /// Add to cache
    cache.set(key, snapshot);
    
    return snapshot;
}

void TableSnapshotCache::invalidate(const CacheKey & key)
{
    std::lock_guard lock(cache_mutex);
    cache.remove(key);
}

void TableSnapshotCache::clear()
{
    std::lock_guard lock(cache_mutex);
    cache.clear();
}

size_t TableSnapshotCache::size() const
{
    std::lock_guard lock(cache_mutex);
    return cache.size();
}

}

#endif
