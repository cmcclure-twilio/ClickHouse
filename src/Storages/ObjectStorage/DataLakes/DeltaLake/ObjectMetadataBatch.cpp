#include "config.h"

#if USE_DELTA_KERNEL_RS

#include <Storages/ObjectStorage/DataLakes/DeltaLake/ObjectMetadataBatch.h>
#include <IO/S3/getObjectInfo.h>
#include <Common/ThreadPool.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ProfileEvents
{
    extern const Event DeltaLakeObjectMetadataBatched;
    extern const Event DeltaLakeObjectMetadataParallel;
}

ObjectMetadataBatch::ObjectMetadataBatch(
    std::shared_ptr<const S3::Client> s3_client_,
    const std::string & bucket_name_,
    size_t batch_size_,
    size_t max_parallel_requests_)
    : s3_client(s3_client_)
    , bucket_name(bucket_name_)
    , batch_size(batch_size_)
    , max_parallel_requests(max_parallel_requests_)
    , log(getLogger("ObjectMetadataBatch"))
{
}

std::unordered_map<std::string, ObjectMetadata> 
ObjectMetadataBatch::getObjectsMetadata(const std::vector<std::string> & object_keys)
{
    if (object_keys.empty())
        return {};

    LOG_TRACE(log, "Getting metadata for {} objects in batches of {}", 
              object_keys.size(), batch_size);
    
    ProfileEvents::increment(ProfileEvents::DeltaLakeObjectMetadataBatched);
    
    std::unordered_map<std::string, ObjectMetadata> results;
    results.reserve(object_keys.size());
    
    /// Process objects in batches to avoid overwhelming S3
    for (size_t i = 0; i < object_keys.size(); i += batch_size)
    {
        size_t end_idx = std::min(i + batch_size, object_keys.size());
        std::vector<std::string> batch(object_keys.begin() + i, object_keys.begin() + end_idx);
        
        auto batch_results = getObjectsMetadataBatch(batch);
        
        /// Merge results
        for (auto && [key, metadata] : batch_results)
        {
            results[key] = std::move(metadata);
        }
    }
    
    LOG_TRACE(log, "Completed metadata fetching for {} objects", results.size());
    return results;
}

std::unordered_map<std::string, ObjectMetadata> 
ObjectMetadataBatch::getObjectsMetadataBatch(const std::vector<std::string> & batch_keys)
{
    if (batch_keys.empty())
        return {};
        
    ProfileEvents::increment(ProfileEvents::DeltaLakeObjectMetadataParallel);
    
    std::unordered_map<std::string, ObjectMetadata> results;
    results.reserve(batch_keys.size());
    
    /// Determine optimal parallelism for this batch
    const size_t num_threads = std::min(
        batch_keys.size(), 
        static_cast<size_t>(max_parallel_requests));
    
    if (num_threads <= 1)
    {
        /// Sequential processing for small batches
        for (const auto & key : batch_keys)
        {
            try
            {
                auto metadata = getObjectMetadataSingle(key);
                if (metadata.has_value())
                {
                    results[key] = *metadata;
                }
            }
            catch (const std::exception & e)
            {
                LOG_WARNING(log, "Failed to get metadata for object {}: {}", key, e.what());
            }
        }
        return results;
    }
    
    /// Parallel processing for larger batches
    ThreadPool pool(num_threads, num_threads, 100);
    std::mutex results_mutex;
    std::vector<std::future<void>> futures;
    
    for (const auto & key : batch_keys)
    {
        auto future = pool.scheduleOrThrow([this, &key, &results, &results_mutex]()
        {
            try
            {
                auto metadata = getObjectMetadataSingle(key);
                if (metadata.has_value())
                {
                    std::lock_guard lock(results_mutex);
                    results[key] = *metadata;
                }
            }
            catch (const std::exception & e)
            {
                LOG_WARNING(log, "Failed to get metadata for object {}: {}", key, e.what());
            }
        });
        futures.push_back(std::move(future));
    }
    
    /// Wait for all requests to complete
    for (auto & future : futures)
    {
        future.get();
    }
    
    return results;
}

std::optional<ObjectMetadata> 
ObjectMetadataBatch::getObjectMetadataSingle(const std::string & object_key)
{
    try
    {
        auto object_info = S3::getObjectInfo(
            *s3_client, bucket_name, object_key, {}, 
            /* with_metadata= */ true, 
            /* throw_on_error= */ false);

        if (object_info.size == 0 && object_info.last_modification_time == 0)
        {
            return std::nullopt;
        }

        ObjectMetadata result;
        result.size_bytes = object_info.size;
        result.last_modified = Poco::Timestamp::fromEpochTime(object_info.last_modification_time);
        result.etag = object_info.etag;
        result.attributes = object_info.metadata;

        return result;
    }
    catch (const std::exception & e)
    {
        LOG_DEBUG(log, "Exception getting metadata for {}: {}", object_key, e.what());
        return std::nullopt;
    }
}

/// Optimized KeysIterator that uses batched metadata fetching
class KeysIteratorOptimized : public IObjectIterator
{
public:
    KeysIteratorOptimized(
        Strings && data_files_,
        ObjectStoragePtr object_storage_,
        IDataLakeMetadata::FileProgressCallback callback_,
        std::optional<UInt64> snapshot_version_ = std::nullopt)
        : data_files(data_files_)
        , object_storage(object_storage_)
        , callback(callback_)
        , snapshot_version(snapshot_version_)
        , log(getLogger("KeysIteratorOptimized"))
    {
        /// Pre-fetch metadata in batches for better performance
        if (auto s3_storage = std::dynamic_pointer_cast<S3ObjectStorage>(object_storage))
        {
            preFetchMetadata(s3_storage);
        }
    }

    size_t estimatedKeysCount() override
    {
        return data_files.size();
    }

    std::optional<UInt64> getSnapshotVersion() const override
    {
        return snapshot_version;
    }

    ObjectInfoPtr next(size_t) override
    {
        while (true)
        {
            size_t current_index = index.fetch_add(1, std::memory_order_relaxed);
            if (current_index >= data_files.size())
                return nullptr;

            auto key = data_files[current_index];
            
            ObjectMetadata metadata;
            
            /// Try to get from pre-fetched cache first
            auto cache_it = metadata_cache.find(key);
            if (cache_it != metadata_cache.end())
            {
                metadata = cache_it->second;
            }
            else
            {
                /// Fallback to individual fetch
                metadata = object_storage->getObjectMetadata(key);
            }

            if (callback)
                callback(FileProgress(0, metadata.size_bytes));

            return std::make_shared<ObjectInfo>(key, std::move(metadata));
        }
    }

private:
    void preFetchMetadata(std::shared_ptr<S3ObjectStorage> s3_storage)
    {
        try
        {
            LOG_TRACE(log, "Pre-fetching metadata for {} files", data_files.size());
            
            auto s3_client = s3_storage->getS3StorageClient();
            auto bucket = s3_storage->getObjectsNamespace();
            
            ObjectMetadataBatch batch_fetcher(s3_client, bucket, 50, 10);
            metadata_cache = batch_fetcher.getObjectsMetadata(data_files);
            
            LOG_TRACE(log, "Pre-fetched metadata for {} files", metadata_cache.size());
        }
        catch (const std::exception & e)
        {
            LOG_WARNING(log, "Failed to pre-fetch metadata: {}", e.what());
            /// Continue without pre-fetching
        }
    }

    Strings data_files;
    ObjectStoragePtr object_storage;
    std::atomic<size_t> index = 0;
    IDataLakeMetadata::FileProgressCallback callback;
    std::optional<UInt64> snapshot_version;
    std::unordered_map<std::string, ObjectMetadata> metadata_cache;
    LoggerPtr log;
};

/// Factory function to create optimized keys iterator
ObjectIterator createOptimizedKeysIterator(
    Strings && data_files,
    ObjectStoragePtr object_storage,
    IDataLakeMetadata::FileProgressCallback callback,
    std::optional<UInt64> snapshot_version)
{
    return std::make_shared<KeysIteratorOptimized>(
        std::move(data_files), object_storage, callback, snapshot_version);
}

}

#endif
