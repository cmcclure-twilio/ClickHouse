#include "config.h"

#if USE_DELTA_KERNEL_RS

#include <Storages/ObjectStorage/DataLakes/DeltaLake/MetadataProcessorOptimized.h>
#include <Storages/ObjectStorage/DataLakes/Common.h>
#include <Storages/ObjectStorage/StorageObjectStorageSource.h>
#include <Common/logger_useful.h>
#include <Common/ThreadPool.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Poco/JSON/Parser.h>
#include <future>

namespace DB
{

namespace ProfileEvents
{
    extern const Event DeltaLakeMetadataProcessingOptimized;
    extern const Event DeltaLakeMetadataFilesCached;
    extern const Event DeltaLakeMetadataFilesParallel;
}

MetadataProcessorOptimized::MetadataProcessorOptimized(
    ObjectStoragePtr object_storage_,
    StorageObjectStorageConfigurationWeakPtr configuration_,
    ContextPtr context_)
    : object_storage(object_storage_)
    , configuration(configuration_)
    , context(context_)
    , log(getLogger("MetadataProcessorOptimized"))
{
}

MetadataProcessorOptimized::ProcessedMetadata MetadataProcessorOptimized::processMetadataFilesOptimized()
{
    auto configuration_ptr = configuration.lock();
    
    LOG_TRACE(log, "Starting optimized metadata processing");
    ProfileEvents::increment(ProfileEvents::DeltaLakeMetadataProcessingOptimized);
    
    std::set<String> result_files;
    NamesAndTypesList current_schema;
    DeltaLakePartitionColumns current_partition_columns;
    
    /// Check cache first
    auto cache_key = generateCacheKey(configuration_ptr->getPath());
    if (auto cached_result = getFromCache(cache_key))
    {
        LOG_TRACE(log, "Found cached metadata for key: {}", cache_key);
        ProfileEvents::increment(ProfileEvents::DeltaLakeMetadataFilesCached);
        return *cached_result;
    }
    
    /// Try checkpoint-based approach first (more efficient)
    const auto checkpoint_version = getCheckpointIfExists(result_files, current_schema, current_partition_columns);
    
    if (checkpoint_version)
    {
        /// Process remaining metadata files in parallel
        processRemainingMetadataFilesParallel(checkpoint_version, current_schema, current_partition_columns, result_files);
        
        LOG_TRACE(log, "Processed metadata using checkpoint version {}", checkpoint_version);
    }
    else
    {
        /// Fallback: process all metadata files in parallel
        processAllMetadataFilesParallel(current_schema, current_partition_columns, result_files);
        
        LOG_TRACE(log, "Processed all metadata files (no checkpoint found)");
    }
    
    ProcessedMetadata result{current_schema, Strings(result_files.begin(), result_files.end()), current_partition_columns};
    
    /// Cache the result
    putInCache(cache_key, result);
    
    LOG_TRACE(log, "Optimized metadata processing completed: {} files, {} schema columns", 
             result.data_files.size(), result.schema.size());
    
    return result;
}

void MetadataProcessorOptimized::processRemainingMetadataFilesParallel(
    size_t checkpoint_version,
    NamesAndTypesList & current_schema,
    DeltaLakePartitionColumns & current_partition_columns,
    std::set<String> & result_files)
{
    auto configuration_ptr = configuration.lock();
    const auto deltalake_metadata_directory = "_delta_log";
    const auto metadata_file_suffix = ".json";
    
    /// Collect all metadata files after checkpoint
    std::vector<String> metadata_files_to_process;
    auto current_version = checkpoint_version;
    
    while (true)
    {
        const auto filename = withPadding(++current_version) + metadata_file_suffix;
        const auto file_path = std::filesystem::path(configuration_ptr->getPath()) / deltalake_metadata_directory / filename;
        
        if (!object_storage->exists(StoredObject(file_path)))
            break;
            
        metadata_files_to_process.push_back(file_path);
    }
    
    if (metadata_files_to_process.empty())
        return;
        
    LOG_TRACE(log, "Processing {} metadata files in parallel", metadata_files_to_process.size());
    ProfileEvents::increment(ProfileEvents::DeltaLakeMetadataFilesParallel);
    
    /// Process files in parallel with controlled concurrency
    const size_t max_threads = std::min(metadata_files_to_process.size(), size_t(8));
    ThreadPool pool(max_threads, max_threads, 100);
    
    std::mutex results_mutex;
    std::vector<std::future<FileProcessingResult>> futures;
    
    for (const auto & file_path : metadata_files_to_process)
    {
        auto future = pool.scheduleOrThrow([this, file_path]() -> FileProcessingResult {
            return processMetadataFileAsync(file_path);
        });
        futures.push_back(std::move(future));
    }
    
    /// Collect results in order to maintain consistency
    for (auto & future : futures)
    {
        try
        {
            auto file_result = future.get();
            
            std::lock_guard lock(results_mutex);
            
            /// Merge schema (first file wins for schema definition)
            if (current_schema.empty() && !file_result.schema.empty())
            {
                current_schema = file_result.schema;
            }
            else if (!current_schema.empty() && !file_result.schema.empty() && current_schema != file_result.schema)
            {
                LOG_WARNING(log, "Schema mismatch detected, using first schema");
            }
            
            /// Merge file lists
            for (const auto & file : file_result.data_files)
            {
                result_files.insert(file);
            }
            
            /// Merge partition columns
            for (const auto & [file, partitions] : file_result.partition_columns)
            {
                current_partition_columns[file] = partitions;
            }
        }
        catch (const std::exception & e)
        {
            LOG_ERROR(log, "Failed to process metadata file: {}", e.what());
            /// Continue processing other files
        }
    }
}

void MetadataProcessorOptimized::processAllMetadataFilesParallel(
    NamesAndTypesList & current_schema,
    DeltaLakePartitionColumns & current_partition_columns,
    std::set<String> & result_files)
{
    auto configuration_ptr = configuration.lock();
    const auto deltalake_metadata_directory = "_delta_log";
    const auto metadata_file_suffix = ".json";
    
    /// List all metadata files
    const auto keys = listFiles(*object_storage, *configuration_ptr, deltalake_metadata_directory, metadata_file_suffix);
    
    if (keys.empty())
        return;
        
    LOG_TRACE(log, "Processing {} metadata files in parallel (full scan)", keys.size());
    ProfileEvents::increment(ProfileEvents::DeltaLakeMetadataFilesParallel);
    
    /// Process in parallel with larger thread pool for full scan
    const size_t max_threads = std::min(keys.size(), size_t(16));
    ThreadPool pool(max_threads, max_threads, 100);
    
    std::mutex results_mutex;
    std::vector<std::future<FileProcessingResult>> futures;
    
    for (const auto & key : keys)
    {
        auto future = pool.scheduleOrThrow([this, key]() -> FileProcessingResult {
            return processMetadataFileAsync(key);
        });
        futures.push_back(std::move(future));
    }
    
    /// Collect and merge results
    for (auto & future : futures)
    {
        try
        {
            auto file_result = future.get();
            
            std::lock_guard lock(results_mutex);
            
            if (current_schema.empty() && !file_result.schema.empty())
            {
                current_schema = file_result.schema;
            }
            
            for (const auto & file : file_result.data_files)
            {
                result_files.insert(file);
            }
            
            for (const auto & [file, partitions] : file_result.partition_columns)
            {
                current_partition_columns[file] = partitions;
            }
        }
        catch (const std::exception & e)
        {
            LOG_ERROR(log, "Failed to process metadata file: {}", e.what());
        }
    }
}

MetadataProcessorOptimized::FileProcessingResult 
MetadataProcessorOptimized::processMetadataFileAsync(const String & metadata_file_path)
{
    FileProcessingResult result;
    
    try
    {
        auto read_settings = context->getReadSettings();
        ObjectInfo object_info(metadata_file_path);
        auto buf = createReadBuffer(object_info, object_storage, context, log);
        
        /// Read entire file into memory for faster processing
        String file_content;
        readStringUntilEOF(file_content, *buf);
        
        ReadBufferFromString string_buf(file_content);
        
        char c;
        while (!string_buf.eof())
        {
            while (string_buf.peek(c) && c != '{')
                string_buf.ignore();
                
            if (string_buf.eof())
                break;
                
            String json_str;
            readJSONObjectPossiblyInvalid(json_str, string_buf);
            
            if (json_str.empty())
                continue;
                
            /// Process JSON object
            processJsonObject(json_str, result);
        }
    }
    catch (const std::exception & e)
    {
        LOG_ERROR(log, "Error processing metadata file {}: {}", metadata_file_path, e.what());
        throw;
    }
    
    return result;
}

std::string MetadataProcessorOptimized::withPadding(size_t version)
{
    static constexpr auto padding = 20;
    const auto version_str = toString(version);
    return std::string(padding - version_str.size(), '0') + version_str;
}

std::string MetadataProcessorOptimized::generateCacheKey(const String & table_path)
{
    /// Create a simple cache key based on table path and current time (for TTL)
    auto now = std::chrono::system_clock::now();
    auto time_point = std::chrono::duration_cast<std::chrono::minutes>(now.time_since_epoch()).count();
    
    /// Cache for 5 minutes
    auto cache_bucket = time_point / 5;
    
    return fmt::format("{}_{}", table_path, cache_bucket);
}

// Note: Simplified cache implementation - in production this should use a proper LRU cache
thread_local std::unordered_map<std::string, MetadataProcessorOptimized::ProcessedMetadata> metadata_cache;

std::optional<MetadataProcessorOptimized::ProcessedMetadata> 
MetadataProcessorOptimized::getFromCache(const std::string & key)
{
    auto it = metadata_cache.find(key);
    if (it != metadata_cache.end())
    {
        return it->second;
    }
    return std::nullopt;
}

void MetadataProcessorOptimized::putInCache(const std::string & key, const ProcessedMetadata & data)
{
    /// Simple cache with size limit
    if (metadata_cache.size() >= 100)
    {
        metadata_cache.clear();
    }
    metadata_cache[key] = data;
}

}

#endif
