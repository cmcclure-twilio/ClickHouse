#pragma once

#include "config.h"

#if USE_DELTA_KERNEL_RS

#include <Disks/ObjectStorages/IObjectStorage.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Core/NamesAndTypes.h>
#include <Common/logger_useful.h>
#include <set>
#include <unordered_map>
#include <optional>

namespace DB
{

using DeltaLakePartitionColumn = std::pair<NameAndTypePair, Field>;
using DeltaLakePartitionColumns = std::unordered_map<std::string, std::vector<DeltaLakePartitionColumn>>;

/// Optimized metadata processor for Delta Lake tables
/// Provides parallel processing of metadata files and intelligent caching
class MetadataProcessorOptimized
{
public:
    struct ProcessedMetadata
    {
        NamesAndTypesList schema;
        Strings data_files;
        DeltaLakePartitionColumns partition_columns;
    };

    MetadataProcessorOptimized(
        ObjectStoragePtr object_storage_,
        StorageObjectStorageConfigurationWeakPtr configuration_,
        ContextPtr context_);

    /// Process metadata files with optimizations
    ProcessedMetadata processMetadataFilesOptimized();

private:
    struct FileProcessingResult
    {
        NamesAndTypesList schema;
        std::set<String> data_files;
        DeltaLakePartitionColumns partition_columns;
    };

    /// Process remaining metadata files after checkpoint in parallel
    void processRemainingMetadataFilesParallel(
        size_t checkpoint_version,
        NamesAndTypesList & current_schema,
        DeltaLakePartitionColumns & current_partition_columns,
        std::set<String> & result_files);

    /// Process all metadata files in parallel (fallback when no checkpoint)
    void processAllMetadataFilesParallel(
        NamesAndTypesList & current_schema,
        DeltaLakePartitionColumns & current_partition_columns,
        std::set<String> & result_files);

    /// Process a single metadata file asynchronously
    FileProcessingResult processMetadataFileAsync(const String & metadata_file_path);

    /// Process a JSON object from metadata file
    void processJsonObject(const String & json_str, FileProcessingResult & result);

    /// Check if checkpoint exists and process it
    size_t getCheckpointIfExists(
        std::set<String> & result,
        NamesAndTypesList & file_schema,
        DeltaLakePartitionColumns & file_partition_columns);

    /// Utility functions
    std::string withPadding(size_t version);
    std::string generateCacheKey(const String & table_path);

    /// Simple caching mechanism
    std::optional<ProcessedMetadata> getFromCache(const std::string & key);
    void putInCache(const std::string & key, const ProcessedMetadata & data);

    ObjectStoragePtr object_storage;
    StorageObjectStorageConfigurationWeakPtr configuration;
    ContextPtr context;
    LoggerPtr log;
};

}

#endif
