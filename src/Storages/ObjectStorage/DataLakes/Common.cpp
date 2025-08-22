#include "Common.h"
#include <Disks/ObjectStorages/IObjectStorage.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/S3IteratorOptimized.h>
#include <Common/logger_useful.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>

#include <fmt/ranges.h>

namespace DB
{

std::vector<String> listFiles(
    const IObjectStorage & object_storage,
    const StorageObjectStorageConfiguration & configuration,
    const String & prefix, const String & suffix)
{
    auto key = std::filesystem::path(configuration.getPathForRead().path) / prefix;
    RelativePathsWithMetadata files_with_metadata;
    object_storage.listObjects(key, files_with_metadata, 0);
    Strings res;
    for (const auto & file_with_metadata : files_with_metadata)
    {
        const auto & filename = file_with_metadata->relative_path;
        if (filename.ends_with(suffix))
            res.push_back(filename);
    }
    LOG_TRACE(getLogger("DataLakeCommon"), "Listed {} files ({})", res.size(), fmt::join(res, ", "));
    return res;
}

std::vector<String> listFilesOptimized(
    const IObjectStorage & object_storage,
    const StorageObjectStorageConfiguration & configuration,
    const String & prefix,
    const String & suffix,
    ContextPtr context)
{
    auto key = std::filesystem::path(configuration.getPathForRead().path) / prefix;

    bool use_optimized = true;
    if (context)
    {
        const auto & settings = context->getSettingsRef();
        try {
            use_optimized = settings.get("delta_lake_enable_optimized_s3_client").safeGet<bool>();
        } catch (...) {
            use_optimized = false;
        }
    }

    if (!use_optimized)
    {
        return listFiles(object_storage, configuration, prefix, suffix);
    }

    return listFiles(object_storage, configuration, prefix, suffix);
}

}
