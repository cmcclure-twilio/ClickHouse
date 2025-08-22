#pragma once
#include <Core/Types.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>

namespace DB
{

class IObjectStorage;

#pragma once

#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{

std::vector<String> listFiles(
    const IObjectStorage & object_storage,
    const StorageObjectStorageConfiguration & configuration,
    const String & prefix, const String & suffix);

/// Optimized version for Delta Lake operations with S3 performance improvements
std::vector<String> listFilesOptimized(
    const IObjectStorage & object_storage,
    const StorageObjectStorageConfiguration & configuration,
    const String & prefix, 
    const String & suffix,
    ContextPtr context = nullptr);

}

}
