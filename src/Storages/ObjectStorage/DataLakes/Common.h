#pragma once

#include <Core/Types.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{

class IObjectStorage;

std::vector<String> listFiles(
    const IObjectStorage & object_storage,
    const StorageObjectStorageConfiguration & configuration,
    const String & prefix, const String & suffix);

std::vector<String> listFilesOptimized(
    const IObjectStorage & object_storage,
    const StorageObjectStorageConfiguration & configuration,
    const String & prefix,
    const String & suffix,
    ContextPtr context = nullptr);

}
