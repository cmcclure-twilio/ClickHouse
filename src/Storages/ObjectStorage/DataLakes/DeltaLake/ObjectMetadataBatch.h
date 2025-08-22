#pragma once

#include "config.h"

#if USE_DELTA_KERNEL_RS

#include <Disks/ObjectStorages/IObjectStorage.h>
#include <IO/S3/Client.h>
#include <unordered_map>
#include <vector>
#include <optional>

namespace DB
{

class ObjectMetadataBatch
{
public:
    ObjectMetadataBatch(
        std::shared_ptr<const S3::Client> s3_client_,
        const std::string & bucket_name_,
        size_t batch_size_ = 50,
        size_t max_parallel_requests_ = 10);

    std::unordered_map<std::string, ObjectMetadata> getObjectsMetadata(
        const std::vector<std::string> & object_keys);

private:
    std::unordered_map<std::string, ObjectMetadata> getObjectsMetadataBatch(
        const std::vector<std::string> & batch_keys);

    std::optional<ObjectMetadata> getObjectMetadataSingle(const std::string & object_key);

    std::shared_ptr<const S3::Client> s3_client;
    std::string bucket_name;
    size_t batch_size;
    size_t max_parallel_requests;
    LoggerPtr log;
};

ObjectIterator createOptimizedKeysIterator(
    Strings && data_files,
    ObjectStoragePtr object_storage,
    IDataLakeMetadata::FileProgressCallback callback,
    std::optional<UInt64> snapshot_version = std::nullopt);

}

#endif
