#pragma once

#include "config.h"

#if USE_DELTA_KERNEL_RS

#include <Disks/ObjectStorages/ObjectStorageIteratorAsync.h>
#include <IO/S3/Client.h>
#include <memory>

namespace DB
{

/// Optimized S3 iterator specifically for Delta Lake metadata access patterns
class DeltaLakeS3IteratorOptimized;

/// Factory function to create optimized S3 iterator for Delta Lake
std::shared_ptr<IObjectStorageIteratorAsync> createOptimizedDeltaLakeS3Iterator(
    const std::string & bucket,
    const std::string & path_prefix,
    std::shared_ptr<const S3::Client> client,
    size_t max_list_size);

}

#endif
