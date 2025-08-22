#pragma once

#include "config.h"

#if USE_DELTA_KERNEL_RS

#include <Disks/ObjectStorages/ObjectStorageIteratorAsync.h>
#include <IO/S3/Client.h>
#include <memory>

namespace DB
{

class DeltaLakeS3IteratorOptimized;

std::shared_ptr<IObjectStorageIteratorAsync> createOptimizedDeltaLakeS3Iterator(
    const std::string & bucket,
    const std::string & path_prefix,
    std::shared_ptr<const S3::Client> client,
    size_t max_list_size);

}

#endif
