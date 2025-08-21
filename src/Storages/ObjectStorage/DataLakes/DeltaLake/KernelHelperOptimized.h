#pragma once

#include "config.h"

#if USE_DELTA_KERNEL_RS

#include <Storages/ObjectStorage/DataLakes/DeltaLake/KernelHelper.h>

namespace DB
{

/// Optimized version of getKernelHelper with performance improvements for S3
DeltaLake::KernelHelperPtr getKernelHelperOptimized(
    const StorageObjectStorageConfigurationPtr & configuration,
    const ObjectStoragePtr & object_storage);

}

#endif
