#pragma once

#include "config.h"

#if USE_DELTA_KERNEL_RS

#include <Storages/ObjectStorage/DataLakes/DeltaLake/KernelHelper.h>

namespace DB
{

DeltaLake::KernelHelperPtr getKernelHelperOptimized(
    const StorageObjectStorageConfigurationPtr & configuration,
    const ObjectStoragePtr & object_storage);

}

#endif
