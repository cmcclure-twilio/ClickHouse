#include "config.h"

#if USE_PARQUET && USE_DELTA_KERNEL_RS
#include <Storages/ObjectStorage/DataLakes/DeltaLakeMetadataDeltaKernel.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/TableSnapshotOptimized.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/KernelHelperOptimized.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/KernelUtils.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/DeltaLakeSink.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/DeltaLakePartitionedSink.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/WriteTransaction.h>
#include <Common/logger_useful.h>

namespace DB
{

/**
 * Optimized DeltaLakeMetadata with async initialization and performance improvements
 */
class DeltaLakeMetadataOptimized : public DeltaLakeMetadataDeltaKernel
{
public:
    DeltaLakeMetadataOptimized(
        ObjectStoragePtr object_storage,
        StorageObjectStorageConfigurationWeakPtr configuration_,
        ContextPtr context)
        : log(getLogger("DeltaLakeMetadataOptimized"))
        , kernel_helper(DB::getKernelHelperOptimized(configuration_.lock(), object_storage))
        , table_snapshot_optimized(std::make_shared<DeltaLake::TableSnapshotOptimized>(
                kernel_helper,
                object_storage,
                context,
                log))
    {
        LOG_TRACE(log, "Created optimized DeltaLake metadata with async initialization");
    }

    bool operator ==(const IDataLakeMetadata & metadata) const override
    {
        const auto & delta_lake_metadata = dynamic_cast<const DeltaLakeMetadataOptimized &>(metadata);
        std::lock_guard lk1(table_snapshot_mutex);
        std::lock_guard lk2(delta_lake_metadata.table_snapshot_mutex);
        return table_snapshot_optimized->getVersion() == delta_lake_metadata.table_snapshot_optimized->getVersion();
    }

    bool update(const ContextPtr & context) override
    {
        std::lock_guard lock(table_snapshot_mutex);
        return table_snapshot_optimized->update(context);
    }

    ObjectIterator iterate(
        const ActionsDAG * filter_dag,
        FileProgressCallback callback,
        size_t list_batch_size,
        ContextPtr /* context  */) const override
    {
        std::lock_guard lock(table_snapshot_mutex);
        return table_snapshot_optimized->iterate(filter_dag, callback, list_batch_size);
    }

    NamesAndTypesList getTableSchema() const override
    {
        std::lock_guard lock(table_snapshot_mutex);
        return table_snapshot_optimized->getTableSchema();
    }

    void modifyFormatSettings(FormatSettings & format_settings) const override
    {
        format_settings.parquet.allow_missing_columns = true;
    }

    ReadFromFormatInfo prepareReadingFromFormat(
        const Strings & requested_columns,
        const StorageSnapshotPtr & storage_snapshot,
        const ContextPtr & context,
        bool supports_subset_of_columns) override
    {
        auto info = DB::prepareReadingFromFormat(requested_columns, storage_snapshot, context, supports_subset_of_columns);

        DB::NameToNameMap physical_names_map;
        DB::NameSet read_columns;
        {
            std::lock_guard lock(table_snapshot_mutex);
            physical_names_map = table_snapshot_optimized->getPhysicalNamesMap();
            read_columns = table_snapshot_optimized->getReadSchema().getNameSet();
        }

        Block format_header;
        for (auto && column_with_type_and_name : info.format_header)
        {
            auto physical_name = DeltaLake::getPhysicalName(column_with_type_and_name.name, physical_names_map);
            if (!read_columns.contains(physical_name))
            {
                LOG_TEST(log, "Filtering out non-readable column: {}", column_with_type_and_name.name);
                continue;
            }
            column_with_type_and_name.name = physical_name;
            format_header.insert(std::move(column_with_type_and_name));
        }
        info.format_header = std::move(format_header);

        if (!physical_names_map.empty())
        {
            for (auto & [column_name, _] : info.requested_columns)
                column_name = DeltaLake::getPhysicalName(column_name, physical_names_map);
        }

        LOG_TEST(log, "Format header: {}", info.format_header.dumpNames());
        return info;
    }

    SinkToStoragePtr write(
        SharedHeader sample_block,
        const StorageID & /* table_id */,
        ObjectStoragePtr object_storage,
        StorageObjectStorageConfigurationPtr configuration,
        const std::optional<FormatSettings> & format_settings,
        ContextPtr context,
        std::shared_ptr<DataLake::ICatalog> /* catalog */) override
    {
        Names partition_columns;
        {
            std::lock_guard lock(table_snapshot_mutex);
            partition_columns = table_snapshot_optimized->getPartitionColumns();
        }

        auto delta_transaction = std::make_shared<DeltaLake::WriteTransaction>(kernel_helper);
        delta_transaction->create();

        if (partition_columns.empty())
        {
            return std::make_shared<DeltaLakeSink>(
                delta_transaction, configuration, object_storage, context, sample_block, format_settings);
        }

        return std::make_shared<DeltaLakePartitionedSink>(
            delta_transaction, configuration, partition_columns, object_storage,
            context, sample_block, format_settings);
    }

    /// Check if snapshot is ready without blocking
    bool isSnapshotReady() const
    {
        std::lock_guard lock(table_snapshot_mutex);
        return table_snapshot_optimized->isSnapshotReady();
    }

private:
    LoggerPtr log;
    DeltaLake::KernelHelperPtr kernel_helper;
    std::shared_ptr<DeltaLake::TableSnapshotOptimized> table_snapshot_optimized;
    mutable std::mutex table_snapshot_mutex;
};

/// Factory function to create optimized DeltaLake metadata
std::shared_ptr<DeltaLakeMetadataOptimized> createOptimizedDeltaLakeMetadata(
    ObjectStoragePtr object_storage,
    StorageObjectStorageConfigurationWeakPtr configuration,
    ContextPtr context)
{
    return std::make_shared<DeltaLakeMetadataOptimized>(object_storage, configuration, context);
}

}

#endif
