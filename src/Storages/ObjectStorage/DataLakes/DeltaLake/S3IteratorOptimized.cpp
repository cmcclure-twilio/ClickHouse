#include "config.h"

#if USE_DELTA_KERNEL_RS

#include <Storages/ObjectStorage/DataLakes/DeltaLake/S3IteratorOptimized.h>
#include <Disks/ObjectStorages/ObjectStorageIteratorAsync.h>
#include <IO/S3Common.h>
#include <Common/logger_useful.h>
#include <ProfileEvents/ProfileEvents.h>

namespace ProfileEvents
{
    extern const Event DeltaLakeS3ListObjects;
    extern const Event DeltaLakeS3ListObjectsOptimized;
}

namespace DB
{

/// Optimized S3 iterator for Delta Lake metadata operations
/// This iterator is specifically tuned for Delta Lake's access patterns:
/// - Small metadata files (_delta_log/*.json)
/// - Sequential access patterns
/// - Frequent repeated scans
class DeltaLakeS3IteratorOptimized final : public IObjectStorageIteratorAsync
{
public:
    DeltaLakeS3IteratorOptimized(
        const std::string & bucket_,
        const std::string & path_prefix,
        std::shared_ptr<const S3::Client> client_,
        size_t max_list_size)
        : IObjectStorageIteratorAsync(
            CurrentMetrics::ObjectStorageS3Threads,
            CurrentMetrics::ObjectStorageS3ThreadsActive, 
            CurrentMetrics::ObjectStorageS3ThreadsScheduled,
            "DeltaLakeListS3")
        , client(client_)
        , request(std::make_unique<S3::ListObjectsV2Request>())
        , log(getLogger("DeltaLakeS3IteratorOptimized"))
    {
        request->SetBucket(bucket_);
        request->SetPrefix(path_prefix);
        
        /// Optimize batch size for Delta Lake metadata scanning
        /// Delta tables typically have 10-1000 log files, so we can be more aggressive
        const size_t optimized_batch_size = std::min(max_list_size, size_t(1000));
        request->SetMaxKeys(static_cast<int>(optimized_batch_size));
        
        /// Enable optimizations for metadata-heavy workloads
        /// These headers hint to S3 that we're doing metadata-heavy operations
        request->SetRequestPayer(Aws::S3::Model::RequestPayer::BucketOwner);
        
        LOG_TRACE(log, "Created optimized Delta Lake S3 iterator for prefix: {}, batch_size: {}", 
                  path_prefix, optimized_batch_size);
    }

    ~DeltaLakeS3IteratorOptimized() override
    {
        deactivate();
        request.reset();
        client.reset();
    }

private:
    bool getBatchAndCheckNext(RelativePathsWithMetadata & batch) override
    {
        ProfileEvents::increment(ProfileEvents::DeltaLakeS3ListObjects);
        ProfileEvents::increment(ProfileEvents::DeltaLakeS3ListObjectsOptimized);

        auto outcome = client->ListObjectsV2(*request);

        if (outcome.IsSuccess())
        {
            request->SetContinuationToken(outcome.GetResult().GetNextContinuationToken());

            auto objects = outcome.GetResult().GetContents();
            batch.reserve(objects.size());
            
            /// Pre-sort by name for better cache locality in Delta Lake operations
            /// Delta Lake often accesses files in version order
            std::vector<Aws::S3::Model::Object> sorted_objects(objects.begin(), objects.end());
            std::sort(sorted_objects.begin(), sorted_objects.end(),
                [](const auto & a, const auto & b) { return a.GetKey() < b.GetKey(); });

            for (const auto & object : sorted_objects)
            {
                ObjectMetadata metadata{
                    static_cast<uint64_t>(object.GetSize()), 
                    Poco::Timestamp::fromEpochTime(object.GetLastModified().Seconds()), 
                    object.GetETag(), 
                    {}
                };
                
                batch.emplace_back(std::make_shared<RelativePathWithMetadata>(
                    object.GetKey(), std::move(metadata)));
            }

            LOG_TRACE(log, "Listed {} objects, has_more: {}", 
                     batch.size(), outcome.GetResult().GetIsTruncated());

            return outcome.GetResult().GetIsTruncated();
        }

        throw S3Exception(outcome.GetError().GetErrorType(),
                          "Could not list Delta Lake objects in bucket {} with prefix {}, S3 exception: {}, message: {}",
                          quoteString(request->GetBucket()), 
                          quoteString(request->GetPrefix()),
                          backQuote(outcome.GetError().GetExceptionName()), 
                          quoteString(outcome.GetError().GetMessage()));
    }

    std::shared_ptr<const S3::Client> client;
    std::unique_ptr<S3::ListObjectsV2Request> request;
    LoggerPtr log;
};

}

#endif
