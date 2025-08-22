#include "config.h"

#if USE_DELTA_KERNEL_RS
#include <Storages/ObjectStorage/S3/Configuration.h>
#include <Storages/ObjectStorage/Local/Configuration.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/KernelHelper.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/KernelUtils.h>
#include <Common/logger_useful.h>

namespace DB::ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

namespace DB::S3AuthSetting
{
    extern const S3AuthSettingsBool no_sign_request;
}

namespace DeltaLake
{

class S3KernelHelperOptimized final : public IKernelHelper
{
public:
    S3KernelHelperOptimized(
        const DB::S3::URI & url_,
        std::shared_ptr<const DB::S3::Client> client_,
        const DB::S3::S3AuthSettings & auth_settings)
        : url(url_)
        , table_location(getTableLocation(url_))
        , client(client_)
    {
        region = client->getRegion();
        if (region.empty() || region == Aws::Region::AWS_GLOBAL)
            region = client->getRegionForBucket(url.bucket, /* force_detect */true);

        // Check if user didn't mention any region.
        // Same as in S3/Client.cpp (stripping len("https://s3.")).
        if (url.endpoint.substr(11) == "amazonaws.com")
            url.addRegionToURI(region);

        no_sign = auth_settings[DB::S3AuthSetting::no_sign_request];
    }

    const std::string & getTableLocation() const override { return table_location; }

    const std::string & getDataPath() const override { return url.key; }

    ffi::EngineBuilder * createBuilder() const override
    {
        ffi::EngineBuilder * builder = KernelUtils::unwrapResult(
            ffi::get_engine_builder(
                KernelUtils::toDeltaString(table_location),
                &KernelUtils::allocateError),
            "get_engine_builder");

        auto set_option = [&](const std::string & name, const std::string & value)
        {
            ffi::set_builder_option(builder, KernelUtils::toDeltaString(name), KernelUtils::toDeltaString(value));
        };

        const auto & credentials = client->getCredentials();
        auto access_key_id = credentials.GetAWSAccessKeyId();
        auto secret_access_key = credentials.GetAWSSecretKey();
        auto token = credentials.GetSessionToken();

        if (!access_key_id.empty())
            set_option("aws_access_key_id", access_key_id);
        if (!secret_access_key.empty())
            set_option("aws_secret_access_key", secret_access_key);

        set_option("aws_token", token);

        if (no_sign || (access_key_id.empty() && secret_access_key.empty()))
            set_option("aws_skip_signature", "true");

        if (!region.empty())
            set_option("aws_region", region);

        set_option("aws_bucket", url.bucket);

        if (url.uri_str.starts_with("http"))
        {
            set_option("allow_http", "true");
            set_option("aws_endpoint", url.endpoint);
        }

        // S3 performance optimizations
        set_option("aws_max_connections", "50");
        set_option("aws_pool_idle_timeout_seconds", "30");
        set_option("aws_connect_timeout_seconds", "10");
        set_option("aws_request_timeout_seconds", "30");
        set_option("aws_retry_mode", "adaptive");
        set_option("aws_max_attempts", "5");
        set_option("aws_multipart_threshold", "8388608");
        set_option("aws_copy_if_not_exists", "false");
        set_option("aws_metadata_timeout", "2");
        set_option("aws_use_virtual_addressing", "true");
        set_option("aws_use_dual_stack", "true");
        set_option("aws_buffer_time", "5");
        set_option("aws_request_min_throughput_bytes_per_second", "1048576");

        LOG_TRACE(log, "Using optimized S3 settings: endpoint: {}, uri: {}, region: {}, bucket: {}",
                  url.endpoint, url.uri_str, region, url.bucket);

        return builder;
    }

private:
    DB::S3::URI url;
    const std::string table_location;
    const std::shared_ptr<const DB::S3::Client> client;
    const LoggerPtr log = getLogger("S3KernelHelperOptimized");

    std::string region;
    bool no_sign;

    static std::string getTableLocation(const DB::S3::URI & url)
    {
        return "s3://" + url.bucket + "/" + url.key;
    }
};

}

namespace DB
{

DeltaLake::KernelHelperPtr getKernelHelperOptimized(
    const StorageObjectStorageConfigurationPtr & configuration,
    const ObjectStoragePtr & object_storage)
{
    switch (configuration->getType())
    {
        case DB::ObjectStorageType::S3:
        {
            const auto * s3_conf = dynamic_cast<const DB::StorageS3Configuration *>(configuration.get());
            return std::make_shared<DeltaLake::S3KernelHelperOptimized>(
                s3_conf->getURL(),
                object_storage->getS3StorageClient(),
                s3_conf->getAuthSettings());
        }
        case DB::ObjectStorageType::Local:
        {
            return getKernelHelper(configuration, object_storage);
        }
        default:
        {
            throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED,
                                "Unsupported storage type: {}", configuration->getType());
        }
    }
}

}

#endif
