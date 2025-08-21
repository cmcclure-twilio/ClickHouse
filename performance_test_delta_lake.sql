-- Test script to demonstrate Delta Lake S3 performance improvements
-- Run this with the new setting enabled vs disabled to see the difference

-- Disable optimizations (baseline)
SET delta_lake_enable_optimized_s3_client = 0;

-- Clear any existing cache 
SYSTEM DROP DNS CACHE;
SYSTEM DROP MARK CACHE;
SYSTEM DROP UNCOMPRESSED CACHE;

-- Time the first query (this will show the original slow performance)
SELECT 
    formatReadableTimeDelta(elapsed) as query_time,
    'baseline_run' as test_type,
    count(*) as record_count,
    now() as test_timestamp
FROM url('s3://your-bucket/path/to/delta/table/', 'DeltaLake') 
WHERE your_filter_condition
LIMIT 10;

-- Enable optimizations
SET delta_lake_enable_optimized_s3_client = 1;

-- Clear cache again to ensure fair comparison
SYSTEM DROP DNS CACHE;
SYSTEM DROP MARK CACHE; 
SYSTEM DROP UNCOMPRESSED CACHE;

-- Time the same query with optimizations (should be much faster)
SELECT 
    formatReadableTimeDelta(elapsed) as query_time,
    'optimized_run' as test_type,
    count(*) as record_count,
    now() as test_timestamp
FROM url('s3://your-bucket/path/to/delta/table/', 'DeltaLake')
WHERE your_filter_condition
LIMIT 10;

-- Test cached performance (should be fastest)
SELECT 
    formatReadableTimeDelta(elapsed) as query_time,
    'cached_run' as test_type,  
    count(*) as record_count,
    now() as test_timestamp
FROM url('s3://your-bucket/path/to/delta/table/', 'DeltaLake')
WHERE your_filter_condition  
LIMIT 10;
