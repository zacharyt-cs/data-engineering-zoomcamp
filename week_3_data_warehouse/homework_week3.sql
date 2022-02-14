CREATE OR REPLACE EXTERNAL TABLE `trips_data_all.fhv_2019_table`
OPTIONS (
    format='PARQUET',
    uris = ['gs://dtc_data_lake_wired-yeti-338315/raw/fhv_tripdata_2019-*.parquet']
);
-- Q1
SELECT COUNT(*) FROM `wired-yeti-338315.trips_data_all.fhv_2019_table`;

-- Q2
SELECT COUNT(DISTINCT dispatching_base_num) FROM `wired-yeti-338315.trips_data_all.fhv_2019_table`;

-- Q3 Create partitioned and clustered table
CREATE OR REPLACE TABLE `wired-yeti-338315.trips_data_all.fhv_2019_partitioned`
PARTITION BY DATE(pickup_datetime)
CLUSTER BY dispatching_base_num
AS
SELECT *
FROM `wired-yeti-338315.trips_data_all.fhv_2019_table`;

-- Q4
/* Question 4: What is the count, estimated and actual data processed for query which counts trip
between 2019/01/01 and 2019/03/31 for dispatching_base_num B00987, B02060, B02279 */
SELECT COUNT(*)
FROM `wired-yeti-338315.trips_data_all.fhv_2019_partitioned`
WHERE dispatching_base_num IN ('B00987', 'B02060', 'B02279')
AND pickup_datetime BETWEEN '2019-01-01' AND '2019-03-31';