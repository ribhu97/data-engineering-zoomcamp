-- Creating the Table
CREATE OR REPLACE EXTERNAL TABLE `de-zoomcamp-338815.nytaxi.fhv-tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://dtc_data_lake_de-zoomcamp-338815/raw/fhv_tripdata_2019-*.parquet', 'gs://dtc_data_lake_de-zoomcamp-338815/raw/fhv_tripdata_2020-*.parquet']
);

-- Question 1
SELECT COUNT(*) as trips
FROM `de-zoomcamp-338815.nytaxi.fhv_tripdata_partitoned_clustered`

-- Question 2
SELECT  COUNT(DISTINCT(dispatching_base_num)) 
FROM `de-zoomcamp-338815.nytaxi.fhv_tripdata_partitoned_clustered`;

-- Question 3
-- Partition by dropoff_datetime and cluster by dispatching_base_num

-- Question 4
SELECT COUNT(*) as trips
FROM `de-zoomcamp-338815.nytaxi.fhv_tripdata_partitoned_clustered`
WHERE DATE(pickup_datetime) BETWEEN '2019-01-01' AND '2019-03-31'
  AND (dispatching_base_num='B00987' OR dispatching_base_num='B02060' OR dispatching_base_num='B02279');

-- Question 5
-- Clustering on both

-- Question 6
-- No Improvements

-- Question 7
-- Columnar