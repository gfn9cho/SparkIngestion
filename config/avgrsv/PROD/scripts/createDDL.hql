CREATE EXTERNAL TABLE `plcy_averagereserveamts_test`(
  `mco` string,
  `asl` string,
  `causeofloss` string,
  `avgrsvmethod` string,
  `loss_resv` double,
  `exp_resv` double,
  `avgrsvgrp` string,
  `loss_cause_limit` string,
  `glbigrp` string,
  `ageofloss` string,
  `ingestion_date` date,
  `effective_date` date,
  `expiration_date` date,
  `load_dts` timestamp,
  `row_id` bigint,
  `source_system` string)
PARTITIONED BY (
  `ingestion_yyyymm` bigint)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES (
  'path'='s3://sa-l4-datalake-processed/processed_edwexternal/plcy_averagereserveamts_test')
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://sa-l4-datalake-processed/processed_edwexternal/plcy_averagereserveamts_test';

CREATE TABLE `processed_edwexternal.plcy_averagereserveamts`(
  `mco` string,
  `asl` string,
  `causeofloss` string,
  `avgrsvmethod` string,
  `loss_resv` double,
  `exp_resv` double,
  `avgrsvgrp` string,
  `loss_cause_limit` string,
  `glbigrp` string,
  `ageofloss` string,
  `ingestion_date` date,
  `effective_date` date,
  `expiration_date` date,
  `load_dts` timestamp,
  `row_id` bigint,
  `source_system` string)
PARTITIONED BY (
  `ingestion_yyyymm` bigint)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  's3://sa-l4-datalake-processed/processed_edwexternal/plcy_averagereserveamts';

CREATE EXTERNAL TABLE `processed_edwexternal.plcy_avgrsrvs_stat`(
  `run_name` string, 
  `ingestion_dttm` string, 
  `layer_name` string, 
  `start_time` string, 
  `end_time` string, 
  `total_run_time` string, 
  `asl_csv_cnt` bigint, 
  `ytc_src_cnt` bigint, 
  `avg_tgt_cnt` bigint, 
  `run_status` string, 
  `run_yyyymm` int, 
  `description` string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY ',' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  's3://sa-l4-datalake-processed/processed_edwexternal/plcy_avgrsrvs_stat'
TBLPROPERTIES (
  'COLUMN_STATS_ACCURATE'='false', 
  'numFiles'='0', 
  'numRows'='-1', 
  'rawDataSize'='-1', 
  'spark.sql.create.version'='2.2 or prior', 
  'spark.sql.sources.schema.numParts'='1', 
  'spark.sql.sources.schema.part.0'='{\"type\":\"struct\",\"fields\":[{\"name\":\"run_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"ingestion_dttm\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"layer_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"start_time\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"end_time\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"total_run_time\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"asl_csv_cnt\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"ytc_src_cnt\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"avg_tgt_cnt\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"run_status\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"run_yyyymm\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"description\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}', 
  'totalSize'='0');