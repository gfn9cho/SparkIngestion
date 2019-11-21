#!/bin/bash

echo "Started the script for Telematics ingestion load"
source /home/hadoop/conf/env_conf.properties
date=`date +%Y-%m-%d`
date_ts=`date '+%Y-%m-%d_%H%M%S%s'`
mkdir -p $ingestion_base_dir/logs/$date
log_file=$ingestion_base_dir"/logs/"$date"/telematicsDataIngestion_"$date_ts".log"

spec_files_path=$ingestion_base_dir/telematics/spec_files

spark-submit --class edf.DataIngestion.DataIngestionUpgraded \
 --master yarn --deploy-mode cluster  \
 --conf $spark_common_conf \
 --conf spark.executor.cores=5 --conf spark.executor.memory=10g --conf spark.driver.memory=20g --conf spark.executor.memoryOverhead=1g \
 --conf spark.dynamicAllocation.enabled=true \
 --conf spark.dynamicAllocation.maxExecutors=30 \
 --conf spark.dynamicAllocation.minExecutors=5 \
 --name Telematics_DI_DataIngestion  \
 --files $spec_files_path/telematics.properties#diProperties.properties,$spec_files_path/telematics_lookup_info.csv#telematics_lookup_info.csv,$spec_files_path/telematics_pii_spec.csv#telematics_pii_spec.csv,$spec_files_path/telematics_tables_spec.csv#telematics_tables_spec.csv,/etc/spark/conf/hive-site.xml \
 --properties-file /usr/lib/spark/conf/spark-defaults.conf \
 $ingestion_jar_path/$telematics_ingestion_jar_name \
Telematics_DI_DataIngestion 1>>$log_file 2>&1
status=$?
echo "Completed the script and the status is $status"
if [ ${status} -eq 0 ]; then
	exit 0
else
	echo "Spark job failed with error status for Telematics, Please have a look at the logs in below file on the server in the appropriate s3 bucket for yarn logs"
exit 1
fi



