#!/bin/bash

echo "Started the script for GAIN TL ingestion load"
source /home/hadoop/conf/env_conf.properties
date=`date +%Y-%m-%d`
date_ts=`date '+%Y-%m-%d_%H%M%S%s'`
mkdir -p $ingestion_base_dir/logs/$date
log_file=$ingestion_base_dir"/logs/"$date"/gaindailyTLDataIngestion_"$date_ts".log"

spec_files_path=$ingestion_base_dir/gain/spec_files

spark-submit --class edf.dataingestion.DataLoad \
 --master yarn --deploy-mode cluster  \
 --conf $spark_common_conf \
 --conf spark.executor.cores=5 --conf spark.executor.memory=10g --conf spark.driver.memory=20g --conf spark.executor.memoryOverhead=1g \
 --conf spark.dynamicAllocation.enabled=true \
 --conf spark.dynamicAllocation.maxExecutors=10 \
 --conf spark.dynamicAllocation.minExecutors=5 \
 --name gain_TL_DataIngestion  \
 --files $spec_files_path/gain_daily_TL.properties#diProperties.properties,$spec_files_path/gain_lookup_info.csv#gain_lookup_info.csv,$spec_files_path/gain_pii_spec.csv#gain_pii_spec.csv,$spec_files_path/gain_table_spec_TL.csv#gain_table_spec_TL.csv,/etc/spark/conf/hive-site.xml \
 --properties-file /usr/lib/spark/conf/spark-defaults.conf \
 $ingestion_jar_path/$gain_ingestion_jar_name \
gain_TL_DataIngestion 1>>$log_file 2>&1
status=$?
echo "Completed the script and the status is $status"
if [ ${status} -eq 0 ]; then
	exit 0
else
	echo "Spark job failed with error status for GAIN TL, Please have a look at the logs in below file on the server in the appropriate s3 bucket for yarn logs"
exit 1
fi



