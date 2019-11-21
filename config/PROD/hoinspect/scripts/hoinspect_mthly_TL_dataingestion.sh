#!/bin/bash

echo "Started the script for HOINSPECT Monthly TL ingestion load"
source /home/hadoop/conf/env_conf.properties
date=`date +%Y-%m-%d`
date_ts=`date '+%Y-%m-%d_%H%M%S%s'`
mkdir -p $ingestion_base_dir/logs/$date
log_file=$ingestion_base_dir"/logs/"$date"/hoinspect_monthlyTLDataIngestion_"$date_ts".log"

spec_files_path=$ingestion_base_dir/hoinspect/spec_files

spark-submit --class edf.dataingestion.DataLoad \
 --master yarn --deploy-mode cluster  \
 --conf $spark_common_conf \
 --conf spark.executor.cores=5 --conf spark.executor.memory=20g --conf spark.driver.memory=20g --conf spark.executor.memoryOverhead=2g \
 --conf spark.dynamicAllocation.enabled=true \
 --conf spark.dynamicAllocation.maxExecutors=50 \
 --conf spark.dynamicAllocation.minExecutors=2 \
 --name HOINSPECT_MTHLY_TL_DataIngestion  \
 --files $spec_files_path/hoinspect_mthly_TL.properties#diProperties.properties,$spec_files_path/hoinspect_mthly_lookup_info.csv#hoinspect_mthly_lookup_info.csv,$spec_files_path/hoinspect_mthly_pii_spec.csv#hoinspect_mthly_pii_spec.csv,$spec_files_path/hoinspect_mthly_table_spec.csv#hoinspect_mthly_table_spec.csv,/etc/spark/conf/hive-site.xml \
 --properties-file /usr/lib/spark/conf/spark-defaults.conf \
 $ingestion_jar_path/$ingestion_jar_name \
HOINSPECT_MTHLY_TL_DataIngestion 1>>$log_file 2>&1
status=$?
echo "Completed the script and the status is $status"
if [ ${status} -eq 0 ]; then
	exit 0
else
	echo "Spark job failed with error status for HOINSPECT Monthly TL, Please have a look at the logs in below file on the server in the appropriate s3 bucket for yarn logs"
exit 1
fi





