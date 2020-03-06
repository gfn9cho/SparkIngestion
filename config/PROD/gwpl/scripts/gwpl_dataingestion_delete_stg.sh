#!/bin/bash

echo "Started the script for GWPL DELETE ingestion load"
source /home/hadoop/conf/env_conf.properties
date=`date +%Y-%m-%d`
date_ts=`date '+%Y-%m-%d_%H%M%S%s'`
mkdir -p $ingestion_base_dir/logs/$date
log_file=$ingestion_base_dir"/logs/"$date"/gwpl_delete_Ingestion_"$date_ts".log"

spec_files_path=$ingestion_base_dir/gwpl/spec_files

spark-submit --class edf.dataingestion.DataLoad \
 --master yarn --deploy-mode cluster  \
 --conf $spark_common_conf \
 --conf spark.executor.cores=5 --conf spark.executor.memory=10g --conf spark.driver.memory=20g --conf spark.executor.memoryOverhead=1g \
 --conf spark.dynamicAllocation.enabled=true \
 --conf spark.dynamicAllocation.maxExecutors=100 \
 --conf spark.dynamicAllocation.minExecutors=5 \
 --name gwpl_delete_dataingestion  \
 --files $spec_files_path/gwpl_delete_stg.properties#diProperties.properties,$spec_files_path/gwpl_lookup_info.csv#gwpl_lookup_info.csv,$spec_files_path/gwpl_pii_spec.csv#gwpl_pii_spec.csv,$spec_files_path/gwpl_table_spec_stg.csv#gwpl_table_spec_stg.csv,/etc/spark/conf/hive-site.xml \
 --properties-file /usr/lib/spark/conf/spark-defaults.conf \
 $ingestion_jar_path/$ingestion_jar_name \
gwpl_delete_dataingestion 1>>$log_file 2>&1
status=$?
echo "Completed the script and the status is $status"
if [ ${status} -eq 0 ]; then
	exit 0
else
	echo "Spark job failed with error status for GWPL DELETE, Please have a look at the logs in below file on the server in the appropriate s3 bucket for yarn logs"
exit 1
fi


