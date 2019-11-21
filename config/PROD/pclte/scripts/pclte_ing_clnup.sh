#!/bin/bash

echo "Started the script for Harmonize Duplicate Removal"
source /home/hadoop/conf/env_conf.properties
date=`date +%Y-%m-%d`
date_ts=`date '+%Y-%m-%d_%H%M%S%s'`
mkdir -p $ingestion_base_dir/logs/$date
log_file=$ingestion_base_dir"/logs/"$date"/pclteIngestionCleanup_"$date_ts".log"

spec_files_path=$ingestion_base_dir/pclte/spec_files

spark-submit --class sparkduprem.ingestionduplicate \
 --master yarn --deploy-mode cluster  \
 --conf $spark_common_conf \
 --conf spark.executor.cores=5 --conf spark.executor.memory=10g --conf spark.driver.memory=10g --conf spark.executor.memoryOverhead=1g \
 --conf spark.dynamicAllocation.enabled=true \
 --conf spark.dynamicAllocation.maxExecutors=10 \
 --conf spark.dynamicAllocation.minExecutors=2 \
 --name PCLTE_Ing_Dup_Rem  \
 --files $spec_files_path/pclte_ing_clnup.properties#diProperties.properties,$spec_files_path/pclte_ing_clnup_tbl_spec.csv#pclte_ing_clnup_tbl_spec.csv,/etc/spark/conf/hive-site.xml \
 --properties-file /usr/lib/spark/conf/spark-defaults.conf \
 $ingestion_jar_path/$dedup_jar_name \
PCLTE_Ing_Dup_Rem 1>>$log_file 2>&1
status=$?
echo "Completed the script and the status is $status"
if [ ${status} -eq 0 ]; then
	exit 0
else
	echo "Spark job failed with error status for PCLTE Harmonize Duplicate Removal, Please have a look at the logs in below file on the server in the appropriate s3 bucket for yarn logs"
exit 1
fi




