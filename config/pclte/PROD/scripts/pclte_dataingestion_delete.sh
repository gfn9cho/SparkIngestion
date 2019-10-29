#!/bin/bash


echo "Started the script for PCLTE Delete Batch "
source /home/hadoop/conf/env_conf.properties
date=`date +%Y-%m-%d`
date_ts=`date '+%Y-%m-%d_%H%M%S%s'`
mkdir -p $ingestion_base_dir/logs/$date
log_file=$ingestion_base_dir"/logs/"$date"/pclte_delete_Ingestion_"$date_ts".log"

spec_files_path=$ingestion_base_dir/pclte/spec_files

spark-submit --class edf.dataingestion.DataLoad \
 --master yarn --deploy-mode cluster  \
 --conf $spark_common_conf \
 --conf spark.executor.cores=5 --conf spark.executor.memory=5g --conf spark.driver.memory=15g --conf spark.executor.memoryOverhead=512m \
 --conf spark.dynamicAllocation.enabled=true \
 --conf spark.dynamicAllocation.maxExecutors=100 \
 --conf spark.dynamicAllocation.minExecutors=5 \
 --name pclte_delete_batch_dataingestion  \
 --files ${spec_files_path}/pclte_delete.properties#diProperties.properties,${spec_files_path}/pclte_lookup_info.csv#pclte_lookup_info.csv,${spec_files_path}/pclte_pii_spec.csv#pclte_pii_spec.csv,${spec_files_path}/pclte_table_spec.csv#pclte_table_spec.csv,/etc/spark/conf/hive-site.xml \
 --properties-file /usr/lib/spark/conf/spark-defaults.conf \
 $ingestion_jar_path/$ingestion_jar_name \
pclte_delete_batch_dataingestion 
status=$?
echo "Completed the script and the status is $status"
if [ ${status} -eq 0 ]; then
	exit 0
else
	echo "Spark job failed with error status for PCLTE Delete Batch, Please have a look at the logs in below file on the server in the appropriate s3 bucket for yarn logs"
exit 1
fi


