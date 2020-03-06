#!/bin/bash


echo "Started the script for GWPL Missing ID utility"
source /home/hadoop/conf/env_conf.properties
base_dir=/home/hadoop/processed
spec_files_path=$base_dir/gwpl/missing_data_utils/spec_files
date=`date +%Y-%m-%d`
date_ts=`date '+%Y-%m-%d_%H%M%S%s'`
mkdir -p $base_dir/logs/$date
log_file=$base_dir"/logs/"$date"/gwplMissingdataingestion_"$date_ts".log"

spark-submit --class edf.missingdataload.FindAndLoadMissingRecords \
 --master yarn --deploy-mode cluster \
 --conf $spark_common_conf \
 --conf spark.executor.cores=5 --conf spark.executor.memory=10g --conf spark.driver.memory=20g --conf spark.executor.memoryOverhead=1g \
 --conf spark.dynamicAllocation.enabled=true \
 --conf spark.dynamicAllocation.maxExecutors=200 \
 --conf spark.dynamicAllocation.minExecutors=5 \
 --name GWPL_Missing_DataIngestion \
 --files ${spec_files_path}/fairScheduler.xml#fairScheduler.xml,${spec_files_path}/gwpl_missing_stg.properties#diProperties.properties,${base_dir}/gwpl/spec_files/gwpl_lookup_info.csv#gwpl_lookup_info.csv,${base_dir}/gwpl/spec_files/gwpl_pii_spec.csv#gwpl_pii_spec.csv,${base_dir}/gwpl/spec_files/gwpl_table_spec_stg.csv#gwpl_table_spec_stg.csv,${spec_files_path}/gwpl_missing_table_list.csv#gwpl_missing_table_list.csv,/etc/spark/conf/hive-site.xml \
 --properties-file /usr/lib/spark/conf/spark-defaults.conf \
 --jars /home/hadoop/processed/jars/sqljdbc42.jar \
 $ingestion_jar_path/$missingid_jar_name \
GWPL_Missing_DataIngestion 1>>$log_file 2>&1
status=$?
echo "Completed the script and the status is $status"
if [ ${status} -eq 0 ]; then
	exit 0
else
	echo "Spark job failed with error status for GWPL Missing ID utility, Please have a look at the logs in below file on the server in the appropriate s3 bucket for yarn logs"
exit 1
fi

