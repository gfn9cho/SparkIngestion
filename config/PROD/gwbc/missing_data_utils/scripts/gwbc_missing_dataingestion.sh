#!/bin/bash


echo "Started the script for GWBC Missing ID utility"
source /home/hadoop/conf/env_conf.properties
base_dir=/home/hadoop/processed
spec_files_path=$base_dir/gwbc/missing_data_utils/spec_files
jar_path=$base_dir/gwbc/missing_data_utils/jars
spark_app_jar_file_suffix=edf_missing_dataingestion
spark_app_jar_file_version=1.0.0.jar
date=`date +%Y-%m-%d`
date_ts=`date '+%Y-%m-%d_%H%M%S%s'`
mkdir -p $base_dir/logs/$date
log_file=$base_dir"/logs/"$date"/gwbcMissingdataingestion_"$date_ts".log"

spark-submit --class edf.missingdataload.FindAndLoadMissingRecords \
 --master yarn --deploy-mode cluster \
 --conf $spark_common_conf \
 --conf spark.executor.cores=5 --conf spark.executor.memory=30g --conf spark.driver.memory=30g --conf spark.executor.memoryOverhead=3g \
 --conf spark.dynamicAllocation.enabled=true \
 --conf spark.dynamicAllocation.maxExecutors=100 \
 --conf spark.dynamicAllocation.minExecutors=5 \
 --name "GWBC_Missing_DataIngestion"  \
 --files ${spec_files_path}/fairScheduler.xml#fairScheduler.xml,${spec_files_path}/gwbc_missing.properties#diProperties.properties,${base_dir}/gwbc/spec_files/gwbc_lookup_info.csv#gwbc_lookup_info.csv,${base_dir}/gwbc/spec_files/gwbc_pii_spec.csv#gwbc_pii_spec.csv,${base_dir}/gwbc/spec_files/gwbc_table_spec.csv#gwbc_table_spec.csv,${spec_files_path}/gwbc_missing_table_list.csv#gwbc_missing_table_list.csv,/etc/spark/conf/hive-site.xml \
 --properties-file /usr/lib/spark/conf/spark-defaults.conf \
 --jars /home/hadoop/processed/jars/sqljdbc42.jar \
 ${jar_path}/${spark_app_jar_file_suffix}-${spark_app_jar_file_version} \
GWBC_Missing_DataIngestion 1>>$log_file 2>&1
status=$?
echo "Completed the script and the status is $status"
if [ ${status} -eq 0 ]; then
	exit 0
else
	echo "Spark job failed with error status for GWBC CI, Please have a look at the logs in below file on the server in the appropriate s3 bucket for yarn logs"
exit 1
fi

