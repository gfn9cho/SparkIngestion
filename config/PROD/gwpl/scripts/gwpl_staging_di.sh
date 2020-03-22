#!/bin/bash


echo "Started the script for gwpl staging incremental load"
source /home/hadoop/conf/env_conf.properties
date=`date +%Y-%m-%d`
date_ts=`date '+%Y-%m-%d_%H%M%S%s'`
mkdir -p $ingestion_base_dir/logs/$date
log_file=$ingestion_base_dir"/logs/"$date"/gwplStaging_"$date_ts".log"

spec_files_path=$ingestion_base_dir/gwpl/spec_files

spark-submit --class edf.dataload.DataIngestion \
 --master yarn --deploy-mode cluster  \
 --conf $spark_common_conf \
 --conf spark.scheduler.mode=FAIR \
 --conf spark.scheduler.allocation.file=fairScheduler.xml \
 --conf spark.executor.memory=15g --conf spark.driver.memory=15g  \
 --conf spark.executor.cores=5 \
 --conf spark.dynamicAllocation.enabled=true \
 --conf spark.dynamicAllocation.maxExecutors=50 \
 --conf spark.dynamicAllocation.minExecutors=6 \
 --name "gwpl_di_staging_incremental"  \
  --files ${spec_files_path}/fairScheduler.xml#fairScheduler.xml,${spec_files_path}/gwpl_staging_DI.properties#diProperties.properties,${spec_files_path}/gwpl_lookup_info.csv#gwpl_lookup_info.csv,${spec_files_path}/gwpl_pii_spec.csv#gwpl_pii_spec.csv,${spec_files_path}/gwpl_table_spec.csv#gwpl_table_spec.csv,${spec_files_path}/gwpl_staging_list.csv#gwpl_staging_list.csv,/etc/spark/conf/hive-site.xml \
 --properties-file /usr/lib/spark/conf/spark-defaults.conf \
 $ingestion_jar_path/$staging_incr_jar_name \
gwpl_di_staging_incremental  1>>$log_file 2>&1
status=$?
echo "Completed the script and the status is $status"
if [ ${status} -eq 0 ]; then
	exit 0
else
	echo "Spark job failed with error status for GWPL DI, Please have a look at the logs in below file on the server in the appropriate s3 bucket for yarn logs"
exit 1
fi

