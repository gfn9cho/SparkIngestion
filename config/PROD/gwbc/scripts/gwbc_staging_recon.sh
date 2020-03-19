#!/bin/bash


echo "Started the script for GWBC 34DI Staging "
base_dir=/home/hadoop/processed
hadoop_base_dir=/user/hadoop/processed

spec_files_path=$base_dir/gwbc/spec_files
jar_path=$base_dir/jars
spark_app_jar_file_suffix=edf_dataingestion-assembly
spark_app_jar_file_version=2.1.0.jar
date=`date +%Y-%m-%d`
date_ts=`date '+%Y-%m-%d_%H%M%S%s'`
mkdir -p $base_dir/logs/$date
log_file=$base_dir"/logs/"$date"/gwbcReconIncr_"$date_ts".log"

spark-submit --class edf.recon.ReconJob \
 --master yarn --deploy-mode cluster  --conf spark.shuffle.spill=true \
 --conf spark.executor.extraJavaOptions=-XX:MaxPermSize=512m \
 --conf spark.scheduler.mode=FAIR \
 --conf spark.scheduler.allocation.file=fairScheduler.xml \
 --conf spark.yarn.maxAppAttempts=1 \
 --conf spark.sql.planner.externalSort=true --conf spark.shuffle.manager=sort \
 --conf spark.ui.port=8088 --conf spark.executor.memoryOverhead=12096  \
 --conf spark.rpc.message.maxSize=1024 --conf spark.file.transferTo=false \
 --conf spark.driver.maxResultSize=10g --conf spark.rdd.compress=true \
 --conf spark.executor.extraJavaOptions="-Dconfig.resource=spark-defaults.conf" \
 --conf spark.driver.JavaOptions="-Dspark.yarn.app.container.log.dir=/mnt/var/log/hadoop" \
 --conf spark.driver.extraJavaOptions="-Dconfig.file=spark-defaults.conf" \
 --conf "spark.sql.parquet.writeLegacyFormat=true" \
 --conf spark.executor.memory=15g --conf spark.driver.memory=16g  \
 --conf spark.executor.cores=5 \
 --conf spark.dynamicAllocation.enabled=true \
 --conf spark.dynamicAllocation.maxExecutors=50 \
 --conf spark.dynamicAllocation.minExecutors=3 \
 --conf spark.executor.instances=1 \
 --conf spark.serializer=org.apache.spark.serializer.KryoSerializer  \
 --name "gwbc_recon_staging_incremental"  \
  --files ${spec_files_path}/fairScheduler.xml#fairScheduler.xml,${spec_files_path}/gwbc_staging_DI.properties#diProperties.properties,${spec_files_path}/gwbc_lookup_info.csv#gwbc_lookup_info.csv,${spec_files_path}/gwbc_pii_spec.csv#gwbc_pii_spec.csv,${spec_files_path}/gwbc_table_spec.csv#gwbc_table_spec.csv,${spec_files_path}/gwbc_staging_list.csv#gwbc_staging_list.csv,/etc/spark/conf/hive-site.xml \
 --properties-file /usr/lib/spark/conf/spark-defaults.conf \
 ${jar_path}/${spark_app_jar_file_suffix}-${spark_app_jar_file_version} \
gwbc_recon_staging_incremental  1>>$log_file 2>&1
status=$?
echo "Completed the script and the status is $status"
if [ ${status} -eq 0 ]; then
	exit 0
else
	echo "Spark job failed with error status for GWBC DI, Please have a look at the logs in below file on the server in the appropriate s3 bucket for yarn logs"
exit 1
fi

