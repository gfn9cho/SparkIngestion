#!/bin/bash

DATE=`date +%Y-%m-%d`

echo "Started the script for telematics data ingestion for ${DATE}"
base_dir=/home/hadoop/edf
hadoop_base_dir=/user/hadoop/edf
mkdir -p $base_dir"/logs/"$DATE
DATE_TS=`date '+%Y-%m-%d_%H%M%S%s'`
log_file=$base_dir"/logs/"$DATE"/TelematicsDataIngestion_"$DATE_TS".log"
oozie_files_path=$base_dir/telematics/oozie
spec_files_path=$base_dir/telematics
jar_path=$base_dir/jars
spark_app_jar_file_suffix=edf_dataingestion_assembly-2.11
spark_app_jar_file_version=1.0.1.jar
oozie_host=ip-10-64-102-242.corp.stateauto.com
oozie_port=11000

spark-submit --class edf.DataIngestion.DataIngestionUpgraded \
--master yarn --deploy-mode cluster  --conf spark.shuffle.spill=true \
 --conf spark.executor.extraJavaOptions=-XX:MaxPermSize=512m \
 --conf spark.sql.planner.externalSort=true --conf spark.shuffle.manager=sort \
 --conf spark.ui.port=8088 --conf spark.executor.memoryOverhead=12096  \
 --conf spark.rpc.message.maxSize=1024 --conf spark.file.transferTo=false \
 --conf spark.driver.maxResultSize=10g --conf spark.rdd.compress=true \
 --conf spark.executor.extraJavaOptions="-Dconfig.resource=spark-defaults.conf" \
 --conf spark.driver.JavaOptions="-Dspark.yarn.app.container.log.dir=/mnt/var/log/hadoop" \
 --conf spark.driver.extraJavaOptions="-Dconfig.file=spark-defaults.conf" \
 --conf spark.sql.parquet.writeLegacyFormat=true \
 --conf spark.sql.sources.partitionOverwriteMode=dynamic \
 --conf spark.executor.memory=15g --conf spark.driver.memory=10g  \
 --conf spark.executor.cores=5 \
 --conf spark.dynamicAllocation.enabled=false \
 --conf spark.dynamicAllocation.maxExecutors=10 \
 --conf spark.dynamicAllocation.minExecutors=1 \
 --conf spark.executor.instances=3 \
 --conf spark.serializer=org.apache.spark.serializer.KryoSerializer  \
 --name "Telematics_DI_DataIngestion"  \
 --jars ${jar_path}/sqljdbc42.jar,${jar_path}/spark-csv_2.11-1.5.0.jar,${jar_path}/${spark_app_jar_file_suffix}-${spark_app_jar_file_version},${jar_path}/mail-1.4.7.jar \
 --files ${spec_files_path}/telematics.properties#diProperties.properties,${spec_files_path}/telematics_lookup_analysis.csv#telematics_lookup_analysis.csv,${spec_files_path}/telematics_pii_spec.csv#telematics_pii_spec.csv,${spec_files_path}/telematics_tables_spec.csv#telematics_tables_spec.csv,/etc/spark/conf/hive-site.xml \
 --properties-file /usr/lib/spark/conf/spark-defaults.conf \
 ${jar_path}/${spark_app_jar_file_suffix}-${spark_app_jar_file_version} \
Telematics_DI_DataIngestion  1>>$log_file 2>&1
status=$?
echo "Completed the dataingestion script and the status is $status"
if [ ${status} -eq 0 ]; then
	exit 0
else
	echo "Spark job failed with error status, Please have a look at the logs file in ${log_file}"
exit 1
fi


