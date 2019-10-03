#!/bin/bash


echo "Started the script for ESE TL "
base_dir=/home/hadoop/edf
hadoop_base_dir=/user/hadoop/edf

spec_files_path=$base_dir/ese/spec_files
jar_path=$base_dir/jars
spark_app_jar_file_suffix=edf_dataingestion-assembly
spark_app_jar_file_version=1.1.7.jar
date=`date +%Y-%m-%d`
date_ts=`date '+%Y-%m-%d_%H%M%S%s'`
mkdir -p $base_dir/logs/$date
log_file=$base_dir"/logs/"$date"/eseTLdataingestion_"$date_ts".log"

spark-submit --class edf.dataingestion.DataLoad \
 --master yarn --deploy-mode cluster  --conf spark.shuffle.spill=true \
 --conf spark.executor.extraJavaOptions=-XX:MaxPermSize=512m \
 --conf spark.sql.planner.externalSort=true --conf spark.shuffle.manager=sort \
 --conf spark.ui.port=8088 --conf spark.executor.memoryOverhead=5g  \
 --conf spark.rpc.message.maxSize=1024 --conf spark.file.transferTo=false \
  --conf spark.driver.maxResultSize=10g --conf spark.rdd.compress=true \
 --conf spark.executor.extraJavaOptions="-Dconfig.resource=spark-defaults.conf" \
 --conf spark.driver.JavaOptions="-Dspark.yarn.app.container.log.dir=/mnt/var/log/hadoop" \
 --conf spark.driver.extraJavaOptions="-Dconfig.file=spark-defaults.conf" \
 --conf "spark.sql.parquet.writeLegacyFormat=true" \
 --conf spark.executor.memory=18g --conf spark.driver.memory=20g  \
 --conf spark.executor.cores=5 \
 --conf spark.dynamicAllocation.enabled=true \
 --conf spark.dynamicAllocation.maxExecutors=50 \
 --conf spark.dynamicAllocation.minExecutors=1 \
 --conf spark.executor.instances=6 \
 --conf spark.serializer=org.apache.spark.serializer.KryoSerializer  \
 --name "ESE_TL_DataIngestion"  \
  --files ${spec_files_path}/ese_TL.properties#diProperties.properties,${spec_files_path}/ese_lookup_info.csv#ese_lookup_info.csv,${spec_files_path}/ese_pii_spec.csv#ese_pii_spec.csv,${spec_files_path}/ese_table_spec_TL.csv#ese_table_spec_TL.csv,/etc/spark/conf/hive-site.xml \
 --properties-file /usr/lib/spark/conf/spark-defaults.conf \
 ${jar_path}/${spark_app_jar_file_suffix}-${spark_app_jar_file_version} \
ESE_TL_DataIngestion 1>>$log_file 2>&1
status=$?
echo "Completed the script and the status is $status"
if [ ${status} -eq 0 ]; then
	exit 0
else
	echo "Spark job failed with error status for ESE TL, Please have a look at  ${log_file} file for yarn logs"
	exit 1
fi

