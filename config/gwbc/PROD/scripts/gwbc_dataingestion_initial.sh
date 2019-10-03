#!/bin/bash


echo "Started the script"
DATE=`date +%Y-%m-%d`
base_dir=/home/hadoop/edf
hadoop_base_dir=/user/hadoop/edf
mkdir -p $base_dir"/logs/"$DATE
DATE_TS=`date '+%Y-%m-%d_%H%M%S%s'`
log_file=$base_dir"/logs/"$DATE"/gwbcDataIngestion_"$DATE_TS".log"
spec_files_path=$base_dir/gwbc/spec_files
jar_path=$base_dir/jars
spark_app_jar_file_suffix=edf_dataingestion-assembly
spark_app_jar_file_version=1.1.5.jar

spark-submit --class edf.dataingestion.DataLoad \
--master yarn --deploy-mode cluster  --conf spark.shuffle.spill=true \
 --conf spark.executor.extraJavaOptions=-XX:MaxPermSize=512m \
 --conf spark.sql.planner.externalSort=true --conf spark.shuffle.manager=sort \
 --conf spark.ui.port=8088 \
 --conf spark.rpc.message.maxSize=1024 --conf spark.file.transferTo=false \
 --conf spark.driver.maxResultSize=10g --conf spark.rdd.compress=true \
 --conf spark.executor.extraJavaOptions="-Dconfig.resource=spark-defaults.conf" \
 --conf spark.driver.JavaOptions="-Dspark.yarn.app.container.log.dir=/mnt/var/log/hadoop" \
 --conf spark.driver.extraJavaOptions="-Dconfig.file=spark-defaults.conf" \
 --conf "spark.sql.parquet.writeLegacyFormat=true" \
 --conf spark.executor.memory=30g --conf spark.driver.memory=20g  \
 --conf spark.executor.cores=10 \
 --conf spark.dynamicAllocation.enabled=false \
 --conf spark.dynamicAllocation.maxExecutors=20 \
 --conf spark.dynamicAllocation.minExecutors=1 \
 --conf spark.executor.instances=3 \
 --conf spark.serializer=org.apache.spark.serializer.KryoSerializer  \
 --name "gwbc_dataingestion"  \
  --files ${spec_files_path}/gwbc_initial.properties#diProperties.properties,${spec_files_path}/gwbc_lookup_info.csv#gwbc_lookup_info.csv,${spec_files_path}/gwbc_pii_spec.csv#gwbc_pii_spec.csv,${spec_files_path}/gwbc_table_spec.csv#gwbc_table_spec.csv,/etc/spark/conf/hive-site.xml \
 --properties-file /usr/lib/spark/conf/spark-defaults.conf \
 ${jar_path}/${spark_app_jar_file_suffix}-${spark_app_jar_file_version} 1>>$log_file 2>&1
status=$?
echo "Completed the script and the status is $status"
if [ ${status} -eq 0 ]; then
	exit 0
else
	echo "Spark job failed with error status, Please have a look at the logs in below path \n ${log_file}"
exit 1
fi


