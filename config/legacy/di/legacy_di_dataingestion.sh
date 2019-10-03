#!/bin/bash


echo "Started the DI script for Legacy source"
base_dir=/home/hadoop/edf
hadoop_base_dir=/user/hadoop/edf
spec_files_path=$base_dir/legacy/di
jar_path=$base_dir/jars
spark_app_jar_file_suffix=edf_dataingestion-assembly
spark_app_jar_file_version=1.1.1.jar

spark-submit --class edf.dataingestion.DataLoad \
 --master yarn --deploy-mode cluster  --conf spark.shuffle.spill=true \
 --conf spark.executor.extraJavaOptions=-XX:MaxPermSize=512m \
 --conf spark.sql.planner.externalSort=true --conf spark.shuffle.manager=sort \
 --conf spark.ui.port=8088 --conf spark.executor.memoryOverhead=12096  \
 --conf spark.rpc.message.maxSize=1024 --conf spark.file.transferTo=false \
  --conf spark.driver.maxResultSize=10g --conf spark.rdd.compress=true \
 --conf spark.executor.extraJavaOptions="-Dconfig.resource=spark-defaults.conf" \
 --conf spark.driver.JavaOptions="-Dspark.yarn.app.container.log.dir=/mnt/var/log/hadoop" \
 --conf spark.driver.extraJavaOptions="-Dconfig.file=spark-defaults.conf" \
 --conf "spark.sql.parquet.writeLegacyFormat=true" \
 --conf spark.executor.memory=50g --conf spark.driver.memory=20g  \
 --conf spark.executor.cores=5 \
 --conf spark.dynamicAllocation.enabled=true \
 --conf spark.dynamicAllocation.maxExecutors=10 \
 --conf spark.dynamicAllocation.minExecutors=1 \
 --conf spark.executor.instances=3 \
 --conf spark.serializer=org.apache.spark.serializer.KryoSerializer  \
 --name "Legacy_DI_DataIngestion"  \
 --files $spec_files_path/legacy.properties#diProperties.properties,$spec_files_path/legacy_lookup_info.csv#legacy_lookup_info.csv,$spec_files_path/legacy_pii_spec.csv#legacy_pii_spec.csv,$spec_files_path/legacy_tables_spec.csv#legacy_tables_spec.csv,/etc/spark/conf/hive-site.xml \
 --properties-file /usr/lib/spark/conf/spark-defaults.conf \
 $jar_path/$spark_app_jar_file_suffix-$spark_app_jar_file_version \
Legacy_DI_DataIngestion 

status=$?
echo "Completed the DI script for Legacy and the status is $status"
if [ ${status} -eq 0 ]; then
	exit 0
else
	echo "Spark job failed with error status, Please have a look at the logs in s3 path s3a://sa-l4f-emr-edl/logs "

exit 1
fi

