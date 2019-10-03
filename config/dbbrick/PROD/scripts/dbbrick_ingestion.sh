#!/bin/bash


echo "Started the DI script for dbbrick source"
base_dir=/home/hadoop/edf
hadoop_base_dir=/user/hadoop/edf

DATE=`date +%Y-%m-%d`
mkdir -p $base_dir"/logs/"$DATE
DATE_TS=`date '+%Y-%m-%d_%H%M%S%s'`
log_file=$base_dir"/logs/"$DATE"/dbbrickDataIngestion_"$DATE_TS".log"

spec_files_path=$base_dir/dbbrick/spec_files
jar_path=$base_dir/jars
spark_app_jar_file_suffix=edf_dataingestion-assembly
spark_app_jar_file_version=1.1.1.jar

spark-submit --class edf.dataingestion.DataLoad \
 --master yarn --deploy-mode cluster  --conf spark.shuffle.spill=true \
 --conf spark.executor.extraJavaOptions=-XX:MaxPermSize=1024m \
 --conf spark.sql.planner.externalSort=true --conf spark.shuffle.manager=sort \
 --conf spark.ui.port=8088 --conf spark.executor.memoryOverhead=5g  \
 --conf spark.rpc.message.maxSize=1024 --conf spark.file.transferTo=false \
 --conf spark.driver.maxResultSize=10g --conf spark.rdd.compress=true \
 --conf spark.executor.extraJavaOptions="-Dconfig.resource=spark-defaults.conf" \
 --conf spark.driver.JavaOptions="-Dspark.yarn.app.container.log.dir=/mnt/var/log/hadoop" \
 --conf spark.driver.extraJavaOptions="-Dconfig.file=spark-defaults.conf" \
 --conf "spark.sql.parquet.writedbbrickFormat=true" \
 --conf spark.executor.memory=50g --conf spark.driver.memory=20g  \
 --conf spark.executor.cores=10 \
 --conf spark.dynamicAllocation.enabled=true \
 --conf spark.dynamicAllocation.maxExecutors=100 \
 --conf spark.dynamicAllocation.minExecutors=5 \
 --conf spark.executor.instances=1 \
 --conf spark.serializer=org.apache.spark.serializer.KryoSerializer  \
 --conf spark.scheduler.mode=FAIR \
 --name "dbbrick_DataIngestion"  \
 --files $spec_files_path/dbbrick.properties#diProperties.properties,$spec_files_path/dbbrick_lookup_info.csv#dbbrick_lookup_info.csv,$spec_files_path/dbbrick_pii_spec.csv#dbbrick_pii_spec.csv,$spec_files_path/dbbrick_table_spec.csv#dbbrick_table_spec.csv,/etc/spark/conf/hive-site.xml \
 --properties-file /usr/lib/spark/conf/spark-defaults.conf \
 $jar_path/$spark_app_jar_file_suffix-$spark_app_jar_file_version \
dbbrick_DataIngestion 1>>$log_file 2>&1

status=$?
echo "Completed the DI script for dbbrick and the status is $status"
if [ ${status} -eq 0 ]; then
	exit 0
else
	echo "Spark job failed with error status, Please have a look at the logs in below file on the server ${log_file}"

exit 1
fi
