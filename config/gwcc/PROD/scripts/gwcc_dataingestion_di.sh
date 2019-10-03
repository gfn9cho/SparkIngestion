#!/bin/bash


base_dir=/home/hadoop/edf
hadoop_base_dir=/user/hadoop/edf
oozie_files_path=$base_dir/gwcc/oozie
spec_files_path=$base_dir/gwcc/spec_files
jar_path=$base_dir/jars
spark_app_jar_file_suffix=edf_dataingestion-assembly
spark_app_jar_file_version=1.1.3.jar
oozie_host=ip-10-64-102-242.corp.stateauto.com
oozie_port=11000


spark-submit --class edf.dataingestion.DataLoad \
--master yarn --deploy-mode cluster  --conf spark.shuffle.spill=true \
 --conf spark.scheduler.mode=FAIR \
 --conf spark.executor.extraJavaOptions=-XX:MaxPermSize=1024m \
 --conf spark.sql.planner.externalSort=true --conf spark.shuffle.manager=sort \
 --conf spark.ui.port=8088 \
 --conf spark.rpc.message.maxSize=1024 --conf spark.file.transferTo=false \
 --conf spark.driver.maxResultSize=2g --conf spark.rdd.compress=true \
 --conf spark.executor.extraJavaOptions="-Dconfig.resource=spark-defaults.conf" \
 --conf spark.driver.JavaOptions="-Dspark.yarn.app.container.log.dir=/mnt/var/log/hadoop" \
 --conf spark.driver.extraJavaOptions="-Dconfig.file=spark-defaults.conf" \
 --conf spark.sql.parquet.writeLegacyFormat=true \
 --conf spark.enable.dynamicAllocation=true \
 --conf spark.dynamicAllocation.maxExecutors=100 \
 --conf spark.dynamicAllocation.minExecutors=1 \
 --conf spark.executor.cores=1 \
 --conf spark.executor.memory=3g --conf spark.driver.memory=25g  \
 --conf spark.executor.instances=1 --conf spark.serializer=org.apache.spark.serializer.KryoSerializer  \
 --name gwcc_di_dataingestion \
 --files $spec_files_path/gwcc_di.properties#diProperties.properties,$spec_files_path/gwcc_lookup_info.csv#gwcc_lookup_info.csv,$spec_files_path/gwcc_pii_spec.csv#gwcc_pii_spec.csv,$spec_files_path/gwcc_table_spec.csv#gwcc_table_spec.csv,/etc/spark/conf/hive-site.xml \
 --properties-file /usr/lib/spark/conf/spark-defaults.conf \
 $jar_path/$spark_app_jar_file_suffix-$spark_app_jar_file_version \
 gwcc_di_dataingestion
 
status=$?
echo "Completed the script and the status is $status"
if [ ${status} -eq 0 ]; then
	exit 0
else
	echo "Spark job failed with error status, Please have a look at the logs in below file on the server in the appropriate s3 bucket for yarn logs"
exit 1
fi

