#!/bin/bash


echo "Started the script"
DATE=`date +%Y-%m-%d`
base_dir=/home/hadoop/edf
mkdir -p $base_dir"/logs/"$DATE
DATE_TS=`date '+%Y-%m-%d_%H%M%S%s'`
log_file=$base_dir"/logs/"$DATE"/v7quoteDataIngestion_"$DATE_TS".log"
spec_files_path=$base_dir/v7pif/spec_files
jar_path=$base_dir/jars/
#spark_app_jar_file_suffix=edf_dataingestion_assembly-2.11-
#spark_app_jar_file_version=1.0.1.jar
spark_app_jar_file_suffix=edf_dataingestion_v6_v7-assembly-
spark_app_jar_file_version=2.0.0.jar

#spark_app_jar_file_suffix=dataingestion_upgrade_2.11-
#spark_app_jar_file_version=0.1.jar
#spark_app_jar_file_suffix=edf_dataingestion_assembly-2.11-
#spark_app_jar_file_suffix=edf_dataingestion_assembly-
#spark_app_jar_file_suffix=edf_dataingestion_v6_v7-assembly-
#spark_app_jar_file_version=2.0.0.jar
#spark_app_jar_file_version=0.1.jar
#spark_app_jar_file_version=1.0.0.jar

spark-submit --class edf.DataIngestion.DataIngestionUpgraded \
--master yarn --deploy-mode cluster --conf spark.shuffle.spill=true \
 --conf spark.executor.extraJavaOptions=-XX:MaxPermSize=512m \
 --conf spark.sql.planner.externalSort=true --conf spark.shuffle.manager=sort \
 --conf spark.ui.port=8088 --conf spark.yarn.executor.memoryOverhead=12096  \
 --conf spark.rpc.message.maxSize=1024 --conf spark.file.transferTo=false \
 --conf spark.driver.maxResultSize=10g --conf spark.rdd.compress=true \
 --conf spark.executor.extraJavaOptions="-Dconfig.resource=spark-defaults.conf" \
 --conf spark.driver.JavaOptions="-Dspark.yarn.app.container.log.dir=/mnt/var/log/hadoop" \
 --conf spark.driver.extraJavaOptions="-Dconfig.file=spark-defaults.conf" \
 --conf "spark.sql.parquet.writeLegacyFormat=true" \
 --conf "spark.sql.parquet.writeLegacyFormat=true" \
 --conf spark.executor.memory=15g --conf spark.driver.memory=8g  \
 --conf spark.executor.instances=3 --conf spark.serializer=org.apache.spark.serializer.KryoSerializer  \
 --queue default  \
 --name V7Quote_DI_DataIngestion \
 --jars $jar_path/sqljdbc42.jar,$jar_path/spark-csv_2.11-1.5.0.jar,$jar_path/$spark_app_jar_file_suffix$spark_app_jar_file_version  \
 --files $spec_files_path/v7quote.properties#diProperties.properties,$spec_files_path/v7quote_LookupAnalysis.csv#v7quote_LookupAnalysis.csv,$spec_files_path/v7quote_PIISpec.csv#v7quote_PIISpec.csv,$spec_files_path/v7quote_TableSpec.csv#v7quote_TableSpec.csv,/etc/spark/conf/hive-site.xml \
 --properties-file /usr/lib/spark/conf/spark-defaults.conf \
 $jar_path/$spark_app_jar_file_suffix$spark_app_jar_file_version  1>>$log_file 2>&1


echo "Completed the script"
if [ $? -eq 0 ]; then
exit 0
else
exit 1
fi

