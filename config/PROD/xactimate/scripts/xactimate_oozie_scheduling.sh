#!/bin/bash


echo "Started the script for Xactimatre incremental job "
base_dir=/home/hadoop/processed
date=`date +%Y-%m-%d`
date_ts=`date '+%Y-%m-%d_%H%M%S%s'`
mkdir -p $base_dir/logs/$date
log_file=$base_dir"/logs/"$date"/xactimate_dataingestion_"$date_ts".log"

host=`hostname`
echo "Host : ${host}"
python3 $base_dir/xactimate/scripts/xactimate_parsing.py 1>>$log_file 2>&1

status=$?
echo "Completed the script and the status is $status"
if [ ${status} -eq 0 ]; then
        exit 0
else
        echo "Spark job failed with error status, Please have a look at the logs file ${log_file} for more details"
exit 1
fi


