#!/bin/ksh -x

pcagg_stage_spec=/home/hadoop/staging/pcagg/spec_files/pcagg_staging_list.csv
pcagg_table_spec=/home/hadoop/processed/pcagg/spec_files/pcagg_table_spec.csv

pcagg_stg_list=/home/hadoop/processed/pcagg/spec_files/pcagg_stg_spec.csv

>$pcagg_stg_list

while read line
do
tbl_name=$(echo $line)
echo $tbl_name
table_list=`cat $pcagg_table_spec|grep -w $tbl_name >>$pcagg_stg_list`
echo $table_list
done < $pcagg_stage_spec



