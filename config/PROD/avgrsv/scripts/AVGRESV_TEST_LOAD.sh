#!/bin/bash
###############################################################################
#                               General Details                               #
###############################################################################
#                                                                             #
# Name         : avgrsrvs_lf_busns.sh                                         #
#                                                                             #
# Description  : Script to do Average Reserves Lake Factor for Business       #
#                                                                             #
# Author       : State Auto                                                   #
#                                                                             #
###############################################################################
#                          Script Environment Setup                           #
###############################################################################
SCRIPT_PATH="${BASH_SOURCE[0]}"
SCRIPT_HOME=`dirname $SCRIPT_PATH`
Run_YYYYMM=$(date +%Y%m)
#Run_MMMYYYY=$(date +%b%Y)
#Run_MMMYYYY=".2019.08.06"
#echo "If not running the job on file received date please add the suffix date in ${Run_MMMYYYY}"
#Run_MMMYYYY=$(date +.%Y.%m.06)
Run_MMMYYYY=$(date +.%Y.%m.%d)
Prev_YYYYMM=$(date +'%Y%m' -d 'last month')
echo "Current YYYYMM: ${Run_YYYYMM}"
echo "Current MMMYYYY: ${Run_MMMYYYY}"
echo "Previous YYYYMM: ${Prev_YYYYMM}"
echo "Note: If not running the job on file received date please add the suffix date from YTC01X file to ${Run_MMMYYYY}"
effective_date=$1
if [ -z "$effective_date" ]
then
    #eff_date="current_date"
    eff_date=$(date +%Y-%m-%d)
else
    eff_date="$effective_date"
fi

source ${SCRIPT_HOME}/details.properties
test_table_path="${TBL_PATH}${TargetTable}/"

Avg_TXT_File=`hdfs dfs -ls -R ${Avg_TXT_Path} | grep 'YTC01X' | sort | tail -1 | awk -F" " '{print $NF}' | awk -F"/" '{print $NF}'`
#Avg_TXT_File="YTC01X${Run_MMMYYYY}.txt"

## Log File Details ##
V_EPOC_TIME="`TZ=America/New_York date +'%Y%d%m%H%M%S'`"
v_Log=${LOG_PATH}/${TargetTable}_${V_EPOC_TIME}.log
export LOGFILE=${v_Log}
#info "log:${v_Log}"
echo "class com.sa.edwexternal.Avg_Rsrvs_LF_For_Busns ${JAR_PATH}/Average_Reserves_Lake_Factor-1.0-SNAPSHOT.jar ${Src_CSV_Path} ${Avg_TXT_Path} ${Src_CSV_File} ${Avg_TXT_File} ${TargetDB} ${TargetTable} ${TargetFinal} ${StatTable} ${LOGFILE} ${eff_date} ${test_table_path}"
echo "log:${v_Log}"
echo "Log file path :${v_Log}" 2>&1
exec 1> ${v_Log} 2>&1

echo "${Avg_TXT_Path}${Avg_TXT_File}"
$(hdfs dfs -test -s ${Avg_TXT_Path}${Avg_TXT_File})

if [ $? != 0 ]
then
   echo "${Src_CSV_Path}${Src_CSV_File}"
   echo "${Avg_TXT_Path}${Avg_TXT_File}"
   echo "[INFO] YTC01X Monthly Reserves File Not Present or Empty"
else
   echo "${Src_CSV_Path}${Src_CSV_File}"
   echo "${Avg_TXT_Path}${Avg_TXT_File}"
   echo "[INFO] YTC01X Monthly Reserves File is Present and is Not Empty"

   echo "[INFO] Reserves Process Starts"
   echo "[INFO] Recreating the ${TargetDB}.${TargetTable} table if exists "
   $(hive -S -e "drop table if exists ${TargetDB}.${TargetTable}") || true
   if $(hdfs dfs -test -s ${test_table_path}); then $(hdfs dfs -rm -r ${test_table_path}); fi || true
   #$(hdfs dfs -rm -r ${TBL_PATH}${TargetTable}/ingestion_yyyymm=${Prev_YYYYMM}) || true

   echo "[INFO] Spark Submit in Progress"   
   echo "class com.sa.edwexternal.Avg_Rsrvs_LF_For_Busns ${JAR_PATH}/Average_Reserves_Lake_Factor-1.0-SNAPSHOT.jar ${Src_CSV_Path} ${Avg_TXT_Path} ${Src_CSV_File} ${Avg_TXT_File} ${TargetDB} ${TargetTable} ${TargetFinal} ${StatTable} ${LOGFILE} ${eff_date} ${test_table_path}"
spark-submit \
--conf spark.dynamicAllocation.enabled=true \
--conf spark.dynamicAllocation.minExecutors=2 \
--conf spark.dynamicAllocation.maxExecutors=17 \
--conf spark.yarn.blacklist.executor.launch.blacklisting.enabled=true \
--master yarn --deploy-mode client \
--name "AVGRESV_MANUAL_TEMP_BUSN_LOAD" \
--class com.sa.edwexternal.Avg_Rsrvs_LF_For_Busns ${JAR_PATH}/Average_Reserves_Lake_Factor-1.0-SNAPSHOT.jar ${Src_CSV_Path} ${Avg_TXT_Path} ${Src_CSV_File} ${Avg_TXT_File} ${TargetDB} ${TargetTable} ${TargetFinal} ${StatTable} ${LOGFILE} ${eff_date} ${test_table_path}

  echo "[INFO] Hive msck repair table in progress for added/deleted partitions"
  $(hive -S -e "msck repair table ${TargetDB}.${TargetTable}")
  echo "[INFO] Scripts Ends Here..."
fi

# >> ${v_Log} 2>&1
  

###############################################################################
#                                   END                                       #
###############################################################################
#echo "${v_Log}" 2>&1             
