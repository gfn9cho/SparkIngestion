#!/bin/bash
###############################################################################
#                               General Details                               #
###############################################################################
#                                                                             #
# Name         : avgrsrvs_lf_load.sh                                          #
#                                                                             #
# Description  : Script to do Average Reserves Lake Factor final Load         #
#                                                                             #
# Author       : State Auto                                                   #
#                                                                             #
###############################################################################
#                          Script Environment Setup                           #
###############################################################################
SCRIPT_PATH="${BASH_SOURCE[0]}"
SCRIPT_HOME=`dirname $SCRIPT_PATH`
Run_YYYYMM=$(date +%Y%m)
Run_MMMYYYY=$(date +%b%Y)
echo "Current YYYYMM: ${Run_YYYYMM}"
echo "Current MMMYYYY: ${Run_MMMYYYY}"

## Parameters passed to Class file executed from a given JAR thru Spark-submit command ##
: '
SourceDB=""
SourceTable="plcy_averagereserveamts_test"
TargetDB=""
TargetTableFinal="plcy_averagereserveamts"
StatTable="plcy_avgrsrvs_stat"
JAR_PATH=""
LOG_PATH=""
'
source ${SCRIPT_HOME}/details.properties
table_folder=${TBL_PATH}${TargetFinal}/
backup_path=${TBL_PATH}${TargetFinal}_backup/

## Log File Details ##
V_EPOC_TIME="`TZ=America/New_York date +'%Y%d%m%H%M%S'`"
v_Log=${LOG_PATH}/${TargetFinal}_${V_EPOC_TIME}.log
export LOGFILE=${v_Log}
#info "log:${v_Log}"
echo "log:${v_Log}"
echo "Log file path :${v_Log}" 2>&1
exec 1> ${v_Log} 2>&1

echo "Creating backup of the table in ${backup_path}"
   aws s3 sync ${table_folder} ${backup_path} || true

echo "[INFO] Spark Submit in Progress"
  
spark-submit \
--conf spark.dynamicAllocation.enabled=true \
--conf spark.dynamicAllocation.minExecutors=2 \
--conf spark.dynamicAllocation.maxExecutors=17 \
--conf spark.yarn.blacklist.executor.launch.blacklisting.enabled=true \
--master yarn --deploy-mode client \
--name "AVGRESV_MANUAL_FINAL_LOAD" \
--class com.sa.edwexternal.Avg_Rsrvs_LF_Load ${JAR_PATH}/Average_Reserves_Lake_Factor-1.0-SNAPSHOT.jar ${SourceDB} ${TargetTable} ${TargetDB} ${TargetFinal} ${StatTable} ${LOGFILE}

echo "[INFO] Script Ends Here..."  

# >> ${v_Log} 2>&1
  

###############################################################################
#                                   END                                       #
###############################################################################
#echo "${v_Log}" 2>&1             
