#!/bin/bash

######################################################################################
#                              General Details                                       #
######################################################################################
#                                                                                    #
# Script Name : execute_oozie.sh                                                     #
# Description : monthly load dmact,v6pif,v7pif,v7mqte,actbill execute oozie workflow #
# Author      : edlake run team                                                      #
#                                                                                    #
######################################################################################

workflow_name=$1
base_dir=/home/hadoop/processed
polling_base_dir=/home/hadoop/polling
DATE_TS=`date '+%Y-%m-%d_%H%M%S%s'`
log_file=${polling_base_dir}/logs/oozie_logs/${workflow_name}_${DATE_TS}"_oozie.log"
STATUS_FILE=${polling_base_dir}/logs/oozie_logs/${workflow_name}_${DATE_TS}"_status.log"
source $polling_base_dir/.app.config


WAIT_INTERVAL=10

LOGGER() {
  if [ "${1}" = "W" ] ; then
      MESSAGE_TYPE="WARNING"
  elif [ "${1}" = "E" ] ; then
      MESSAGE_TYPE="ERROR"
  else MESSAGE_TYPE="INFO"
  fi
  echo "["`date "+%Y-%m-%d-%H:%M:%S.%6N"`"][${MESSAGE_TYPE}] : ${2}" | tee -a $log_file
}

LOGGER I "Start execute oozie $workflow_name job monthly load script"


oozie_url="http://ip-10-84-38-68.corp.stateauto.com:11000/oozie"

LOGGER I "oozie job --oozie ${oozie_url} -config ${base_dir}/${workflow_name}/oozie/${workflow_name}_job.properties -run"

oozie_jobId=`oozie job --oozie ${oozie_url} -config ${base_dir}/${workflow_name}/oozie/${workflow_name}_job.properties -run`

exitCode=$?
        if [[ ${exitCode} -ne 0 ]];then
        LOGGER I "oozie workflow - ${workflow_name} has failed, Exiting.."
        exit 1
        fi

actual_id=`echo ${oozie_jobId}|cut -d':' -f2`

LOGGER I "Workflow submitted with Job ID: ${actual_id}" 
LOGGER I "Complete the $workflow_name execute oozie script"


