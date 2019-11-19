#!/bin/bash

##############################################################################
#                              General Details                               #
##############################################################################
#                                                                            #
# Script Name : execute_oozie.sh                                             #
# Description : monthly load dmact,v6pif and v7pif execute oozie workflow    #
# Author      : edlake run team                                              #
#                                                                            #
##############################################################################

workflow_name=$1
base_dir=/home/hadoop/processed
polling_base_dir=/home/hadoop/polling
DATE_TS=`date '+%Y-%m-%d_%H%M%S%s'`
log_file=${polling_base_dir}/logs/oozie_logs/${workflow_name}_${DATE_TS}"_oozie.log"
STATUS_FILE=${polling_base_dir}/logs/oozie_logs/${workflow_name}_${DATE_TS}"_status.log"
source $polling_base_dir/.app.config

WAIT_INTERVAL=5

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


oozie_url="http://ip-10-84-38-68.ec2.internal:11000/oozie"

LOGGER I "oozie job --oozie ${oozie_url} -config ${base_dir}/dmact/${workflow_name}/oozie/${workflow_name}_job.properties -run"

oozie_jobId=`oozie job --oozie ${oozie_url} -config ${base_dir}/dmact/${workflow_name}/oozie/${workflow_name}_job.properties -run`

exitCode=$?
        if [[ ${exitCode} -ne 0 ]];then
        LOGGER I "oozie workflow - ${workflow_name} has failed, Exiting.."
        exit 1
        fi

actual_id=`echo ${oozie_jobId}|cut -d':' -f2`

LOGGER I "Workflow submitted with Job ID: ${actual_id}" 

LOGGER I "Complete the $workflow_name execute oozie script"

: '
LOGGER I "Monitoring the workflow at an interval of $WAIT_INTERVAL seconds"

#while true; do

LOGGER I  "while loop started"
LOGGER I "oozie job --oozie ${oozie_url} -info ${actual_id}"

#oozie job --oozie ${oozie_url} -info ${actual_id} > ${STATUS_FILE}  2>&1
LOGGER I "Check the workflow status"

if [[ -e ${STATUS_FILE} ]]; then
             if [ 1 -eq $(cat ${STATUS_FILE} | grep "Status        :" | grep -c "SUCCEEDED") ] && [ 1 -ne $(cat ${STATUS_FILE} | grep "Ended         : -"| grep -c "-" ) ];then
                LOGGER I "Workflow Successful!!!"
                exit 0
          elif [[ 1 -eq $(cat ${STATUS_FILE} | grep "Status        :" | grep -c "KILLED") || 1 -eq $(cat ${STATUS_FILE} | grep "Status        :" | grep -c "FAILED") ]] && [ 1 -ne $(cat ${STATUS_FILE} | grep "Ended         : -"| grep -c "-" ) ]; then
                LOGGER I "Workflow is not Successful!!!"
                 exit 1

          elif [[ 1 -eq $(cat ${STATUS_FILE} | grep "Status        :" | grep -c "FAILED") || 1 -eq $(cat ${STATUS_FILE} | grep "Status        :" | grep -c "FAILED") ]] && [ 1 -ne $(cat ${STATUS_FILE} | grep "Ended         : -"| grep -c "-" ) ]; then
                LOGGER I "Workflow is not Successful!!!"
                 exit 1
          elif [[ 1 -eq $(cat ${STATUS_FILE} | grep "Status        :" | grep -c "RUNNING") || 1 -eq $(cat ${STATUS_FILE} | grep "Ended         :" | grep -c "-") ]]; then
                LOGGER I "Workflow Running!!!"


          elif [[ 1 -eq $(cat ${STATUS_FILE} | grep "Status        :" | grep -c "PREP") || 1 -eq $(cat ${STATUS_FILE} | grep "Ended         :" | grep -c "-") ]]; then
                LOGGER I "Workflow yet to start!!!"

     fi
fi

    sleep $WAIT_INTERVAL
#done

'








