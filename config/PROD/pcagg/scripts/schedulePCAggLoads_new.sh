#!/bin/bash

ech0 "Starting with the DIs"

echo "starting with DI 1"
sh /home/hadoop/processed/pcagg/scripts/pcagg_dataingestion_di_new.sh
status=$?
if [ ${status} -eq 0 ]; then
	echo "Completed the DI script and the status is $status"
        echo "starting with Hard delete batch"
	sh /home/hadoop/processed/pcagg/scripts/pcagg_dataingestion_delete_new.sh
	status=$?
	if [ ${status} -eq 0 ]; then
		echo "Completed the hard delete batch script and the status is $status"
        	echo "starting with staging load"
		sh /home/hadoop/staging/pcagg/scripts/pcagg_staging_new.sh
		status=$?
	        if [ ${status} -eq 0 ]; then
	                echo "competed the GWCC Staging layer"
        	        exit 0;
			else
				echo "Spark job failed with error status for staging layer"
				exit 1;
			fi
	else
		echo "Spark job failed with error status for hard delete batch load"
	    exit 1
	fi

else
        echo "Spark job failed with error status for DI load"
	exit 1
fi

echo "completed staging load, and the status : ${status}"



