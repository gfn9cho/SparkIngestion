#!/bin/bash

echo "Starting with the DIs"

echo "starting with DI"
sh /home/hadoop/processed/gwcl/scripts/gwcl_dataingestion_di.sh
status=$?
if [ ${status} -eq 0 ]; then
	echo "Completed the DI script and the status is $status"
        echo "starting with Hard delete batch"
	sh /home/hadoop/processed/gwcl/scripts/gwcl_dataingestion_delete.sh
	status=$?
	if [ ${status} -eq 0 ]; then
		echo "Completed the hard delete batch script and the status is $status"
        	echo "starting with staging load"
		sh /home/hadoop/staging/gwcl/scripts/gwcl_staging.sh
		status=$?
	        if [ ${status} -eq 0 ]; then
	                echo "competed the Staging layer"
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





