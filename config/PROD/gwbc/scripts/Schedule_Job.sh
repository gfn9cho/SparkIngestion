#!/bin/bash

ech0 "Starting with the DIs"

       echo "starting with DI 2"
       sh /home/hadoop/processed/gwbc/scripts/gwbc_dataingestion_di.sh
       status=$?
       if [ ${status} -eq 0 ]; then
               echo "Completed the second  DI script and the status is $status"
               echo "starting with Hard delete batch"
               sh /home/hadoop/processed/gwbc/scripts/gwbc_dataingestion_delete.sh
               status=$?
               if [ ${status} -eq 0 ]; then
                       echo "Completed the hard delete batch script and the status is $status"
                       echo "starting with staging load"

                       sh /home/hadoop/staging/gwbc/gwbc_staging.sh
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
               echo "Spark job failed with error status for 2nd DI load"
               exit 1
       fi