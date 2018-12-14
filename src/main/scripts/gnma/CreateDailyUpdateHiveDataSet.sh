#!/bin/bash

if [ -z "$BASE" ]
then
  source $OOZIE_HOME/SetupEnv.sh
fi

currentUser=$(whoami)
if [ "$currentUser" != "oozie" ]
then
    echo "Must login in as oozie"
    exit 3;
fi

cd $APP/yb-apache-hbase/src/main/avro
kite-dataset delete dataset:hive://$HIVE:9083/prd1/gnma_loan_daily
kite-dataset create dataset:hive://$HIVE:9083/prd1/gnma_loan_daily --schema gnma_loan_daily.avsc --partition-by gnma_daily_partition.json --format parquet
kite-dataset delete dataset:hive://$HIVE:9083/prd1/gnma_arm_loan_daily
kite-dataset create dataset:hive://$HIVE:9083/prd1/gnma_arm_loan_daily --schema gnma_arm_loan_daily.avsc --partition-by gnma_daily_partition.json --format parquet

