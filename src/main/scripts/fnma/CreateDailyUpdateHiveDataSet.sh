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
kite-dataset delete dataset:hive://$HIVE:9083/prd1/fnma_loan_daily
kite-dataset create dataset:hive://$HIVE:9083/prd1/fnma_loan_daily --schema fnma_loan_daily.avsc --partition-by fnma_daily_partition.json --format parquet

kite-dataset delete dataset:hive://$HIVE:9083/prd1/fnma_arm_loan_daily
kite-dataset create dataset:hive://$HIVE:9083/prd1/fnma_arm_loan_daily --schema fnma_arm_loan_daily.avsc --partition-by fnma_daily_partition.json --format parquet

kite-dataset delete dataset:hive://$HIVE:9083/prd1/fnma_mod_loan_daily
kite-dataset create dataset:hive://$HIVE:9083/prd1/fnma_mod_loan_daily --schema fnma_mod_loan_daily.avsc --partition-by fnma_daily_partition.json --format parquet
