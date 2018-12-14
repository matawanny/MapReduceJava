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

kite-dataset delete dataset:hive://$HIVE:9083/prd1/fhlmc_loan_daily
#kite-dataset partition-config --schema fhlmc_loan_daily.avsc as_of_date:copy -o fhlmc_loan_daily.json
kite-dataset create dataset:hive://$HIVE:9083/prd1/fhlmc_loan_daily --schema fhlmc_loan_daily.avsc --partition-by fhlmc_loan_daily.json --format parquet
#kite-dataset csv-import fhlmc_loan.csv fhlmc_loan_daily --delimiter '|'

#kite-dataset update dataset:hive://$HIVE:9083/prd1/fhlmc_loan_daily --schema fhlmc_loan_daily.avsc

kite-dataset delete dataset:hive://$HIVE:9083/prd1/fhlmc_arm_loan_daily
#kite-dataset partition-config --schema fhlmc_arm_loan_daily.avsc as_of_date:copy -o fhlmc_arm_loan_daily.json
kite-dataset create dataset:hive://$HIVE:9083/prd1/fhlmc_arm_loan_daily --schema fhlmc_arm_loan_daily.avsc --partition-by fhlmc_arm_loan_daily.json --format parquet

#kite-dataset csv-import fhlmc_arm_loan.csv fhlmc_arm_loan_daily --delimiter '|'

kite-dataset delete dataset:hive://$HIVE:9083/prd1/fhlmc_mod_loan_daily
#kite-dataset partition-config --schema fhlmc_mod_loan_daily.avsc as_of_date:copy -o fhlmc_mod_loan_daily.json
kite-dataset create dataset:hive://$HIVE:9083/prd1/fhlmc_mod_loan_daily --schema fhlmc_mod_loan_daily.avsc --partition-by fhlmc_mod_loan_daily.json --format parquet
#kite-dataset csv-import fhlmc_mod_loan.csv fhlmc_mod_loan_daily --delimiter '|'


