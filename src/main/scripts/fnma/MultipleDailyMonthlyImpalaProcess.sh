#!/bin/bash

START=$(date +%s)
currentUser=$(whoami)
if [ "$currentUser" != "oozie" ]
then
    echo "Must login in as oozie"
    exit 10
fi

start=$1
if [ -z "$start" ]
then
      echo "Must input start date!"
      exit 11
fi

end=$2
if [ -z "$end" ]
then
      echo "Must input end date!"
      exit 12
fi

if [ -z "$BASE" ]
then
  source $OOZIE_HOME/SetupEnv.sh
fi

startDay=$(date -d "$start" +"%Y%m%d")
endDay=$(date -d "$end" +"%Y%m%d")
today=$(date +"%Y%m%d")
if [[ $startDay -gt $endDay ]]
then
    echo "Start day cannot after end day!"
    exit 13
fi

if [[ $endDay -gt $today ]]
then
    echo "End day cannot after today!"
    exit 14
fi      

d=$start
#d=$(date -d "$start" +"%Y-%m-%d")
end=$(echo $end | tr -d "-")

while [ "$d" -le "$end" ]
do
        day=$(date -d "$d" +%a)
        if [[ "$day" != "Sun" ]]
        then
		  as_of_date=$(echo $d | tr -d "-")
          echo $as_of_date
          echo "sh $APP/yb-apache-hbase/src/main/scripts/fnma/Impala_fnma_daily_export.sh $as_of_date"
          sh $APP/yb-apache-hbase/src/main/scripts/fnma/Impala_fnma_daily_export.sh $as_of_date
          		  
		  ls /net/ybr-prodnfs11/vendordata-PROD/data-grp15/embsdata/embs/daily/$as_of_date/Products/FNMMONLL.ZIP
          if [[ $? -eq 0 ]]
          then
          	echo "sh $APP/yb-apache-hbase/src/main/scripts/fnma/Impala_fnma_monthly_export.sh $as_of_date"
          	sh $APP/yb-apache-hbase/src/main/scripts/fnma/Impala_fnma_monthly_export.sh $as_of_date
          fi
        fi
        d=$(date -I -d "$d + 1 day")
        d=$(echo $d | tr -d "-")
done
