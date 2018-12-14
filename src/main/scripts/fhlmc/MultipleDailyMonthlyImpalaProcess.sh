#!/bin/bash

START=$(date +%s)
currentUser=$(whoami)
if [ "$currentUser" != "oozie" ]
then
    echo "Must login in as oozie"
    exit 10
fi

if [ -z "$BASE" ]
then
  source $OOZIE_HOME/SetupEnv.sh
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
    exit 15
fi      

d=$start
afterend=$(date -I -d "$end + 1 day")

while [ "$d" != "$afterend" ] ;
do
        day=$(date -d "$d" +%a)
        if [[ "$day" != "Sun" ]]
        then
		  year=$(echo $d|cut -d '-' -f 1)
          month=$(echo $d|cut -d '-' -f 2)
          dayofmonth=$(echo $d|cut -d '-' -f 3)
          length=${#year}
          if [[ $length -gt 4 ]]
          then
             year=${d:0:4}
             month=${d:4:2}
             dayofmonth=${d:7:2}
          fi
          if [[ ${#month} -lt 2 ]]
          then
             monthStr=$month
             month="0"
             month+=$monthStr
          fi
          if [[ ${#dayofmonth} -lt 2 ]]
          then
             dayofmonthStr=$dayofmonth
             dayofmonth="0"
             dayofmonth+=$dayofmonthStr
          fi
          as_of_date=$year
          as_of_date+=$month
          as_of_date+=$dayofmonth
          echo $as_of_date
          echo "sh $APP/yb-apache-hbase/src/main/scripts/fhlmc/Impala_fhlmc_daily_export.sh $as_of_date"
          sh $APP/yb-apache-hbase/src/main/scripts/fhlmc/Impala_fhlmc_daily_export.sh $as_of_date
          		  
          if [ -f "/net/ybr-prodnfs11/vendordata-PROD/data-grp15/embsdata/embs/daily/$AS_OF_DATE/Products/FHLMONLF.ZIP" ]
          then
          	echo "sh $APP/yb-apache-hbase/src/main/scripts/fhlmc/Impala_fhlmc_monthly_export.sh $as_of_date"
          	sh  $APP/yb-apache-hbase/src/main/scripts/fhlmc/Impala_fhlmc_monthly_export.sh $as_of_date
          fi
        fi
        d=$(date -I -d "$d + 1 day")
done
