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
          echo "sh $APP/yb-apache-hbase/src/main/scripts/fnma/DailyLoanProcess.sh $as_of_date"
          sh $APP/yb-apache-hbase/src/main/scripts/fnma/DailyLoanProcess.sh $as_of_date
          daily_return_code=$?
          if [[ $daily_return_code -lt 10 ]]
          then
          	echo "sh $APP/yb-apache-hbase/src/main/scripts/fnma/Impala_fnma_daily_export.sh $as_of_date $daily_return_code"
          	sh -x $APP/yb-apache-hbase/src/main/scripts/fnma/Impala_fnma_daily_export.sh $as_of_date $daily_return_code
          fi
          		  
		  ls /net/ybr-prodnfs11/vendordata-PROD/data-grp15/embsdata/embs/daily/$as_of_date/Products/FNMMONLL.ZIP
          if [[ $? -eq 0 ]]
          then
          	echo "sh $APP/yb-apache-hbase/src/main/scripts/fnma/MonthlyInitializeProcess.sh $as_of_date"
          	sh $APP/yb-apache-hbase/src/main/scripts/fnma/MonthlyInitializeProcess.sh $as_of_date
          	echo "sh $APP/yb-apache-hbase/src/main/scripts/fnma/MonthlyLoanProcess.sh $as_of_date"
          	sh $APP/yb-apache-hbase/src/main/scripts/fnma/MonthlyLoanProcess.sh $as_of_date
          	monthly_return_code=$?
          	if [[ $monthly_return_code -lt 10 ]]
          	then          	
	          	echo "sh $APP/yb-apache-hbase/src/main/scripts/fnma/Impala_fnma_monthly_export.sh $as_of_date $monthly_return_code"
	          	sh -x $APP/yb-apache-hbase/src/main/scripts/fnma/Impala_fnma_monthly_export.sh $as_of_date $monthly_return_code
			fi
          	start_of_month=${as_of_date:0:6}
          	start_of_month+="01"
          	d1=$start_of_month
          	while [ "$d1" -le "$as_of_date" ]
          	do
			  day1=$(date -d "$d1" +%a)
			  if [[ "$day1" != "Sun" ]]
			  then
			     day1Int=$(echo $d1 | tr -d "-")
			     echo "sh $APP/yb-apache-hbase/src/main/scripts/fnma/DailyLoanProcess.sh $day1Int UpdateHbaseOnly"
			     sh $APP/yb-apache-hbase/src/main/scripts/fnma/DailyLoanProcess.sh $day1Int UpdateHbaseOnly
			  fi
			  d1=$(date -I -d "$d1 + 1 day")
        	  d1=$(echo $d1 | tr -d "-")       	
          	done
          fi
        fi
        d=$(date -I -d "$d + 1 day")
        d=$(echo $d | tr -d "-")
done