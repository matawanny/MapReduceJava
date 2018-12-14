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

monthly_process(){
	local asOfDate=$1
	local targetFile="/net/ybr-prodnfs11/vendordata-PROD/data-grp15/embsdata/embs/daily/$asOfDate/Products/"
	local targetFile+=$2
	targetFile+=".ZIP"
	local runImpala=$3
	
	ls $targetFile
	if [[ $? -eq 0 ]]
	 then
	   echo "sh $APP/yb-apache-hbase/src/main/scripts/gnma/MonthlyLoanProcess.sh $1 $asOfDate"
	   sh $APP/yb-apache-hbase/src/main/scripts/gnma/MonthlyLoanProcess.sh $1 $asOfDate
	   monthly_return_code=$?
       if [[ "$runImpala" = "true" && "$monthly_return_code" -lt 10 ]]
       then 	   
		   echo "sh $APP/yb-apache-hbase/src/main/scripts/gnma/Impala_gnma_monthly_export.sh $asOfDate $monthly_return_code"
		   sh $APP/yb-apache-hbase/src/main/scripts/gnma/Impala_gnma_monthly_export.sh $asOfDate $monthly_return_code
	   fi
	 else
	   return 2
	fi
}

while [ "$d" -le "$end" ]
do
        day=$(date -d "$d" +%a)
        if [[ "$day" != "Sun" ]]
        then
          as_of_date=$(echo $d | tr -d "-")
          echo "sh $APP/yb-apache-hbase/src/main/scripts/gnma/DailyLoanProcess.sh $as_of_date"
          sh $APP/yb-apache-hbase/src/main/scripts/gnma/DailyLoanProcess.sh $as_of_date
          daily_return_code=$?
          if [[ $daily_return_code -lt 10 ]]
          then
	          echo "sh $APP/yb-apache-hbase/src/main/scripts/gnma/Impala_gnma_daily_export.sh $as_of_date $daily_return_code"
	          sh $APP/yb-apache-hbase/src/main/scripts/gnma/Impala_gnma_daily_export.sh $as_of_date $daily_return_code
	      fi    
          monthly_process $as_of_date GNMMNILL true
          monthly_process $as_of_date GN1MONLL false		
          monthly_process $as_of_date GN2MONLL true				  
        fi
        d=$(date -I -d "$d + 1 day")
        d=$(echo $d | tr -d "-")
done