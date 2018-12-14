#!/bin/bash

print_current_time(){
  local output="`date "+%Y%m%d %H:%M:%S"`"
  echo $output
}

print_current_time
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

if [ -z "$1" ]
then
  today=$(date +"%Y%m%d")
else
  today=$(date -d "$1" +"%Y%m%d")  
fi
echo "today=$today"
weekday=$(date -d "$today" +%a)
case "$weekday" in 
	Mon)
		days=2
		;;
	*)  
	    days=1  
	    ;;	
esac	
lastBusinessDay=$(date -d "$days day ago" +"%Y%m%d")
ls $EXPORT/gnma/$lastBusinessDay/*loan_daily.SIG >/dev/null 2>&1
if [[ $? -ne 0 ]]
then
  d=$lastBusinessDay
else
  d=$today  
fi
end=$today

monthly_process(){
	local asOfDate=$1
	local targetFile="/net/ybr-prodnfs11/vendordata-PROD/data-grp15/embsdata/embs/daily/$asOfDate/Products/"
	targetFile+=$2
	targetFile+=".ZIP"
	runImpala=$3
	
	ls $targetFile
	if [[ $? -eq 0 ]]
	 then
	   echo "sh $APP/yb-apache-hbase/src/main/scripts/gnma/MonthlyLoanProcess.sh $asOfDate $2"
	   sh -x $APP/yb-apache-hbase/src/main/scripts/gnma/MonthlyLoanProcess.sh $asOfDate $2
      	monthly_return_code=$?
      	if [[ $runImpala -eq 1 && $monthly_return_code -lt 10 ]]
      	then	   
		   echo "sh $APP/yb-apache-hbase/src/main/scripts/gnma/Impala_gnma_monthly_export.sh $asOfDate $monthly_return_code"
		   sh -x $APP/yb-apache-hbase/src/main/scripts/gnma/Impala_gnma_monthly_export.sh $asOfDate $monthly_return_code
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
          if [ -f "/net/ybr-prodnfs11/vendordata-PROD/data-grp15/embsdata/embs/daily/$as_of_date/Products/GNMMNILL.ZIP" ]
          then         		  
		  	monthly_process $as_of_date GNMMNILL 1 
		  fi	
          if [ -f "/net/ybr-prodnfs11/vendordata-PROD/data-grp15/embsdata/embs/daily/$as_of_date/Products/GN1MONLL.ZIP" ]
          then
	          if [ -f "/net/ybr-prodnfs11/vendordata-PROD/data-grp15/embsdata/embs/daily/$as_of_date/Products/GN2MONLL.ZIP" ]
	          then                    		  
          		monthly_process $as_of_date GN1MONLL 0
          	  else	
          	  	monthly_process $as_of_date GN1MONLL 1
          	  fi	
          fi		
          if [ -f "/net/ybr-prodnfs11/vendordata-PROD/data-grp15/embsdata/embs/daily/$as_of_date/Products/GN2MONLL.ZIP" ]
          then         		  
          	monthly_process $as_of_date GN2MONLL 1
          fi		
        fi
        d=$(date -I -d "$d + 1 day")
        d=$(echo $d | tr -d "-")
done