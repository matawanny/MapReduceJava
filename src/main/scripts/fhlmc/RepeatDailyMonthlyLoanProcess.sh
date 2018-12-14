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
ls $EXPORT/fhlmc/$lastBusinessDay/*loan_daily.SIG >/dev/null 2>&1
if [[ $? -ne 0 ]]
then
  d=$lastBusinessDay
else
  d=$today  
fi

end=$today

while [ "$d" -le "$end" ]
do
        day=$(date -d "$d" +%a)
        if [[ "$day" != "Sun" ]]
        then
          as_of_date=$(echo $d | tr -d "-")
          echo "sh $APP/yb-apache-hbase/src/main/scripts/fhlmc/DailyLoanProcess.sh $as_of_date"
          sh $APP/yb-apache-hbase/src/main/scripts/fhlmc/DailyLoanProcess.sh $as_of_date
          daily_return_code=$?
          if [[ $daily_return_code -lt 10 ]]
          then
	          echo "sh $APP/yb-apache-hbase/src/main/scripts/fhlmc/Impala_fhlmc_daily_export.sh $as_of_date $daily_return_code"
	          sh -x $APP/yb-apache-hbase/src/main/scripts/fhlmc/Impala_fhlmc_daily_export.sh $as_of_date $daily_return_code
	      fi
          		  
          if [ -f "/net/ybr-prodnfs11/vendordata-PROD/data-grp15/embsdata/embs/daily/$as_of_date/Products/FHLMONLF.ZIP" ]
          then
          	echo "sh $APP/yb-apache-hbase/src/main/scripts/fhlmc/MonthlyInitializeProcess.sh $as_of_date"
          	sh -x $APP/yb-apache-hbase/src/main/scripts/fhlmc/MonthlyInitializeProcess.sh $as_of_date
          	echo "sh $APP/yb-apache-hbase/src/main/scripts/fhlmc/MonthlyLoanProcess.sh $as_of_date"
          	sh -x $APP/yb-apache-hbase/src/main/scripts/fhlmc/MonthlyLoanProcess.sh $as_of_date
          	monthly_return_code=$?
            if [[ $monthly_return_code -lt 10 ]]
          	then     	
	          	echo "sh $APP/yb-apache-hbase/src/main/scripts/fhlmc/Impala_fhlmc_monthly_export.sh $as_of_date $monthly_return_code"
	          	sh -x $APP/yb-apache-hbase/src/main/scripts/fhlmc/Impala_fhlmc_monthly_export.sh $as_of_date $monthly_return_code
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
			     echo "sh $APP/yb-apache-hbase/src/main/scripts/fhlmc/DailyLoanProcess.sh $day1Int UpdateHBaseOnly"
			     sh $APP/yb-apache-hbase/src/main/scripts/fhlmc/DailyLoanProcess.sh $day1Int UpdateHBaseOnly
			  fi
			  d1=$(date -I -d "$d1 + 1 day")
        	  d1=$(echo $d1 | tr -d "-")       	
          	done
          fi	
        fi
        d=$(date -I -d "$d + 1 day")
        d=$(echo $d | tr -d "-")
done
