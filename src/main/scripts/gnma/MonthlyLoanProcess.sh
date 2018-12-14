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
	

AS_OF_DATE=$1
lastChgDate=$1
if [ -z "$AS_OF_DATE" ]
then
      echo "$T Must input as of date!"
      exit 11
fi

targetFile=$2
if [ -z "$targetFile" ]
then
  echo "$T Must input Target File: GNMMNILL, GN1MONL or GN2MONL"
  exit 12
fi


dateMonthly=${AS_OF_DATE:0:6}
dateMonthly+="00"
echo $T $dateMonthly

dateMonthly1=${AS_OF_DATE:0:6}
dateMonthly1+="001"
echo $T $dateMonthly1

if [ ! -d "$EMBS/gnma/$AS_OF_DATE" ]
then
  echo "$T Create a folder $EMBS/gnma/$AS_OF_DATE"
  mkdir $EMBS/gnma/$AS_OF_DATE
fi
cd $EMBS/gnma/$AS_OF_DATE

batch_process(){
	local txtFileName=$1
	txtFileName+=".TXT"
	local zipFileName=$1
	zipFileName+=".ZIP"
	local newFileName=$1
	newFileName+=".dat"
	local asOfDateMonthly=$2
	local deleteFolder=$3
	echo $T $deleteFolder

	local loanFileName=$1
	loanFileName+="_loan.csv"
	local armLoanFileName=$1
	armLoanFileName+="_arm_loan.csv"
	cd $EMBS/gnma/$AS_OF_DATE
	rm -rf $loanFileName $armLoanFileName
			
	if [ ! -f "/net/ybr-prodnfs11/vendordata-PROD/data-grp15/embsdata/embs/daily/$AS_OF_DATE/Products/$zipFileName" ]
	then
		echo "$T There is no monthly files $zipFileName under /net/ybr-prodnfs11/vendordata-PROD/data-grp15/embsdata/embs/daily/$AS_OF_DATE/Products/"
  		return 20
	else
		echo "$T copy source file $zipFileName"  
		cp /net/ybr-prodnfs11/vendordata-PROD/data-grp15/embsdata/embs/daily/$AS_OF_DATE/Products/$zipFileName .
	fi
	unzip -o $zipFileName
	java -Xms1024m -Xmx2048m -jar $BIGDATA_JAR -t gnma_loan -i $txtFileName -o $newFileName

	local FILEZIZE=$(stat -c%s $newFileName)	
	echo "$T Process $newFileName"
	
	FILELINES=$(wc -l $newFileName | awk  '{print $1;}') 
	
	java -Xms1024m -Xmx2048m -cp $HBASEJAVA_JAR com.yieldbook.mortgage.hbase.process.MonthlyProcess -t gnma -i $newFileName -d $asOfDateMonthly -l $lastChgDate
	
	echo "$T kite-dataset csv-import"
	MONTHLY_LOAN_PROCESSED=$(expr 0)
	ls $loanFileName
	if [[ $? -eq 0 ]]
	then
       if [ $deleteFolder = 'true' ]
       then  	
			hadoop fs -ls /user/hive/warehouse/prd1.db/gnma_loan_daily/as_of_date_copy=$asOfDateMonthly
			if [[ $? -eq 0 ]]
			then
				hadoop fs -rm -r /user/hive/warehouse/prd1.db/gnma_loan_daily/as_of_date_copy=$asOfDateMonthly
			fi	
		fi
		let MONTHLY_LOAN_PROCESSED=MONTHLY_LOAN_PROCESSED+1
		kite-dataset csv-import $loanFileName dataset:hive://$HIVE:9083/prd1/gnma_loan_daily --delimiter '|' --no-header
	fi

	ls $armLoanFileName
	if [[ $? -eq 0 ]]
	then
       if [ "$deleteFolder" = "true" ]
       then  	
			hadoop fs -ls /user/hive/warehouse/prd1.db/gnma_arm_loan_daily/as_of_date_copy=$asOfDateMonthly
			if [[ $? -eq 0 ]]
			then
				hadoop fs -rm -r /user/hive/warehouse/prd1.db/gnma_arm_loan_daily/as_of_date_copy=$asOfDateMonthly
			fi	
		fi
		let MONTHLY_LOAN_PROCESSED=MONTHLY_LOAN_PROCESSED+2	
		kite-dataset csv-import $armLoanFileName dataset:hive://$HIVE:9083/prd1/gnma_arm_loan_daily --delimiter '|' --no-header
	fi
	
	echo "$T Clean up local files:"
	rm -rf $txtFileName $newFileName $zipFileName
	# rm -rf $loanFileName $armLoanFileName

	END=$(date +%s)
	local DIFF=$(echo "$END - $START" | bc)
	
	echo "$T \nThe Daily ETL process START=$START PROCESS_TIME=$DIFF(s) FILEZIZE=$FILEZIZE FILELINES=$FILELINES\n"
	echo $MONTHLY_LOAN_PROCESSED
	return $MONTHLY_LOAN_PROCESSED
}


if [ "$targetFile" = "GNMMNILL" ]
then
	batch_process $targetFile $dateMonthly true
	exit_code=$?
elif  [ "$targetFile" = "GN1MONLL" ]
then
	batch_process $targetFile $dateMonthly1 true
	exit_code=$?
elif [ "$targetFile" = "GN2MONLL" ]
then
	batch_process $targetFile $dateMonthly1 false
	exit_code=$?
else
   echo "$T $targetFile does not support!"
   exit 13
fi   	

echo "exit_code=$exit_code"	
exit $exit_code

