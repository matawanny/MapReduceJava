#!/bin/bash

START=$(date +%s)
currentUser=$(whoami)
if [ "$currentUser" != "oozie" ]
then
    echo "Must login in as oozie"
    exit 10;
fi	

AS_OF_DATE=$1

UPDATE_HBASE_ONLY=$2
if [ -z "$AS_OF_DATE" ]
then
      echo "Must input as of date!"
      exit 11;
fi

if [ -z "$BASE" ]
then
  source $OOZIE_HOME/SetupEnv.sh
fi

AS_OF_DATE=$(echo $AS_OF_DATE | tr -d "-")
echo $AS_OF_DATE
effectiveDate=${AS_OF_DATE:0:6}
effectiveDate+="01"
echo $effectiveDate

if [ ! -d "$EMBS/fnma/$AS_OF_DATE" ]
then
  echo "$T Create a folder $EMBS/fnma/$AS_OF_DATE"
  mkdir $EMBS/fnma/$AS_OF_DATE
fi
cd $EMBS/fnma/$AS_OF_DATE

if [ ! -f "$EMBS/fnma/$AS_OF_DATE/FNMDLYLL.TXT" ]
then
	if [ ! -f "$EMBS/fnma/$AS_OF_DATE/FNMDLYLL.ZIP" ]
	then
		if [ ! -f "/net/ybr-prodnfs11/vendordata-PROD/data-grp15/embsdata/embs/daily/$AS_OF_DATE/Products/FNMDLYLL.ZIP" ]
		then
			echo 'There is no daily files FNMDLYLL.ZIP under /net/ybr-prodnfs11/vendordata-PROD/data-grp15/embsdata/embs/daily/$AS_OF_DATE/Products/'
	  		exit 12
		else  
			echo "copy source file FNMDLYLL.ZIP"
			cp /net/ybr-prodnfs11/vendordata-PROD/data-grp15/embsdata/embs/daily/$AS_OF_DATE/Products/FNMDLYLL.ZIP .
		fi
	fi
	unzip -o FNMDLYLL.ZIP
fi

DAILY_LOAN_PROCESSED=$(expr 0)
newFileName="fnma_embs_daily.csv"
FILEZIZE=$(stat -c%s $newFileName)
ls $newFileName
if [[ $? -ne 0 || $FILEZIZE -eq 0 ]]
then
	theFile="FNMDLYLL.TXT"
	#dos2unix $theFile
	rm -rf $newFileName
	sed -i 's/ *| */|/g' $theFile
	java -Xms1024m -Xmx2048m -jar $BIGDATA_JAR -t fnma_loan -i $theFile -o $newFileName -d $AS_OF_DATE
fi

FILEZIZE=$(stat -c%s $newFileName)
echo "Process $newFileName"

FILELINES=$(wc -l $newFileName | awk  '{print $1;}') 
rm -rf fnma_loan_daily.csv fnma_arm_loan_daily.csv fnma_mod_loan_daily.csv

java -Xms1024m -Xmx2048m -cp $HBASEJAVA_JAR com.yieldbook.mortgage.hbase.process.DailyProcess -t fnma_loan -i $newFileName -d $AS_OF_DATE

if [ -z "$UPDATE_HBASE_ONLY" ]
then
	echo "kite-dataset csv-import"
	hadoop fs -ls /user/hive/warehouse/prd1.db/fnma_loan_daily/as_of_date_copy=$AS_OF_DATE
	if [[ $? -eq 0 ]]
	then
		hadoop fs -rm -r /user/hive/warehouse/prd1.db/fnma_loan_daily/as_of_date_copy=$AS_OF_DATE
	fi
	ls fnma_loan_daily.csv
	if [[ $? -eq 0 ]]
	then	
		let DAILY_LOAN_PROCESSED=DAILY_LOAN_PROCESSED+1
		kite-dataset csv-import fnma_loan_daily.csv dataset:hive://$HIVE:9083/prd1/fnma_loan_daily --delimiter '|' --no-header
	fi
	
	hadoop fs -ls /user/hive/warehouse/prd1.db/fnma_arm_loan_daily/as_of_date_copy=$AS_OF_DATE
	if [[ $? -eq 0 ]]
	then
		hadoop fs -rm -r /user/hive/warehouse/prd1.db/fnma_arm_loan_daily/as_of_date_copy=$AS_OF_DATE
	fi
	ls fnma_arm_loan_daily.csv
	if [[ $? -eq 0 ]]
	then	
		let DAILY_LOAN_PROCESSED=DAILY_LOAN_PROCESSED+2
		kite-dataset csv-import fnma_arm_loan_daily.csv dataset:hive://$HIVE:9083/prd1/fnma_arm_loan_daily --delimiter '|' --no-header
	fi
	
	hadoop fs -ls /user/hive/warehouse/prd1.db/fnma_mod_loan_daily/as_of_date_copy=$AS_OF_DATE
	if [[ $? -eq 0 ]]
	then
		hadoop fs -rm -r /user/hive/warehouse/prd1.db/fnma_mod_loan_daily/as_of_date_copy=$AS_OF_DATE
	fi
	ls fnma_mod_loan_daily.csv
	if [[ $? -eq 0 ]]
	then	
		let DAILY_LOAN_PROCESSED=DAILY_LOAN_PROCESSED+4
		kite-dataset csv-import fnma_mod_loan_daily.csv dataset:hive://$HIVE:9083/prd1/fnma_mod_loan_daily --delimiter '|' --no-header
    fi		
fi

echo "Clean up local files:"
#rm -rf FHLDLYLL.TXT FHLDLYL*.dat *_daily.csv
#rm -rf FHLDLYLL.ZIP FHLDLYLL.SIG

END=$(date +%s)
DIFF=$(echo "$END - $START" | bc)

echo "The Daily ETL process START=$START PROCESS_TIME=$DIFF(s) FILEZIZE=$FILEZIZE FILELINES=$FILELINES"
echo "DAILY_LOAN_PROCESSED=$DAILY_LOAN_PROCESSED"
exit $DAILY_LOAN_PROCESSED
