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
AS_OF_DATE=$(echo $AS_OF_DATE | tr -d "-")
UPDATE_HBASE_ONLY=$2
if [ -z "$AS_OF_DATE" ]
then
      echo "Must input as of date!"
      exit 11
fi

if [ ! -d "$EMBS/gnma/$AS_OF_DATE" ]
then
  echo "$T Create a folder $EMBS/gnma/$AS_OF_DATE"
  mkdir $EMBS/gnma/$AS_OF_DATE
fi
cd $EMBS/gnma/$AS_OF_DATE


if [ ! -f "$EMBS/gnma/$AS_OF_DATE/GNMDLYLL.TXT" ]
then
	if [ ! -f "$EMBS/gnma/$AS_OF_DATE/GNMDLYLL.ZIP" ]
	then
		if [ ! -f "/net/ybr-prodnfs11/vendordata-PROD/data-grp15/embsdata/embs/daily/$AS_OF_DATE/Products/GNMDLYLL.ZIP" ]
		then
			echo 'There is no daily files GNMDLYLL.ZIP under /net/ybr-prodnfs11/vendordata-PROD/data-grp15/embsdata/embs/daily/$AS_OF_DATE/Products/'
	  		exit 12
		else
			echo "copy source file GNMDLYLL.ZIP"  
			cp /net/ybr-prodnfs11/vendordata-PROD/data-grp15/embsdata/embs/daily/$AS_OF_DATE/Products/GNMDLYLL.ZIP .
		fi
	fi
	unzip -o GNMDLYLL.ZIP
fi

DAILY_LOAN_PROCESSED=$(expr 0)
newFileName="gnma_embs_daily.dat"
FILEZIZE=$(stat -c%s $newFileName)
ls $newFileName

if [[ $? -ne 0 || $FILEZIZE -eq 0 ]]
then
	theFile="GNMDLYLL.TXT"
	rm -rf $newFileName
	java -Xms1024m -Xmx2048m -jar $BIGDATA_JAR -t gnma_loan -i $theFile -o $newFileName
fi

FILEZIZE=$(stat -c%s $newFileName)
echo "Process $newFileName"

FILELINES=$(wc -l $newFileName | awk  '{print $1;}') 
rm -rf gnma_loan_daily.csv gnma_arm_loan_daily.csv

java -Xms1024m -Xmx2048m -cp $HBASEJAVA_JAR com.yieldbook.mortgage.hbase.process.DailyProcess -t gnma_loan -i $newFileName -d $AS_OF_DATE -l $lastChgDate

if [ -z "$UPDATE_HBASE_ONLY" ]
then
	echo "kite-dataset csv-import"
	hadoop fs -ls /user/hive/warehouse/prd1.db/gnma_loan_daily/as_of_date_copy=$AS_OF_DATE
	if [[ $? -eq 0 ]]
	then
		hadoop fs -rm -r /user/hive/warehouse/prd1.db/gnma_loan_daily/as_of_date_copy=$AS_OF_DATE
	fi
	ls gnma_loan_daily.csv
	if [[ $? -eq 0 ]]
	then
	    let DAILY_LOAN_PROCESSED=DAILY_LOAN_PROCESSED+1	
		kite-dataset csv-import gnma_loan_daily.csv dataset:hive://$HIVE:9083/prd1/gnma_loan_daily --delimiter '|' --no-header
	fi
	
	hadoop fs -ls /user/hive/warehouse/prd1.db/gnma_arm_loan_daily/as_of_date_copy=$AS_OF_DATE
	if [[ $? -eq 0 ]]
	then
		hadoop fs -rm -r /user/hive/warehouse/prd1.db/gnma_arm_loan_daily/as_of_date_copy=$AS_OF_DATE
	fi
	ls gnma_arm_loan_daily.csv
	if [[ $? -eq 0 ]]
	then	
		let DAILY_LOAN_PROCESSED=DAILY_LOAN_PROCESSED+2
		kite-dataset csv-import gnma_arm_loan_daily.csv dataset:hive://$HIVE:9083/prd1/gnma_arm_loan_daily --delimiter '|' --no-header
	fi
fi

echo "Clean up local files:"
# rm -rf GNMDLYLL.TXT GNMDLYLL*.dat *_daily.csv
# rm -rf GNMDLYLL.ZIP 
END=$(date +%s)
DIFF=$(echo "$END - $START" | bc)

echo "The Daily ETL process START=$START PROCESS_TIME=$DIFF(s) FILEZIZE=$FILEZIZE FILELINES=$FILELINES"
echo "DAILY_LOAN_PROCESSED=$DAILY_LOAN_PROCESSED"
exit $DAILY_LOAN_PROCESSED
