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

AS_OF_DATE=$(echo $1 | tr -d "-")
day=$1
UPDATE_HBASE_ONLY=$2
if [ -z "$AS_OF_DATE" ]
then
      echo "Must input as of date!"
      exit 11
fi

if [ ! -d "$EMBS/fhlmc/$AS_OF_DATE" ]
then
  echo "Create a folder $AS_OF_DATE"
  mkdir $EMBS/fhlmc/$AS_OF_DATE
fi

cd $EMBS/fhlmc/$AS_OF_DATE
SecDailyFileMissing=0
SecDailyFileName="FHLSECD_ALL.TXT"
SecDailyDicFileName="FHLSECD_DIC.TXT"
if [ ! -f "$EMBS/fhlmc/$AS_OF_DATE/$SecDailyDicFileName" ]
then
	ls $EMBS/fhlmc/$AS_OF_DATE/FHLSEC?D.ZIP
	if [[ $? -ne 0 ]]
	then

		ls /net/ybr-prodnfs11/vendordata-PROD/data-grp15/embsdata/embs/daily/$AS_OF_DATE/Products/FHLSEC?D.ZIP
		if [[ $? -ne 0 ]]
		then
			echo 'There is no security level file FHLSEC?D.ZIP files under /net/ybr-prodnfs11/vendordata-PROD/data-grp15/embsdata/embs/daily/$AS_OF_DATE/Products/'
	  		SecDailyFileMissing=1
		else
			echo copy source file FHLSEC?D.ZIP  
			cp /net/ybr-prodnfs11/vendordata-PROD/data-grp15/embsdata/embs/daily/$AS_OF_DATE/Products/FHLSEC?D.ZIP .
			for f in FHLSEC?D.ZIP
		    do
		       fname=${f%.*}
		       echo $fname
		       unzip -o $f
		       fname+=".TXT"
		       # dos2unix $fname
		       firstWord=$(head -n  1 $fname|cut -f 1 -d '|')
		       if [ "$firstWord" = "Prefix" ]
		       then
		         sed -i 1d $fname
		       fi
		     done
			 cat FHLSEC?D.TXT > $SecDailyFileName
			 awk -F "|" '{print $3, $4}' $SecDailyFileName > $SecDailyDicFileName			
		fi
	fi
fi

FHLDLYLF_MISSING=0
FHLDLYLA_MISSING=0

if [ ! -f "$EMBS/fhlmc/$AS_OF_DATE/FHLDLYLF.TXT" ]
then
	if [ ! -f "$EMBS/fhlmc/$AS_OF_DATE/FHLDLYLF.ZIP" ]
	then
		if [ ! -f "/net/ybr-prodnfs11/vendordata-PROD/data-grp15/embsdata/embs/daily/$AS_OF_DATE/Products/FHLDLYLF.ZIP" ]
		then
			echo 'There is no daily files FHLDLYLF.ZIP under /net/ybr-prodnfs11/vendordata-PROD/data-grp15/embsdata/embs/daily/$AS_OF_DATE/Products/'
	  		FHLDLYLF_MISSING=1
		else
			echo "copy source file FHLDLYLF.ZIP"  
			cp /net/ybr-prodnfs11/vendordata-PROD/data-grp15/embsdata/embs/daily/$AS_OF_DATE/Products/FHLDLYLF.ZIP .
			fi
	fi
	unzip -o FHLDLYLF.ZIP
fi

if [ ! -f "$EMBS/fhlmc/$AS_OF_DATE/FHLDLYLA.TXT" ]
then
	if [ ! -f "$EMBS/fhlmc/$AS_OF_DATE/FHLDLYA.ZIP.ZIP" ]	
	then
		if [ ! -f "/net/ybr-prodnfs11/vendordata-PROD/data-grp15/embsdata/embs/daily/$AS_OF_DATE/Products/FHLDLYLA.ZIP" ]
		then
			echo 'There is no daily FHLDLYA.ZIP files under /net/ybr-prodnfs11/vendordata-PROD/data-grp15/embsdata/embs/daily/$AS_OF_DATE/Products/'
	  		FHLDLYLA_MISSING=1
		else  
		    echo "copy source file FHLDLYLA.ZIP"
			cp /net/ybr-prodnfs11/vendordata-PROD/data-grp15/embsdata/embs/daily/$AS_OF_DATE/Products/FHLDLYLA.ZIP .
		fi
	fi
	if [ ! -f "$EMBS/fhlmc/$AS_OF_DATE/FHLDLYA.ZIP.ZIP" ]
	then
		unzip -o FHLDLYLA.ZIP
	fi
fi

if [[ "$FHLDLYLA_MISSING" -eq "1" && "$FHLDLYLF_MISSING" -eq "1" ]]
then
  echo "No FHLDLYA.ZIP neither FHLDLYF.ZIP exists."
  exit 13
fi

filename="FHLDLYLF.TXT"
filename2="FHLDLYLA.TXT"
filename3="FHLDLYLFA.dat"

echo "Process $filename"
DAILY_LOAN_PROCESSED=$(expr 0)
echo "remove header if it has"
firstWord=$(head -n  1 $filename|cut -f 1 -d '|')
if [ "$firstWord" = "Loan Identifier" ]
then
   sed -i 1d $filename
fi

FILELINES1=$(wc -l $filename | awk  '{print $1;}') 

name=$(echo $filename|cut -f 1 -d '.'|cut -f 2 -d '_')
prefix=$(echo $filename|cut -f 1 -d '.'|cut -f 1 -d '_')

FILESIZE1=$(stat -c%s FHLDLYLF.TXT)
FILESIZE2=$(stat -c%s FHLDLYLA.TXT)

FILELINES2=$(wc -l $filename2 | awk  '{print $1;}') 

if [[ $FILELINES2 -ne 1 ]]
then
	echo "Process $filename2"
	echo "remove header if it has"
	firstWord=$(head -n  1 $filename2|cut -f 1 -d '|')
	if [ "$firstWord" = "Loan Identifier" ]
	then
   		sed -i 1d $filename2
	fi
	
	let FILEZIZE=$FILESIZE1+$FILESIZE2
	# dos2unix $filename2
	cat $filename $filename2 > $filename3
else
	let FILEZIZE=$FILESIZE1
	filename3=$filename
fi

FILELINES=$(wc -l $filename3 | awk  '{print $1;}') 

rm -rf fhlmc_loan_daily.csv fhlmc_arm_loan_daily.csv fhlmc_mod_loan_daily.csv

if [[ "$FHLDLYLA_MISSING" -eq "0" ]]
then
	java -Xms1024m -Xmx2048m -cp $HBASEJAVA_JAR com.yieldbook.mortgage.hbase.process.DailyProcess -t fhlmc_loan -i $filename3 -p $SecDailyDicFileName -d $AS_OF_DATE
else
	java -Xms1024m -Xmx2048m -cp $HBASEJAVA_JAR com.yieldbook.mortgage.hbase.process.DailyProcess -t fhlmc_loan -i $filename3 -d $AS_OF_DATE
fi
if [ -z "$UPDATE_HBASE_ONLY" ]
then
	echo "kite-dataset csv-import"
	hadoop fs -ls /user/hive/warehouse/prd1.db/fhlmc_loan_daily/as_of_date_copy=$day
	if [[ $? -eq 0 ]]
	then
		hadoop fs -rm -r /user/hive/warehouse/prd1.db/fhlmc_loan_daily/as_of_date_copy=$day
	fi
	ls fhlmc_loan_daily.csv
	if [[ $? -eq 0 ]]
	then
		let DAILY_LOAN_PROCESSED=DAILY_LOAN_PROCESSED+1
		kite-dataset csv-import fhlmc_loan_daily.csv dataset:hive://$HIVE:9083/prd1/fhlmc_loan_daily --delimiter '|' --no-header
	fi

	hadoop fs -ls /user/hive/warehouse/prd1.db/fhlmc_arm_loan_daily/as_of_date_copy=$day
	if [[ $? -eq 0 ]]
	then
		hadoop fs -rm -r /user/hive/warehouse/prd1.db/fhlmc_arm_loan_daily/as_of_date_copy=$day
	fi
	ls fhlmc_arm_loan_daily.csv
	if [[ $? -eq 0 ]]
	then
		let DAILY_LOAN_PROCESSED=DAILY_LOAN_PROCESSED+2
		kite-dataset csv-import fhlmc_arm_loan_daily.csv dataset:hive://$HIVE:9083/prd1/fhlmc_arm_loan_daily --delimiter '|' --no-header
	fi
	
	hadoop fs -ls /user/hive/warehouse/prd1.db/fhlmc_mod_loan_daily/as_of_date_copy=$day
	if [[ $? -eq 0 ]]
	then
		hadoop fs -rm -r /user/hive/warehouse/prd1.db/fhlmc_mod_loan_daily/as_of_date_copy=$day
	fi
	ls fhlmc_mod_loan_daily.csv
	if [[ $? -eq 0 ]]
	then	
		let DAILY_LOAN_PROCESSED=DAILY_LOAN_PROCESSED+4
		kite-dataset csv-import fhlmc_mod_loan_daily.csv dataset:hive://$HIVE:9083/prd1/fhlmc_mod_loan_daily --delimiter '|' --no-header
	fi
fi

echo "Clean up local files:"
#rm -rf FHLDLYL*.TXT FHLDLYL*.dat *_daily.csv
#rm -rf FHLDLYL?.ZIP FHLDLYL?.SIG

END=$(date +%s)
DIFF=$(echo "$END - $START" | bc)

echo "The Daily ETL process START=$START PROCESS_TIME=$DIFF(s) FILEZIZE=$FILEZIZE FILELINES=$FILELINES"
echo "DAILY_LOAN_PROCESSED=$DAILY_LOAN_PROCESSED"
exit $DAILY_LOAN_PROCESSED
