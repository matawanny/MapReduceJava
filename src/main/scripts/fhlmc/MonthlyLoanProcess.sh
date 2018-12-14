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
if [ -z "$AS_OF_DATE" ]
then
      echo "$T Must input as of date!"
      exit 11
fi

if [ ! -d "$EMBS/fhlmc/$AS_OF_DATE" ]
then
  echo "$T Create a folder $EMBS/fhlmc/$AS_OF_DATE"
  mkdir $EMBS/fhlmc/$AS_OF_DATE
fi
cd $EMBS/fhlmc/$AS_OF_DATE

FHLMONLA_MISSING=0
FHLMONLF_MISSING=0
if [ ! -f "$EMBS/fhlmc/$AS_OF_DATE/FHLMONLA.TXT" ]
then
    if [ ! -f "$EMBS/fhlmc/$AS_OF_DATE/FHLMONLA.ZIP" ]
    then
		if [ ! -f "/net/ybr-prodnfs11/vendordata-PROD/data-grp15/embsdata/embs/daily/$AS_OF_DATE/Products/FHLMONLA.ZIP" ]
		then 
			echo "$T There is no monthly files FHLMONLA.ZIP under /net/ybr-prodnfs11/vendordata-PROD/data-grp15/embsdata/embs/daily/$AS_OF_DATE/Products/"
	  		FHLMONLA_MISSING=1
		else
			echo "$T copy source file FHLMONLA.ZIP"	
			cp /net/ybr-prodnfs11/vendordata-PROD/data-grp15/embsdata/embs/daily/$AS_OF_DATE/Products/FHLMONLA.ZIP .
		fi
	fi
    if [ -f "$EMBS/fhlmc/$AS_OF_DATE/FHLMONLA.ZIP" ]
    then	
		unzip -o FHLMONLA.ZIP
	fi
fi

if [ ! -f "$EMBS/fhlmc/$AS_OF_DATE/FHLMONLF.TXT" ]
then
    if [ ! -f "$EMBS/fhlmc/$AS_OF_DATE/FHLMONLF.ZIP" ]
    then
		
		if [ ! -f "/net/ybr-prodnfs11/vendordata-PROD/data-grp15/embsdata/embs/daily/$AS_OF_DATE/Products/FHLMONLF.ZIP" ]
		then 
			echo "$T There is no monthly files FHLMONLF.ZIP under /net/ybr-prodnfs11/vendordata-PROD/data-grp15/embsdata/embs/daily/$AS_OF_DATE/Products/"
	  		FHLMONLF_MISSING=1
		else
			echo "$T copy source file FHLMONLF.ZIP"	
			cp /net/ybr-prodnfs11/vendordata-PROD/data-grp15/embsdata/embs/daily/$AS_OF_DATE/Products/FHLMONLF.ZIP .
		fi
	fi
	unzip -o FHLMONLF.ZIP
fi

if [[ "$FHLMONLA_MISSING" -eq "1" && "$FHLMONLF_MISSING" -eq "1" ]]
then
  echo "No FHLMONLA.ZIP neither FHLMONLF.ZIP exists."
  exit 12
fi

hdfs_folder_check() {
	local hdfsFolder="/user/oozie/"
	hdfsFolder+=$1
	hadoop fs -ls $hdfsFolder
	if [[ $? -ne 0 ]]
	then
		echo "$T Create HDFS folder $hdfsFolder"
		hadoop fs -mkdir $hdfsFolder
	fi	
}

hdfs_folder_check "source"
hdfs_folder_check "source/fhlmc"
hdfs_folder_check "source/fhlmc/output_loan"
hdfs_folder_check "source/fhlmc/output_arm_loan"
hdfs_folder_check "source/fhlmc/output_mod_loan"

FILESIZE1=$(stat -c%s FHLMONLA.TXT)
FILESIZE2=$(stat -c%s FHLMONLF.TXT)
let FILEZIZE=$FILESIZE1+FILESIZE2

hadoop fs -rm -r /user/oozie/source/fhlmc/output_loan
hadoop fs -rm -r /user/oozie/source/fhlmc/output_arm_loan
hadoop fs -rm -r /user/oozie/source/fhlmc/output_mod_loan

filename="FHLMONLA.TXT"
echo 'Process $filename'
FILELINES=$(wc -l $filename | awk  '{print $1;}') 

echo "remove header if it has"
firstWord=$(head -n  1 $filename|cut -f 1 -d '|')
if [ "$firstWord" = "Loan Identifier" ]
then
   sed -i 1d $filename
fi

newFileName=$(echo $filename|cut -f 1 -d '.')
newFileName+="_TS.dat"

if [ -f $newFileName ]
then
        echo 'rm -rf $newFileName'
        rm -rf $newFileName
fi

echo $AS_OF_DATE
asOfDateMonthly=${AS_OF_DATE:0:6}
asOfDateMonthly+="00"
echo $asOfDateMonthly
effectiveDate=${AS_OF_DATE:0:6}
effectiveDate+="01"
echo $effectiveDate

sed  "s/$/\|$effectiveDate\|$asOfDateMonthly/g" $filename >>$newFileName

filename2="FHLMONLF.TXT"
echo 'Process $filename2'

FILELINES2=$(wc -l $filename2 | awk  '{print $1;}') 

echo "$T remove header if it has"
firstWord=$(head -n  1 $filename2|cut -f 1 -d '|')
if [ "$firstWord" = "Loan Identifier" ]
then
   sed -i 1d $filename2
fi

newFileName2=$(echo $filename2|cut -f 1 -d '.')
newFileName2+="_TS.dat"

if [ -f $newFileName2 ]
then
        echo 'rm -rf $newFileName2'
        rm -rf $newFileName2
fi

sed  "s/$/\|$effectiveDate\|$asOfDateMonthly/g" $filename2 >>$newFileName2

newFileName3="FHLMONLAF_TS.dat"
if [ -f $newFileName3 ]
then
        echo 'rm -rf $newFileName3'
        rm -rf $newFileName3
fi

cat $newFileName $newFileName2 > $newFileName3

#sed -i 's/ *| */|/g' $filename

rm -rf fhlmc*monthly.csv
java -Xms1024m -Xmx2048m -cp $HBASEJAVA_JAR com.yieldbook.mortgage.hbase.process.MonthlyProcess -t fhlmc -i $newFileName3 -d $asOfDateMonthly

echo "$T kite-dataset csv-import"
MONTHLY_LOAN_PROCESSED=$(expr 0)
CSVFILE="fhlmc_loan_monthly.csv"
CSVLINES=$(wc -l $CSVFILE | awk  '{print $1;}')
if [ $CSVLINES -lt 1 ]
then
   rm -rf $CSVFILE
else 
	let MONTHLY_LOAN_PROCESSED=MONTHLY_LOAN_PROCESSED+1
	hadoop fs -ls /user/hive/warehouse/prd1.db/fhlmc_loan_daily/as_of_date_copy=$asOfDateMonthly
	if [[ $? -eq 0 ]]
	then
		hadoop fs -rm -r /user/hive/warehouse/prd1.db/fhlmc_loan_daily/as_of_date_copy=$asOfDateMonthly
	fi
	kite-dataset csv-import $CSVFILE dataset:hive://$HIVE:9083/prd1/fhlmc_loan_daily --delimiter '|' --no-header
fi

CSVFILE="fhlmc_arm_loan_monthly.csv"
CSVLINES=$(wc -l $CSVFILE | awk  '{print $1;}')
if [ $CSVLINES -lt 1 ]
then
   rm -rf $CSVFILE
else
	let MONTHLY_LOAN_PROCESSED=MONTHLY_LOAN_PROCESSED+2
	hadoop fs -ls /user/hive/warehouse/prd1.db/fhlmc_arm_loan_daily/as_of_date_copy=$asOfDateMonthly
	if [[ $? -eq 0 ]]
	then
		hadoop fs -rm -r /user/hive/warehouse/prd1.db/fhlmc_arm_loan_daily/as_of_date_copy=$asOfDateMonthly
	fi
	kite-dataset csv-import $CSVFILE dataset:hive://$HIVE:9083/prd1/fhlmc_arm_loan_daily --delimiter '|' --no-header
fi

CSVFILE="fhlmc_mod_loan_monthly.csv"
CSVLINES=$(wc -l $CSVFILE | awk  '{print $1;}')
if [ $CSVLINES -lt 1 ]
then
   rm -rf $CSVFILE
else
	let MONTHLY_LOAN_PROCESSED=MONTHLY_LOAN_PROCESSED+4
	hadoop fs -ls /user/hive/warehouse/prd1.db/fhlmc_mod_loan_daily/as_of_date_copy=$asOfDateMonthly
	if [[ $? -eq 0 ]]
	then
		hadoop fs -rm -r /user/hive/warehouse/prd1.db/fhlmc_mod_loan_daily/as_of_date_copy=$asOfDateMonthly
	fi
	kite-dataset csv-import $CSVFILE dataset:hive://$HIVE:9083/prd1/fhlmc_mod_loan_daily --delimiter '|' --no-header
fi

echo "$T Staring ETL fhlmc_loan:"
hadoop fs -rm /user/oozie/source/fhlmc/$newFileName3
hadoop fs -copyFromLocal $newFileName3 /user/oozie/source/fhlmc

inputPath='/user/oozie/source/fhlmc/'
inputPath+=$newFileName3
outputPath='/user/oozie/source/fhlmc/output_loan'
table='fhlmc_loan_monthly'

hadoop jar $HBASEJAVA_JAR com.yieldbook.mortgage.hbase.bulkimport.FhlmcLoanDriver $inputPath $outputPath $table

echo "$T String ETL fhlmc_mod_loan:"

outputPath='/user/oozie/source/fhlmc/output_mod_loan'
table='fhlmc_mod_loan_monthly'

hadoop jar $HBASEJAVA_JAR com.yieldbook.mortgage.hbase.bulkimport.FhlmcModLoanDriver $inputPath $outputPath $table

echo "$T String ETL fhlmc_arm_loan:"

outputPath='/user/oozie/source/fhlmc/output_arm_loan'
table='fhlmc_arm_loan_monthly'

hadoop jar $HBASEJAVA_JAR com.yieldbook.mortgage.hbase.bulkimport.FhlmcArmLoanDriver $inputPath $outputPath $table

echo "$T Clean up HDFS input file:"
hadoop fs -rm $inputPath


echo "$T Clean up local files:"
#rm -rf FHLMONL?.TXT  
#rm -rf FHLMONL?.ZIP FHLMONL?.SIG

END=$(date +%s)
DIFF=$(echo "$END - $START" | bc)

echo "$T The ETL process START=$START	DIFF=$DIFF FILEZIZE=$FILEZIZE"
echo "MONTHLY_LOAN_PROCESSED=$MONTHLY_LOAN_PROCESSED"
exit $MONTHLY_LOAN_PROCESSED

