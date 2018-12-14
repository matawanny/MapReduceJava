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

echo $T $AS_OF_DATE
asOfDateMonthly=${AS_OF_DATE:0:6}
asOfDateMonthly+="00"
echo $T  $asOfDateMonthly

if [ ! -d "$EMBS/fnma/$AS_OF_DATE" ]
then
  echo "$T Create a folder $EMBS/fnma/$AS_OF_DATE"
  mkdir $EMBS/fnma/$AS_OF_DATE
fi
cd $EMBS/fnma/$AS_OF_DATE

ls $EMBS/fnma/$AS_OF_DATE/FNMMONLL*.dat
if [[ $? -ne 0 ]]
then
    if [ ! -f " $EMBS/fnma/$AS_OF_DATE/FNMMONLL.ZIP" ]
    then
		
		if [ ! -f "/net/ybr-prodnfs11/vendordata-PROD/data-grp15/embsdata/embs/daily/$AS_OF_DATE/Products/FNMMONLL.ZIP" ]
		then 
			echo "$T There is no monthly files FNMMONLL.ZIP under /net/ybr-prodnfs11/vendordata-PROD/data-grp15/embsdata/embs/daily/$AS_OF_DATE/Products/"
	  		exit 12
		else
			echo "$T copy source file FNMMONLL.ZIP"	
			cp /net/ybr-prodnfs11/vendordata-PROD/data-grp15/embsdata/embs/daily/$AS_OF_DATE/Products/FNMMONLL.ZIP .
		fi
	fi
	unzip -o FNMMONLL.ZIP
fi

newFileName="fnma_embs_all_monthly.csv"
if [ ! -f "$EMBS/fnma/$AS_OF_DATE/$newFileName" ]
then
	i=1
	FILES="$(zipinfo FNMMONLL.ZIP | grep dat | awk '{print $9}')"
	for zf in $FILES ; do
			echo "$T $zf"
			fileName="fnma_embs_monthly_"
			fileName+=$i
			fileName+=".csv"
			if [ -f $fileName ]
			then
		        echo "$T rm -rf $fileName"
		        rm -rf $fileName
			fi
			#dos2unix $zf
			sed -i 's/ *| */|/g' $zf
			rm -rf $fileName
			java -Xms1024m -Xmx2048m -jar $BIGDATA_JAR -t fnma_loan -i $zf -o $fileName -d $AS_OF_DATE
		    let i=i+1
	done
	cat fnma_embs_monthly_*.csv > $newFileName
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
hdfs_folder_check "source/fnma"
hdfs_folder_check "source/fnma/output_loan"
hdfs_folder_check "source/fnma/output_arm_loan"
hdfs_folder_check "source/fnma/output_mod_loan"
hdfs_folder_check "fnma_loan_monthly"

FILEZIZE=$(stat -c%s $newFileName)
hadoop fs -rm -r /user/oozie/source/fnma/output_loan
hadoop fs -rm -r /user/oozie/source/fnma/output_arm_loan
hadoop fs -rm -r /user/oozie/source/fnma/output_mod_loan

echo "$T Process $newFileName"
FILELINES=$(wc -l $newFileName | awk  '{print $1;}') 

echo "$T Remove header if it has"
firstWord=$(head -n  1 $newFileName|cut -f 1 -d '|')
if [ "$firstWord" = "Loan Identifier" ]
then
   sed -i 1d $newFileName
fi

echo "$T Staring ETL fnma_loan:"
hadoop fs -rm /user/oozie/source/$newFileName
hadoop fs -copyFromLocal $newFileName /user/oozie/source/fnma/

inputPath='/user/oozie/source/fnma/'
inputPath+=$newFileName
outputPath='/user/oozie/source/fnma/output_loan'
table='fnma_loan_monthly'

hadoop jar $HBASEJAVA_JAR com.yieldbook.mortgage.hbase.bulkimport.FnmaLoanDriver $inputPath $outputPath $table $AS_OF_DATE

echo "$T String ETL fnma_mod_loan:"

outputPath='/user/oozie/source/fnma/output_mod_loan'
table='fnma_mod_loan_monthly'

hadoop jar $HBASEJAVA_JAR com.yieldbook.mortgage.hbase.bulkimport.FnmaModLoanDriver $inputPath $outputPath $table $AS_OF_DATE

echo "$T String ETL fnma_arm_loan:"

outputPath='/user/oozie/source/fnma/output_arm_loan'
table='fnma_arm_loan_monthly'

hadoop jar $HBASEJAVA_JAR com.yieldbook.mortgage.hbase.bulkimport.FnmaArmLoanDriver $inputPath $outputPath $table $AS_OF_DATE

echo "$T Clean up HDFS input file:"
hadoop fs -rm $inputPath

java -Xms1024m -Xmx2048m -cp $HBASEJAVA_JAR com.yieldbook.mortgage.hbase.process.MonthlyProcess -t fnma -i $newFileName -d $asOfDateMonthly

echo "$T kite-dataset csv-import"
MONTHLY_LOAN_PROCESSED=$(expr 0)
hadoop fs -ls /user/hive/warehouse/prd1.db/fnma_loan_daily/as_of_date_copy=$asOfDateMonthly
if [[ $? -eq 0 ]]
then
	hadoop fs -rm -r /user/hive/warehouse/prd1.db/fnma_loan_daily/as_of_date_copy=$asOfDateMonthly
fi
loanMonthlyFile="fnma_loan_monthly.csv"
loan_num=$(wc -l $loanMonthlyFile | awk  '{print $1;}')
echo "$T fnma_loan_monthly.csv has $loan_num records."
let loan_1_start=1
let loan_1_end=loan_num/5
let loan_2_start=loan_1_end+1
let loan_2_end=loan_num/5*2
let loan_3_start=loan_2_end+1
let loan_3_end=loan_num/5*3
let loan_4_start=loan_3_end+1
let loan_4_end=loan_num/5*4
let loan_5_start=loan_4_end+1
let loan_5_end=loan_num
sed -n "${loan_1_start},${loan_1_end}p" $loanMonthlyFile > fnma_loan_monthly_1.csv
sed -n "${loan_2_start},${loan_2_end}p" $loanMonthlyFile > fnma_loan_monthly_2.csv
sed -n "${loan_3_start},${loan_3_end}p" $loanMonthlyFile > fnma_loan_monthly_3.csv
sed -n "${loan_4_start},${loan_4_end}p" $loanMonthlyFile > fnma_loan_monthly_4.csv
sed -n "${loan_5_start},${loan_5_end}p" $loanMonthlyFile > fnma_loan_monthly_5.csv

let MONTHLY_LOAN_PROCESSED=MONTHLY_LOAN_PROCESSED+1
hadoop fs -rm -r /user/oozie/fnma_loan_monthly/fnma_loan_monthly_*.csv
hadoop fs -copyFromLocal fnma_loan_monthly_*.csv /user/oozie/fnma_loan_monthly
kite-dataset csv-import hdfs:/user/oozie/fnma_loan_monthly dataset:hive://$HIVE:9083/prd1/fnma_loan_daily --delimiter '|' --no-header

if [ -f "$EMBS/fnma/$AS_OF_DATE/fnma_arm_loan_monthly.csv" ]
then
	let MONTHLY_LOAN_PROCESSED=MONTHLY_LOAN_PROCESSED+2
	hadoop fs -ls /user/hive/warehouse/prd1.db/fnma_arm_loan_daily/as_of_date_copy=$asOfDateMonthly
	if [[ $? -eq 0 ]]
	then
		hadoop fs -rm -r /user/hive/warehouse/prd1.db/fnma_arm_loan_daily/as_of_date_copy=$asOfDateMonthly
	fi
	kite-dataset csv-import fnma_arm_loan_monthly.csv dataset:hive://$HIVE:9083/prd1/fnma_arm_loan_daily --delimiter '|' --no-header
fi

if [ -f "$EMBS/fnma/$AS_OF_DATE/fnma_mod_loan_monthly.csv" ]
then
	let MONTHLY_LOAN_PROCESSED=MONTHLY_LOAN_PROCESSED+4
	hadoop fs -ls /user/hive/warehouse/prd1.db/fnma_mod_loan_daily/as_of_date_copy=$asOfDateMonthly
	if [[ $? -eq 0 ]]
	then
		hadoop fs -rm -r /user/hive/warehouse/prd1.db/fnma_mod_loan_daily/as_of_date_copy=$asOfDateMonthly
	fi
	kite-dataset csv-import fnma_mod_loan_monthly.csv dataset:hive://$HIVE:9083/prd1/fnma_mod_loan_daily --delimiter '|' --no-header
fi

echo "$T Clean up local files:"
#rm -rf FNMMONLL.TXT  *_monthly.csv *_monthly_*.csv
#rm -rf FHLMONL?.ZIP FHLMONL?.SIG
#rm -rf FNMMONLL.dat FNMMONLL*.dat

END=$(date +%s)
DIFF=$(echo "$END - $START" | bc)

echo "$T The ETL process START=$START	DIFF=$DIFF FILEZIZE=$FILEZIZE"
echo "MONTHLY_LOAN_PROCESSED=$MONTHLY_LOAN_PROCESSED"
exit $MONTHLY_LOAN_PROCESSED

