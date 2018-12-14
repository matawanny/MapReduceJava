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
START=$1

AS_OF_DATE=$1
if [ -z "$AS_OF_DATE" ]
then
      echo "Must input as of date!"
      exit 11
fi

if [ ! -d "$EMBS/fnma/$AS_OF_DATE" ]
then
  echo "$T Create a folder $EMBS/fnma/$AS_OF_DATE"
  mkdir $EMBS/fnma/$AS_OF_DATE
fi
cd $EMBS/fnma/$AS_OF_DATE


if [ ! -f "$EMBS/fnma/$AS_OF_DATE/FNMA_key_all_sort.txt" ]
then
	if [ ! -f "$EMBS/fnma/$AS_OF_DATE/FNMMONLL.ZIP" ]
	then
		echo copy source file FNMMONLL.ZIP
		cp /net/ybr-prodnfs11/vendordata-PROD/data-grp15/embsdata/embs/daily/$AS_OF_DATE/Products/FNMMONLL.ZIP .
	fi
	
	if [ ! -f "$EMBS/fnma/$AS_OF_DATE/FNMMONLL.ZIP" ]
	then
		echo 'There is no monthly files under /net/ybr-prodnfs11/vendordata-PROD/data-grp15/embsdata/embs/daily/$AS_OF_DATE/Products/'
		exit 12
	fi
	unzip -o FNMMONLL.ZIP
	
	FILES="$(zipinfo FNMMONLL.ZIP | grep dat | awk '{print $9}')"
	
	i=1
	ls FNMA_key_all_sort.txt
	if [[ $? -ne 0 ]]
	then
		for zf in $FILES ; do
			echo "$zf"
			newFileName="fnma_embs_monthly_"
			newFileName+=$i
			newFileName+=".csv"
			if [ -f $newFileName ]
			then
			        echo 'rm -rf $newFileName'
			        rm -rf $newFileName
			fi
			java -Xms1024m -Xmx2048m -jar $BIGDATA_JAR -t fnma_loan -i $zf -o $newFileName -d $AS_OF_DATE
						
			newKeyFileName="FNMA_key_"
			newKeyFileName+=$i
			newKeyFileName+=".txt"
			awk -F'|' '{print $7}' $newFileName > $newKeyFileName
		    let i=i+1
		done

		cat FNMA_key_?.txt > FNMA_key_all.txt
		sort FNMA_key_all.txt > FNMA_key_all_sort.txt
	fi
fi	

FNMA_key_num=$(wc -l FNMA_key_all_sort.txt | awk  '{print $1;}')
echo "table fnma_loan"
let FNMA_key_1=FNMA_key_num/5
let FNMA_key_2=FNMA_key_num/5*2
let FNMA_key_3=FNMA_key_num/5*3
let FNMA_key_4=FNMA_key_num/5*4
key1=$(sed -n "${FNMA_key_1}p"<FNMA_key_all_sort.txt)
key2=$(sed -n "${FNMA_key_2}p"<FNMA_key_all_sort.txt)
key3=$(sed -n "${FNMA_key_3}p"<FNMA_key_all_sort.txt)
key4=$(sed -n "${FNMA_key_4}p"<FNMA_key_all_sort.txt)
key5=$(sed -n "${FNMA_key_num}p"<FNMA_key_all_sort.txt)

java -Xms1024m -Xmx2048m -cp $HBASEJAVA_JAR com.yieldbook.mortgage.hbase.admin.FnmaLoanInitialize $key1 $key2 $key3 $key4 $key5

echo 'table fnma_loan_monthly, table fnma_arm_loan_monthly, table fnma_mod_loan_monthly has been created.'
#rm -rf *.csv *.txt *.dat
exit 0
