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
      echo "Must input as of date!"
      exit 11
fi

if [ ! -d "$EMBS/fhlmc/$AS_OF_DATE" ]
then
  echo "$T Create a folder $EMBS/fhlmc/$AS_OF_DATE"
  mkdir $EMBS/fhlmc/$AS_OF_DATE
fi
cd $EMBS/fhlmc/$AS_OF_DATE

FHLMONL_PROCESSED=$(expr 0)
ls FHLMONLA.TXT >/dev/null 2>&1
if [[ $? -ne 0 ]]
then
    ls FHLMONLA.ZIP >/dev/null 2>&1
    if [[ $? -ne 0 ]]
    then
		echo copy source file FHLMONLA.ZIP
		if [ ! -f "/net/ybr-prodnfs11/vendordata-PROD/data-grp15/embsdata/embs/daily/$AS_OF_DATE/Products/FHLMONLA.ZIP" ]
		then 
			echo 'There is no monthly files FHLMONLA.ZIP under /net/ybr-prodnfs11/vendordata-PROD/data-grp15/embsdata/embs/daily/$AS_OF_DATE/Products/'
		else	
			cp /net/ybr-prodnfs11/vendordata-PROD/data-grp15/embsdata/embs/daily/$AS_OF_DATE/Products/FHLMONLA.ZIP .
			let FHLMONL_PROCESSED=FHLMONL_PROCESSED+1
		fi
	fi
	unzip -o FHLMONLA.ZIP
else
   	let FHLMONL_PROCESSED=FHLMONL_PROCESSED+1
fi

ls FHLMONLF.TXT
if [[ $? -ne 0 ]]
then
    ls FHLMONLF.ZIP
    if [[ $? -ne 0 ]]
    then
		echo copy source file FHLMONLF.ZIP
		if [ ! -f "/net/ybr-prodnfs11/vendordata-PROD/data-grp15/embsdata/embs/daily/$AS_OF_DATE/Products/FHLMONLF.ZIP" ]
		then 
			echo 'There is no monthly files FHLMONLF.ZIP under /net/ybr-prodnfs11/vendordata-PROD/data-grp15/embsdata/embs/daily/$AS_OF_DATE/Products/'
		else	
			cp /net/ybr-prodnfs11/vendordata-PROD/data-grp15/embsdata/embs/daily/$AS_OF_DATE/Products/FHLMONLF.ZIP .
			let FHLMONL_PROCESSED=FHLMONL_PROCESSED+1
		fi
	fi
	unzip -o FHLMONLF.ZIP
else
   	let FHLMONL_PROCESSED=FHLMONL_PROCESSED+1
fi

if [[ "$FHLMONLA_MISSING" -eq "1" && "$FHLMONLF_MISSING" -eq "1" ]]
then
  echo "No FHLMONLA.ZIP neither $FHLMONLF.ZIP exists."
  exit 13
fi

echo "FHLMONL_PROCESSED=$FHLMONL_PROCESSED"
case $FHLMONL_PROCESSED in
0)
  echo "No FHLMONLA.ZIP neither $FHLMONLF.ZIP exists."
  exit 13
  ;;
1)
  awk -F'|' '{print $1}' FHLMONLA.TXT >FHLMONLAF_key.txt
  ;;
2)
  awk -F'|' '{print $1}' FHLMONLF.TXT >FHLMONLF_key.txt
  ;;
3)
	awk -F'|' '{print $1}' FHLMONLA.TXT >FHLMONLA_key.txt
	awk -F'|' '{print $1}' FHLMONLF.TXT >FHLMONLF_key.txt
	cat FHLMONLA_key.txt FHLMONLF_key.txt > FHLMONLAF_key.txt
	;;
esac	
	
sort FHLMONLAF_key.txt > FHLMONLAF_key_sort.txt
FHLMONLAF_key_num=$(wc -l FHLMONLAF_key_sort.txt | awk  '{print $1;}')
echo "Build index for table fhlmc_loan"
let FHLMONLAF_key_1=FHLMONLAF_key_num/5
let FHLMONLAF_key_2=FHLMONLAF_key_num/5*2
let FHLMONLAF_key_3=FHLMONLAF_key_num/5*3
let FHLMONLAF_key_4=FHLMONLAF_key_num/5*4
key1=$(sed -n "${FHLMONLAF_key_1}p"<FHLMONLAF_key_sort.txt)
key2=$(sed -n "${FHLMONLAF_key_2}p"<FHLMONLAF_key_sort.txt)
key3=$(sed -n "${FHLMONLAF_key_3}p"<FHLMONLAF_key_sort.txt)
key4=$(sed -n "${FHLMONLAF_key_4}p"<FHLMONLAF_key_sort.txt)
key5=$(sed -n "${FHLMONLAF_key_num}p"<FHLMONLAF_key_sort.txt)

java -Xms1024m -Xmx2048m -cp $HBASEJAVA_JAR com.yieldbook.mortgage.hbase.admin.FhlmcLoanInitialize $key1 $key2 $key3 $key4 $key5

echo 'table fhlmc_loan_monthly, table fhlmc_arm_loan_monthly, table fhlmc_mod_loan_monthly has been created.'

