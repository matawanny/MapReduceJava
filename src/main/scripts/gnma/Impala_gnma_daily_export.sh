#!/bin/bash

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

write_signature(){
  local prefixFileName=$(echo $1|cut -f 1 -d '.' )
  local agencyName=$(echo $1|cut -f 1 -d '_' )
  local sigFileName=$EXPORT
  sigFileName+="/"
  sigFileName+=$agencyName
  sigFileName+="/"
  sigFileName+=$2
  sigFileName+="/"
  sigFileName+=$prefixFileName
  sigFileName+=".SIG"
  
  local dataFileName=$prefixFileName
  dataFileName+=".dat"
  
  local output="`date "+%Y%m%d %H:%M:%S"`";
  output+=" "
  output+=$dataFileName
  echo $output > $sigFileName
}

gnma_loan_daily(){
local output_file=gnma_loan_daily.dat

impala-shell -B -i $IMPALA -q "invalidate metadata; select cusip, eff_date, loan_seq_num, 
gnma_issuer_num, NVL(agency, '') as agency, NVL(loan_purpose, '') as loan_purpose, refi_type, 
first_payment_date, maturity_date, round(note_rate, 3) as note_rate, 
round(orig_loan_amt, 2) as orig_loan_amt, cast(orig_upb as decimal(11,2)) as orig_upb, 
round(current_upb, 2) as current_upb, orig_loan_term, loan_age, rem_months_to_maturity, 
months_delinq, months_prepaid, round(gross_margin,3) as gross_margin, 
round(orig_ltv,2) as orig_ltv, round(orig_cltv,2) as orig_cltv, 
round(orig_dti,2) as orig_dti, credit_score, 
NVL(down_paym_assist, '') as down_paym_assist, NVL(buy_down_status, '') as buy_down_status, 
IF(upfront_mip=0,NULL,round(upfront_mip, 3)) as upfront_mip, 
IF(annual_mip=0,NULL,round(annual_mip, 3)) as annual_mip, 
NVL(num_borrowers, '') as num_borrowers, NVL(first_time_buyer, '') as first_time_buyer, 
NVL(num_units, '') as num_units, NVL(state, '') as state, NVL(msa, '') as msa, 
NVL(tpo_flag, '') as tpo_flag, NVL(curr_month_liq_flag, '') as curr_month_liq_flag, removal_reason, 
'' as last_chg_date, loan_orig_date, NVL(seller_issuer_id, '') as seller_issuer_id from  
prd1.gnma_loan_daily where as_of_date=$AS_OF_DATE" -o $output_file --print_header --output_delimiter=\|

firstWord=$(head -n  1 $output_file|cut -f 1 -d '|')
if [ "$firstWord" != "cusip" ]
then
   sed -i 1d $output_file
fi

FILELINES=$(wc -l $output_file | awk  '{print $1;}') 
if [ $FILELINES -le 1 ]
then
   rm -rf $output_file
else 
	sed -i "s/NULL//g" $output_file
	write_signature $output_file $AS_OF_DATE
fi
}

gnma_arm_loan_daily(){
local output_file=gnma_arm_loan_daily.dat

impala-shell -B -i $IMPALA -q "invalidate metadata;select cusip, loan_seq_num, 
NVL(index_type, '') as index_type, lookback, interest_chg_date, 
round(init_rate_cap, 1) as init_rate_cap, 
round(sub_init_rate_cap, 1) as sub_init_rate_cap, 
round(life_time_rate_cap, 1) as life_time_rate_cap, 
round(next_interest_rate_ceiling, 3) as next_interest_rate_ceiling, 
round(life_time_rate_ceiling, 3) as life_time_rate_ceiling, 
round(life_time_rate_floor, 3) as life_time_rate_floor, 
round(prospect_interest_rate, 3) as prospect_interest_rate, 
'' as last_chg_date, eff_date from prd1.gnma_arm_loan_daily where 
as_of_date=$AS_OF_DATE" -o $output_file --print_header --output_delimiter=\|

firstWord=$(head -n  1 $output_file|cut -f 1 -d '|')
if [ "$firstWord" != "cusip" ]
then
   sed -i 1d $output_file
fi

FILELINES=$(wc -l $output_file | awk  '{print $1;}') 
if [ $FILELINES -le 1 ]
then 
   rm -rf $output_file
else
	sed -i "s/NULL//g" $output_file
	write_signature $output_file $AS_OF_DATE
fi
}

if [ ! -d "$EXPORT/gnma/$AS_OF_DATE" ]
then
  echo "Create a folder $EXPORT/gnma/$AS_OF_DATE"
  mkdir $EXPORT/gnma/$AS_OF_DATE
fi
cd $EXPORT/gnma/$AS_OF_DATE

if [ -z "$2" ]
then
  gnma_loan_daily
  gnma_arm_loan_daily
else
	case $2 in
	1)
	  gnma_loan_daily
	  ;;
	2)
	  gnma_arm_loan_daily
	  ;;
	3)
	  gnma_loan_daily    
	  gnma_arm_loan_daily
	  ;;
	esac
fi

ls >/dev/null 2>&1
if [[ $? -ne 0 ]]
then
	cd ..
	rmdir $AS_OF_DATE
fi
