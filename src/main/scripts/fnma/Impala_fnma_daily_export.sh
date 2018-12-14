#!/bin/bash
currentUser=$(whoami)
if [ "$currentUser" != "oozie" ]
then
    echo "Must login in as oozie"
    exit 10;
fi

if [ -z "$BASE" ]
then
  source $OOZIE_HOME/SetupEnv.sh
fi

AS_OF_DATE=$1
if [ -z "$AS_OF_DATE" ]
then
      echo "Must input as of date!"
      exit 11;
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
  
  local output="`date "+%Y%m%d %H:%M:%S"`"
  output+=" "
  output+=$dataFileName
  echo $output > $sigFileName
}

fnma_loan_daily(){
local output_file=fnma_loan_daily.dat
impala-shell -B -i $IMPALA -q "invalidate metadata; select cusip, loan_identifier as loan_num,  
CASE prod_type_ind WHEN 'FRM' THEN 'F' WHEN 'ARM' THEN 'A' ELSE NULL END as prod_type_ind, 
NVL(substr(loan_purpose,1,1),'') as loan_purpose,  
NVL(substr(occupancy_type,1,1),'') as occupancy_type, num_units, state, credit_score, orig_loan_term, orig_ltv, 
NVL(substr(prepay_premium_term,1,1),'') as prepay_premium_term, NVL(substr(io_flag,1,1), '') as io_flag, 
NVL(from_unixtime(first_payment_date div 1000,'yyyyMMdd'),'') as first_payment_date, 
NVL(from_unixtime(first_pi_date div 1000,'yyyyMMdd'),'') as first_pi_date, 
NVL(from_unixtime(maturity_date div 1000,'MM/yyyy'),'') as maturity_date, round(orig_note_rate,4) as orig_note_rate, 
round(note_rate,4) as note_rate, round(net_note_rate,4) as net_note_rate, round(orig_loan_size,2) as orig_loan_size,  
loan_age, rem_months_to_maturity, NVL(months_to_amortize,0) as months_to_amortize, 
CASE WHEN LENGTH(servicer)>0 THEN REGEXP_REPLACE(servicer, '[^a-zA-Z0-9 ]+', '') ELSE 'UNKNOWN' END as servicer, 
CASE WHEN LENGTH(seller)>0 THEN REGEXP_REPLACE(seller, '[^a-zA-Z0-9 ]+', '') ELSE 'UNKNOWN' END as seller, 
'' as last_chg_date, round(current_upb,2) as current_upb, eff_date, orig_dti, 
NVL(substr(first_time_buyer,1,1), '') as first_time_buyer, ins_percent, 
IF(num_borrowers>=10,cast(num_borrowers as string),concat('0',cast(num_borrowers as string))) as num_borrowers, 
orig_cltv, CASE property_type WHEN 'COOP' THEN 'CP' WHEN '' THEN '' ELSE substr(property_type,1,2) END as property_type, 
NVL(substr(tpo_flag,1,1), '') as tpo_flag   
from prd1.fnma_loan_daily where as_of_date=$AS_OF_DATE " -o $output_file --print_header --output_delimiter=\|

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

fnma_arm_loan_daily(){
local output_file=fnma_arm_loan_daily.dat

impala-shell -B -i $IMPALA -q "invalidate metadata; select cusip, loan_identifier as loan_num,  
NVL(substr(convertible_flag,1,1), '') as convertible_flag, rate_adjmt_freq, initial_period, 
NVL(from_unixtime(next_adjmt_date div 1000,'yyyyMMdd'),'') as next_adjmt_date, lookback, 
round(gross_margin,4) as gross_margin, round(net_margin,4) as net_margin, 
round(net_max_life_rate,4) as net_max_life_rate, round(max_life_rate,4) as max_life_rate, 
round(init_cap_up,4) as init_cap_up, round(init_cap_dn,4) as init_cap_dn, round(periodic_cap_up,4) as periodic_cap_up, 
round(periodic_cap_dn,4) as periodic_cap_dn, months_to_adjust, index_num, '' as last_chg_date, 
eff_date from prd1.fnma_arm_loan_daily 
where as_of_date=$AS_OF_DATE" -o $output_file --print_header --output_delimiter=\|

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

fnma_mod_loan_daily(){
local output_file=fnma_mod_loan_daily.dat

impala-shell -B -i $IMPALA -q "invalidate metadata; select cusip, loan_identifier as loan_num,   
eff_date, '' as last_chg_date, NVL(days_delinquent,'') as days_delinquent,
NVL(loan_performance_history, '') as loan_performance_history, mod_date_loan_age, 
NVL(mod_program, '') as mod_program, NVL(mod_type, '') as mod_type, num_of_mods, 
round(tot_capitalized_amt,2) as tot_capitalized_amt, origin_loan_amt, 
round(deferred_upb,2) as deferred_upb, NVL(rate_step_ind, '') as rate_step_ind, initial_fixed_per, tot_steps, 
rem_steps, round(next_step_rate,3) as next_step_rate, round(terminal_step_rate,3) as terminal_step_rate, 
NVL(from_unixtime(terminal_step_date div 1000,'yyyyMMdd'),'') as terminal_step_date, rate_adj_freq, 
months_to_adj, NVL(from_unixtime(next_adj_date div 1000,'yyyyMMdd'),'') as next_adj_date, 
round(periodic_cap_up,3) as periodic_cap_up, NVL(origin_channel,'') as origin_channel, 
round(origin_note_rate,4) as origin_note_rate, round(origin_upb,2) as origin_upb, origin_loan_term, 
NVL(from_unixtime(origin_first_paym_date div 1000,'yyyyMMdd'),'') as origin_first_paym_date, 
NVL(from_unixtime(origin_maturity_date div 1000,'yyyyMMdd'),'') as origin_maturity_date, origin_ltv, 
origin_cltv, origin_dti, origin_credit_score, NVL(origin_loan_purpose,'') as origin_loan_purpose, 
NVL(origin_occupancy_status,'') as origin_occupancy_status, NVL(origin_product_type, '') as origin_product_type, 
NVL(origin_io_flag, '') as origin_io_flag from prd1.fnma_mod_loan_daily 
where as_of_date=$AS_OF_DATE" -o $output_file --print_header --output_delimiter=\|

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

if [ ! -d "$EXPORT/fnma/$AS_OF_DATE" ]
then
  echo "$T Create a folder $EXPORT/fnma/$AS_OF_DATE"
  mkdir $EXPORT/fnma/$AS_OF_DATE
fi
cd $EXPORT/fnma/$AS_OF_DATE

if [ -z "$2" ]
then
  fnma_loan_daily
  fnma_arm_loan_daily
  fnma_mod_loan_daily
else
	case $2 in
	1)
	  fnma_loan_daily
	  ;;
	2)
	  fnma_arm_loan_daily
	  ;;
	3)
	  fnma_loan_daily    
	  fnma_arm_loan_daily
	  ;;
	4)
	  fnma_mod_loan_daily
	  ;;
	5)
	  fnma_loan_daily 
	  fnma_mod_loan_daily
	  ;;
	6)
	  fnma_arm_loan_daily
	  fnma_mod_loan_daily
	  ;;
	7)
	  fnma_loan_daily
	  fnma_arm_loan_daily
	  fnma_mod_loan_daily
	  ;;
	esac
fi

ls >/dev/null 2>&1
if [[ $? -ne 0 ]]
then
	cd ..
	rmdir $AS_OF_DATE
fi
