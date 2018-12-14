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

echo $AS_OF_DATE
asOfDateMonthly=${AS_OF_DATE:0:6}
asOfDateMonthly+="00"
echo $asOfDateMonthly

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

fhlmc_loan_monthly(){
local output_file=fhlmc_loan_monthly.dat

impala-shell -B -i $IMPALA -q "invalidate metadata; select cusip, loan_identifier as loan_seq_num, 
CASE prod_type_ind WHEN 'FRM' THEN 'F' WHEN 'ARM' THEN 'A' ELSE NULL END as prod_type_ind, loan_purpose, 
IF(tpo_flag='9','',tpo_flag) as tpo_flag, IF(property_type='99','',property_type) as property_type, 
IF(occupancy_status='9','',occupancy_status) as occupancy_status, 
IF(num_units='99','',num_units) as num_units, state, 
IF(credit_score=9999,NULL,credit_score) as credit_score, orig_loan_term, 
IF(orig_ltv=999,NULL,orig_ltv) as orig_ltv, prepay_penalty_flag, io_flag, 
NVL(from_unixtime(first_payment_date div 1000,'yyyyMMdd'), '') as first_payment_date, 
NVL(from_unixtime(first_pi_date div 1000,'yyyyMMdd'),'') as first_pi_date, 
NVL(from_unixtime(maturity_date div 1000,'yyyyMMdd'), '') as maturity_date, 
round(orig_note_rate,3) as orig_note_rate, round(pc_issuance_note_rate,3) as pc_issuance_note_rate, 
round(net_note_rate,3) as net_note_rate, round(orig_loan_amt,2) as orig_loan_amt, round(orig_upb,2) as orig_upb, 
loan_age, rem_months_to_maturity, NVL(months_to_amortize,0) as months_to_amortize, 
CASE WHEN LENGTH(servicer)>0 THEN REGEXP_REPLACE(servicer, '[^a-zA-Z0-9 ]+', '') ELSE 'UNKNOWN' END as servicer, 
CASE WHEN LENGTH(seller)>0 THEN REGEXP_REPLACE(seller, '[^a-zA-Z0-9 ]+', '') ELSE 'UNKNOWN' END as seller, 
'' as last_chg_date, round(current_upb, 2) as current_upb, eff_date, doc_assets, doc_empl, doc_income, 
IF(orig_cltv=999,NULL,orig_cltv) as orig_cltv, IF(num_borrowers='99','',num_borrowers) as num_borrowers, 
IF(first_time_buyer='9',NULL,first_time_buyer) as first_time_buyer, 
IF(ins_percent=999,NULL,ins_percent) as ins_percent, IF(orig_dti=999,NULL,orig_dti) as orig_dti, msa, 
IF(upd_credit_score=9999,NULL,upd_credit_score) as upd_credit_score, 
IF(estm_ltv=999,NULL,estm_ltv) as estm_ltv, correction_flag, prefix, 
IF(ins_cancel_ind='7', '', ins_cancel_ind) as ins_cancel_ind, 
govt_ins_grnte, assumability_ind, IF(prepay_term='99','',prepay_term) as prepay_term  from prd1.fhlmc_loan_daily 
where as_of_date=$asOfDateMonthly " -o $output_file --print_header --output_delimiter=\|

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

fhlmc_arm_loan_monthly(){
local output_file=fhlmc_arm_loan_monthly.dat

impala-shell -B -i $IMPALA -q "invalidate metadata; select cusip, loan_identifier as loan_seq_num, 
convertible_flag, rate_adjmt_freq, initial_period, 
NVL(from_unixtime(next_adjmt_date div 1000,'yyyyMMdd'),'') as next_adjmt_date, lookback, 
IF(cast(gross_margin as decimal(5,3))=77.777,NULL,cast(gross_margin as decimal(5,3))) as gross_margin, 
IF(cast(net_margin as decimal(5,3))=77.777,NULL,cast(net_margin as decimal(5,3))) as net_margin,
IF(cast(net_max_life_rate as decimal(5,3))=77.777,NULL,cast(net_max_life_rate as decimal(5,3))) as net_max_life_rate,
IF(cast(max_life_rate as decimal(5,3))=77.777,NULL,cast(max_life_rate as decimal(5,3))) as max_life_rate,
IF(cast(init_cap_up as decimal(5,3))=77.777,NULL,cast(init_cap_up as decimal(5,3))) as init_cap_up,
IF(cast(init_cap_dn as decimal(5,3))=77.777,NULL,cast(init_cap_dn as decimal(5,3))) as init_cap_dn,
IF(cast(periodic_cap as decimal(5,3))=77.777,NULL,cast(periodic_cap as decimal(5,3))) as periodic_cap,
months_to_adjust, index_desc, '' as last_chg_date, 
eff_date from prd1.fhlmc_arm_loan_daily 
where as_of_date=$asOfDateMonthly " -o $output_file --print_header --output_delimiter=\|

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

fhlmc_mod_loan_monthly(){
local output_file=fhlmc_mod_loan_monthly.dat

impala-shell -B -i $IMPALA -q "invalidate metadata; select cusip, loan_identifier as loan_seq_num, 
eff_date, correction_flag, product_type, 
IF(origin_loan_purpose='9',NULL,origin_loan_purpose) as origin_loan_purpose, 
IF(origin_tpo_flag='9',NULL,origin_tpo_flag) as origin_tpo_flag, 
IF(origin_occupancy_status='9','',origin_occupancy_status) as origin_occupancy_status, 
IF(origin_credit_score=9999,NULL,origin_credit_score) as origin_credit_score, origin_loan_term, 
IF(origin_ltv=999,NULL,origin_ltv) as origin_ltv, origin_io_flag, 
NVL(from_unixtime(origin_first_paym_date div 1000,'yyyyMMdd'),'') as origin_first_paym_date, 
NVL(from_unixtime(origin_maturity_date div 1000,'yyyyMMdd'),'') as origin_maturity_date, 
round(origin_note_rate,3) as origin_note_rate, round(origin_loan_amt,2) as origin_loan_amt, 
IF(origin_cltv=999,NULL,origin_cltv) as origin_cltv, IF(origin_dti=999,NULL,origin_dti) as origin_dti, 
origin_product_type, mod_date_loan_age, IF(mod_program='9','',mod_program) mod_program, mod_type, 
num_of_mods, round(tot_capitalized_amt,2) as tot_capitalized_amt, '' as last_chg_date, 
round(int_bear_loan_amt,2) as int_bear_loan_amt, round(deferred_amt,2) as deferred_amt, 
round(deferred_upb,2) as deferred_upb, rate_step_ind, tot_steps, rem_steps, initial_fixed_per, 
rate_adj_freq, periodic_cap_up, months_to_adj, round(next_step_rate,3) as next_step_rate, 
NVL(from_unixtime(next_adj_date div 1000,'yyyyMMdd'),'') as next_adj_date, 
round(terminal_step_rate,3) as  terminal_step_rate, 
NVL(from_unixtime(terminal_step_date div 1000,'yyyyMMdd'),'') as terminal_step_date, 
round(cur_gross_note_rate,3) as cur_gross_note_rate from prd1.fhlmc_mod_loan_daily 
where as_of_date=$asOfDateMonthly" -o $output_file --print_header --output_delimiter=\|

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

if [ ! -d $EXPORT/fhlmc/$AS_OF_DATE ]
then
	echo "$T Create folder $EXPORT/fhlmc/$AS_OF_DATE"
	mkdir $EXPORT/fhlmc/$AS_OF_DATE
fi
cd $EXPORT/fhlmc/$AS_OF_DATE

if [ -z "$2" ]
then
  fhlmc_loan_monthly
  fhlmc_arm_loan_monthly
  fhlmc_mod_loan_monthly
else
	case $2 in
	1)
	  fhlmc_loan_monthly
	  ;;
	2)
	  fhlmc_arm_loan_monthly
	  ;;
	3)
	  fhlmc_loan_monthly
	  fhlmc_arm_loan_monthly
	  ;;
	4)
	  fhlmc_mod_loan_monthly
	  ;;
	5)
	  fhlmc_loan_monthly
	  fhlmc_mod_loan_monthly
	  ;;
	6)
	  fhlmc_arm_loan_monthly
	  fhlmc_mod_loan_monthly
	  ;;
	7)
	  fhlmc_loan_monthly
	  fhlmc_arm_loan_monthly
	  fhlmc_mod_loan_monthly
	  ;;
	esac
fi

ls >/dev/null 2>&1
if [[ $? -ne 0 ]]
then
	cd ..
	rmdir $AS_OF_DATE
fi
