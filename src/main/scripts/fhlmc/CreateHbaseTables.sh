#!/bin/bash

awk -F'|' '{print $1}' FHLMONLA.TXT >FHLMONLA_key.txt
sort FHLMONLA_key.txt > FHLMONLA_key_sort.txt

awk -F'|' '{print $1}' FHLMONLF.TXT >FHLMONLF_key.txt
sort FHLMONLF_key.txt > FHLMONLF_key_sort.txt

cat FHLMONLA_key.txt FHLMONLF_key.txt > FHLMONLAF_key.txt
sort FHLMONLAF_key.txt > FHLMONLAF_key_sort.txt


FHLMONLA_key_num=$(wc -l FHLMONLA_key_sort.txt | awk  '{print $1;}')
FHLMONLF_key_num=$(wc -l FHLMONLF_key_sort.txt | awk  '{print $1;}')
FHLMONLAF_key_num=$(wc -l FHLMONLAF_key_sort.txt | awk  '{print $1;}')

echo "table fhlmc_loan"
let FHLMONLAF_key_1=FHLMONLAF_key_num/5
let FHLMONLAF_key_2=FHLMONLAF_key_num/5*2
let FHLMONLAF_key_3=FHLMONLAF_key_num/5*3
let FHLMONLAF_key_4=FHLMONLAF_key_num/5*4
sed -n "${FHLMONLAF_key_1}p"<FHLMONLAF_key_sort.txt
sed -n "${FHLMONLAF_key_2}p"<FHLMONLAF_key_sort.txt
sed -n "${FHLMONLAF_key_3}p"<FHLMONLAF_key_sort.txt
sed -n "${FHLMONLAF_key_4}p"<FHLMONLAF_key_sort.txt
sed -n "${FHLMONLAF_key_num}p"<FHLMONLAF_key_sort.txt

echo "table fhlmc_arm_loan"
let FHLMONLA_key_1=FHLMONLA_key_num/5
let FHLMONLA_key_2=FHLMONLA_key_num/5*2
let FHLMONLA_key_3=FHLMONLA_key_num/5*3
let FHLMONLA_key_4=FHLMONLA_key_num/5*4
sed -n "${FHLMONLA_key_1}p"<FHLMONLA_key_sort.txt
sed -n "${FHLMONLA_key_2}p"<FHLMONLA_key_sort.txt
sed -n "${FHLMONLA_key_3}p"<FHLMONLA_key_sort.txt
sed -n "${FHLMONLA_key_4}p"<FHLMONLA_key_sort.txt
sed -n "${FHLMONLA_key_num}p"<FHLMONLA_key_sort.txt

echo "table fhlmc_mod_loan"
let FHLMONLF_key_1=FHLMONLF_key_num/5
let FHLMONLF_key_2=FHLMONLF_key_num/5*2
let FHLMONLF_key_3=FHLMONLF_key_num/5*3
let FHLMONLF_key_4=FHLMONLF_key_num/5*4
sed -n "${FHLMONLF_key_1}p"<FHLMONLF_key_sort.txt
sed -n "${FHLMONLF_key_2}p"<FHLMONLF_key_sort.txt
sed -n "${FHLMONLF_key_3}p"<FHLMONLF_key_sort.txt
sed -n "${FHLMONLF_key_4}p"<FHLMONLF_key_sort.txt
sed -n "${FHLMONLF_key_num}p"<FHLMONLF_key_sort.txt

#create 'fhlmc_loan_hbase', {NAME =>'m'}, {SPLITS => ['C05084008226', 'J15433000241', 'Q20826000030', 'V60172000602','V84230000010']}
#create 'fhlmc_arm_loan_hbase', {NAME =>'m'}, {SPLITS => ['C05084008226', 'J15433000241', 'Q20826000030', 'V60172000602','V84230000010']}
#create 'fhlmc_mod_loan_hbase', {NAME =>'m'}, {SPLITS => ['C05084008226', 'J15433000241', 'Q20826000030', 'V60172000602','V84230000010']}


