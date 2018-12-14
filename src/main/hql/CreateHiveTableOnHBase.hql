use default;
CREATE EXTERNAL TABLE fhlmc_loan_monthly(
loan_identifier string,cusip string,prod_type_ind string,loan_purpose string,tpo_flag string,property_type string,occupancy_status string,num_units string,state string,credit_score int,orig_loan_term int,orig_ltv int,prepay_penalty_flag string,io_flag string,first_payment_date bigint,first_pi_date bigint,maturity_date bigint,orig_note_rate double,pc_issuance_note_rate double,net_note_rate double,orig_loan_amt double,orig_upb double,loan_age int,rem_months_to_maturity int,months_to_amortize int,servicer string,seller string,last_chg_date bigint,current_upb double,eff_date bigint,doc_assets string,doc_empl string,doc_income string,orig_cltv int,num_borrowers string,first_time_buyer string,ins_percent int,orig_dti int,msa string,upd_credit_score int,estm_ltv int,correction_flag string,prefix string,ins_cancel_ind string,govt_ins_grnte string,assumability_ind string,prepay_term string,as_of_date bigint)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES
("hbase.columns.mapping"=":key,m:cusip,m:prod_type_ind,m:loan_purpose,m:tpo_flag,m:property_type,m:occupancy_status,m:num_units,m:state,m:credit_score,m:orig_loan_term,m:orig_ltv,m:prepay_penalty_flag,m:io_flag,m:first_payment_date,m:first_pi_date,m:maturity_date,m:orig_note_rate,m:pc_issuance_note_rate,m:net_note_rate,m:orig_loan_amt,m:orig_upb,m:loan_age,m:rem_months_to_maturity,m:months_to_amortize,m:servicer,m:seller,m:last_chg_date,m:current_upb,m:eff_date,m:doc_assets,m:doc_empl,m:doc_income,m:orig_cltv,m:num_borrowers,m:first_time_buyer,m:ins_percent,m:orig_dti,m:msa,m:upd_credit_score,m:estm_ltv,m:correction_flag,m:prefix,m:ins_cancel_ind,m:govt_ins_grnte,m:assumability_ind,m:prepay_term,m:as_of_date",
"hbase.table.default.storage.type"="binary");

CREATE INDEX as_of_date_i ON TABLE fhlmc_loan_monthly(as_of_date) AS 'COMPACT' WITH DEFERRED REBUILD;

CREATE EXTERNAL TABLE fhlmc_mod_loan_monthly(
loan_identifier string,cusip string,eff_date bigint,correction_flag string,product_type string,origin_loan_purpose string,origin_tpo_flag string,origin_occupancy_status string,origin_credit_score int,origin_loan_term int,origin_ltv int,origin_io_flag string,origin_first_paym_date bigint,origin_maturity_date bigint,origin_note_rate double,origin_loan_amt double,origin_cltv int,origin_dti int,origin_product_type string,mod_date_loan_age int,mod_program string,mod_type string,num_of_mods int,tot_capitalized_amt double,last_chg_date bigint,int_bear_loan_amt double,deferred_amt double,deferred_upb double,rate_step_ind string,tot_steps int,rem_steps int,initial_fixed_per int,rate_adj_freq int,periodic_cap_up double,months_to_adj int,next_step_rate double,next_adj_date bigint,terminal_step_rate double,terminal_step_date bigint,cur_gross_note_rate double,as_of_date bigint)
 STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES
("hbase.columns.mapping"=":key,m:cusip,m:eff_date,m:correction_flag,m:product_type,m:origin_loan_purpose,m:origin_tpo_flag,m:origin_occupancy_status,m:origin_credit_score,m:origin_loan_term,m:origin_ltv,m:origin_io_flag,m:origin_first_paym_date,m:origin_maturity_date,m:origin_note_rate,m:origin_loan_amt,m:origin_cltv,m:origin_dti,m:origin_product_type,m:mod_date_loan_age,m:mod_program,m:mod_type,m:num_of_mods,m:tot_capitalized_amt,m:last_chg_date,m:int_bear_loan_amt,m:deferred_amt,m:deferred_upb,m:rate_step_ind,m:tot_steps,m:rem_steps,m:initial_fixed_per,m:rate_adj_freq,m:periodic_cap_up,m:months_to_adj,m:next_step_rate,m:next_adj_date,m:terminal_step_rate,m:terminal_step_date,m:cur_gross_note_rate,m:as_of_date",
"hbase.table.default.storage.type"="binary");


CREATE EXTERNAL TABLE fhlmc_arm_loan_monthly(
loan_identifier string,cusip string,product_type string,convertible_flag string,rate_adjmt_freq int,initial_period int,next_adjmt_date bigint,lookback int,gross_margin double,net_margin double,net_max_life_rate double,max_life_rate double,init_cap_up double,init_cap_dn double,periodic_cap double,months_to_adjust int,index_desc string,last_chg_date bigint,eff_date bigint,as_of_date bigint)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES
("hbase.columns.mapping"=":key,m:cusip,m:product_type,m:convertible_flag,m:rate_adjmt_freq,m:initial_period,m:next_adjmt_date,m:lookback,m:gross_margin,m:net_margin,m:net_max_life_rate,m:max_life_rate,m:init_cap_up,m:init_cap_dn,m:periodic_cap,m:months_to_adjust,m:index_desc,m:last_chg_date,m:eff_date,m:as_of_date",
"hbase.table.default.storage.type"="binary");

CREATE EXTERNAL TABLE fnma_loan_monthly(
loan_identifier string,cusip string,prod_type_ind string,loan_purpose string,occupancy_type string,num_units string,state string,credit_score int,orig_loan_term int,orig_ltv int,prepay_premium_term string,io_flag string,first_payment_date bigint,first_pi_date bigint,maturity_date bigint,orig_note_rate float,note_rate float,net_note_rate float,orig_loan_size double,loan_age int,rem_months_to_maturity int,months_to_amortize int,servicer string,seller string,last_chg_date bigint,current_upb double,eff_date bigint,orig_dti int,first_time_buyer string,ins_percent int,num_borrowers int,orig_cltv int,property_type string,tpo_flag string,as_of_date bigint)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES
("hbase.columns.mapping"=":key,m:cusip,m:prod_type_ind,m:loan_purpose,m:occupancy_type,m:num_units,m:state,m:credit_score,m:orig_loan_term,m:orig_ltv,m:prepay_premium_term,m:io_flag,m:first_payment_date,m:first_pi_date,m:maturity_date,m:orig_note_rate,m:note_rate,m:net_note_rate,m:orig_loan_size,m:loan_age,m:rem_months_to_maturity,m:months_to_amortize,m:servicer,m:seller,m:last_chg_date,m:current_upb,m:eff_date,m:orig_dti,m:first_time_buyer,m:ins_percent,m:num_borrowers,m:orig_cltv,m:property_type,m:tpo_flag,m:as_of_date",
"hbase.table.default.storage.type"="binary");

CREATE EXTERNAL TABLE fnma_arm_loan_monthly(
loan_identifier string,cusip string,convertible_flag string,rate_adjmt_freq int,initial_period int,next_adjmt_date bigint,lookback int,gross_margin float,net_margin float,net_max_life_rate float,max_life_rate float,init_cap_up float,init_cap_dn float,periodic_cap_up float,periodic_cap_dn float,months_to_adjust int,index_num int,last_chg_date bigint,eff_date bigint,as_of_date bigint)
 STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES
("hbase.columns.mapping"=":key,m:cusip,m:convertible_flag,m:rate_adjmt_freq,m:initial_period,m:next_adjmt_date,m:lookback,m:gross_margin,m:net_margin,m:net_max_life_rate,m:max_life_rate,m:init_cap_up,m:init_cap_dn,m:periodic_cap_up,m:periodic_cap_dn,m:months_to_adjust,m:index_num,m:last_chg_date,m:eff_date,m:as_of_date",
"hbase.table.default.storage.type"="binary");

CREATE EXTERNAL TABLE fnma_mod_loan_monthly(
loan_identifier string,cusip string,eff_date bigint,last_chg_date bigint,days_delinquent string,loan_performance_history string,mod_date_loan_age int,mod_program string,mod_type string,num_of_mods int,tot_capitalized_amt double,origin_loan_amt double,deferred_upb double,rate_step_ind string,initial_fixed_per int,tot_steps int,rem_steps int,next_step_rate float,terminal_step_rate float,terminal_step_date bigint,rate_adj_freq int,months_to_adj int,next_adj_date bigint,periodic_cap_up float,origin_channel string,origin_note_rate float,origin_upb double,origin_loan_term int,origin_first_paym_date bigint,origin_maturity_date bigint,origin_ltv int,origin_cltv int,origin_dti int,origin_credit_score int,origin_loan_purpose string,origin_occupancy_status string,origin_product_type string,origin_io_flag string,as_of_date bigint)
 STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES
("hbase.columns.mapping"=":key,m:cusip,m:eff_date,m:last_chg_date,m:days_delinquent,m:loan_performance_history,m:mod_date_loan_age,m:mod_program,m:mod_type,m:num_of_mods,m:tot_capitalized_amt,m:origin_loan_amt,m:deferred_upb,m:rate_step_ind,m:initial_fixed_per,m:tot_steps,m:rem_steps,m:next_step_rate,m:terminal_step_rate,m:terminal_step_date,m:rate_adj_freq,m:months_to_adj,m:next_adj_date,m:periodic_cap_up,m:origin_channel,m:origin_note_rate,m:origin_upb,m:origin_loan_term,m:origin_first_paym_date,m:origin_maturity_date,m:origin_ltv,m:origin_cltv,m:origin_dti,m:origin_credit_score,m:origin_loan_purpose,m:origin_occupancy_status,m:origin_product_type,m:origin_io_flag,m:as_of_date",
"hbase.table.default.storage.type"="binary");

CREATE EXTERNAL TABLE gnma_loan_monthly(
cusip  String,eff_date  int,loan_seq_num  int,gnma_issuer_num  int,agency  String,loan_purpose  String,refi_type  int,first_payment_date  int,maturity_date  int,note_rate  double,orig_loan_amt  double,orig_upb  double,current_upb  double,orig_loan_term  int,loan_age  int,rem_months_to_maturity  int,months_delinq  int,months_prepaid  int,gross_margin  double,orig_ltv  double,orig_cltv  double,orig_dti  double,credit_score  int,down_paym_assist  String,buy_down_status  String,upfront_mip  double,annual_mip  double,num_borrowers  String,first_time_buyer  String,num_units  String,state  String,msa  String,tpo_flag  String,curr_month_liq_flag  String,removal_reason  int,lastChgDate  int,loan_orig_date  int,seller_issuer  String)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES
("hbase.columns.mapping"=":key,m:cusip,m:eff_date,m:gnma_issuer_num,m:agency,m:loan_purpose,m:refi_type,m:first_payment_date,m:maturity_date,m:note_rate,m:orig_loan_amt,m:orig_upb,m:current_upb,m:orig_loan_term,m:loan_age,m:rem_months_to_maturity,m:months_delinq,m:months_prepaid,m:gross_margin,m:orig_ltv,m:orig_cltv,m:orig_dti,m:credit_score,m:down_paym_assist,m:buy_down_status,m:upfront_mip,m:annual_mip,m:num_borrowers,m:first_time_buyer,m:num_units,m:state,m:msa,m:tpo_flag,m:curr_month_liq_flag,m:removal_reason,m:lastChgDate,m:loan_orig_date,m:seller_issuer,"hbase.table.default.storage.type"="binary");




