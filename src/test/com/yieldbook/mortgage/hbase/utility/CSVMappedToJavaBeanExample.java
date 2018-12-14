package com.yieldbook.mortgage.hbase.utility;

import java.io.FileReader;
import java.util.List;

import com.opencsv.CSVParser;
import com.opencsv.CSVReader;
import com.opencsv.ICSVParser;
import com.opencsv.bean.ColumnPositionMappingStrategy;
import com.opencsv.bean.CsvToBean;
import com.yieldbook.mortgage.hbase.bean.LoanFhlmc;

public class CSVMappedToJavaBeanExample {
	   @SuppressWarnings({"rawtypes", "unchecked"})
	   public static void main(String[] args) throws Exception
	   {
	      CsvToBean csv = new CsvToBean();
	       
	      String csvFilename = "C:/data/FHLDLYLF_TS.dat";
	      //CSVReader csvReader = new CSVReader(new FileReader(csvFilename));
	      
	      CSVReader csvReader = new CSVReader(new FileReader(csvFilename), '|',
					CSVParser.DEFAULT_QUOTE_CHARACTER,
					ICSVParser.DEFAULT_ESCAPE_CHARACTER,
					CSVReader.DEFAULT_SKIP_LINES,
					ICSVParser.DEFAULT_STRICT_QUOTES,
					ICSVParser.DEFAULT_IGNORE_LEADING_WHITESPACE);
	       
	      //Set column mapping strategy
	      //List list = csv.parse(setColumMapping(), csvReader);
	      
	        csv.setMappingStrategy(setColumMapping());
	        csv.setCsvReader(csvReader);
	        List<LoanFhlmc> list = csv.parse();
	       
	      for (LoanFhlmc loan : list) {
	          System.out.println(loan);
	      }
	   }
	    
	   @SuppressWarnings({"rawtypes", "unchecked"})
	   private static ColumnPositionMappingStrategy setColumMapping()
	   {
	      ColumnPositionMappingStrategy strategy = new ColumnPositionMappingStrategy();
	      strategy.setType(LoanFhlmc.class);
	      String[] columns = new String[] {"loan_identifier", "loan_correction_indicator", "prefix", "security_identifier", "cusip", "mortgage_loan_amount", "issuance_investor_loan_upb", "current_investor_loan_upb", "amortization_type", "original_interest_rate", "issuance_interest_rate", "current_interest_rate", "issuance_net_interest_rate", "current_net_interest_rate", "first_payment_date", "maturity_date", "loan_term", "remaining_months_to_maturity", "loan_age", "ltv", "cltv", "dti", "borrower_credit_score", "f1", "f2", "f3", "number_of_borrowers", "first_time_home_buyer_indicator", "loan_purpose", "occupancy_status", "number_of_units", "property_type", "channel", "property_state", "seller_name", "servicer_name", "mortgage_insurance_percent", "mortgage_insurance_cancellation_indicator", "government_insured_guarantee", "assumability_indicator", "interest_only_loan_indicator", "interest_only_first_principal_and_interest_payment_date", "months_to_amortization", "prepayment_penalty_indicator", "prepayment_penalty_total_term", "index", "mortgage_margin", "mbs_pc_margin", "interest_rate_adjustment_frequency", "interest_rate_lookback", "interest_rate_rounding_method", "interest_rate_rounding_method_percent", "convertibility_indicator", "initial_fixed_rate_period", "next_interest_rate_adjustment_date", "months_to_next_interest_rate_adjustment_date", "life_ceiling_interest_rate", "life_ceiling_net_interest_rate", "life_floor_interest_rate", "life_floor_net_interest_rate", "initial_interest_rate_cap_up_percent", "initial_interest_rate_cap_down_percent", "periodic_interest_rate_cap_up_percent", "periodic_interest_rate_cap_down_percent", "modification_program", "modification_type", "number_of_modifications", "total_capitalized_amount", "interest_bearing_mortgage_loan_amount", "original_deferred_amount", "current_deferred_upb", "loan_age_as_of_modification", "eltv", "updated_credit_score", "f4", "interest_rate_step_indicator", "initial_step_fixed_rate_period", "total_number_of_steps", "number_of_remaining_steps", "next_step_rate", "terminal_step_rate", "terminal_step_date", "step_rate_adjustment_frequency", "next_step_rate_adjustment_date", "months_to_next_step_rate_adjustment_date", "periodic_step_cap_up_percent", "origination_mortgage_loan_amount", "origination_interest_rate", "origination_amortization_type", "origination_interest_only_loan_indicator", "origination_first_payment_date", "origination_maturity_date", "origination_loan_term", "origination_ltv", "origination_cltv", "origination_debt_to_income_ratio", "origination_credit_score", "f5", "f6", "f7", "origination_loan_purpose", "origination_occupancy_status", "origination_channel", "days_delinquent", "loan_performance_history", "as_of_date"};
	      strategy.setColumnMapping(columns);
	      return strategy;
	   }
}
