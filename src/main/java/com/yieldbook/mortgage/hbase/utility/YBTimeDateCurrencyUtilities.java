package com.yieldbook.mortgage.hbase.utility;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class YBTimeDateCurrencyUtilities {

	public static final DateFormat df = new SimpleDateFormat("yyyyMMdd");
	public static final DateFormat dft = new SimpleDateFormat("yyyyMMddhh:mm");
	
	public static final DateFormat df1 = new SimpleDateFormat("MM/dd/yyyy");
	public static final DateFormat dft1 = new SimpleDateFormat("MM/dd/yyyy");	
	
	public static final DateFormat dfmonthyear = new SimpleDateFormat("MMyyyy");
	public static final DateFormat dfmonthyear1 = new SimpleDateFormat("MM/yyyy");	
	public static long getMillionSeconds(String asOfDate) throws ParseException{
		
		Date date = null;
		
		if(asOfDate.contains("/")){
			if(asOfDate.contains(":"))
				date = dft1.parse(asOfDate);
			else
				date = df1.parse(asOfDate);
		}else{
			if(asOfDate.contains(":"))
				date = dft.parse(asOfDate);
			else
				date = df.parse(asOfDate);
		}	
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		return calendar.getTimeInMillis();
	}
	
	public static long getMonthYearMillionSeconds(String asOfDate) throws ParseException{
		
		Date date = null;
		
		if(asOfDate.contains("/")){
			date = dfmonthyear1.parse(asOfDate);
		}else{
			date = dfmonthyear.parse(asOfDate);
		}	
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		return calendar.getTimeInMillis();
	}
	
	public static String getMonthYearMillionSecsEmbs(String asOfDate){
		
		Date date = null;
		try {
			
			if(asOfDate.contains("/")){
				if(asOfDate.length()==6){
					asOfDate="0"+asOfDate;
				}
				date = dfmonthyear1.parse(asOfDate);
			}else{
				if(asOfDate.length()==5){
					asOfDate="0"+asOfDate;
				}
				date = dfmonthyear.parse(asOfDate);
			}			
		} catch (ParseException e) {
			return asOfDate;
		}
		
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		return calendar.getTimeInMillis()+"";
	}
	
	public static String getMonthYearDayMillionSecsEmbs(String asOfDate){
		
		Date date = null;
		
		try {
			
			if(asOfDate.contains("/")){

				date = df1.parse(asOfDate);
			}else{
				date = df.parse(asOfDate);
			}			
		} catch (ParseException e) {
			return asOfDate;
		}
		
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		return calendar.getTimeInMillis()+"";
	}
	
	public static String getMonthYearMillionSecsStringEmbs(String asOfDate){
		
		Date date = null;
		try {
			date = dfmonthyear.parse(asOfDate);
		} catch (ParseException e) {

		}
		
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		return calendar.getTimeInMillis() + "";
	}
	
	public static String getEffectiveDate(String dateInSeconds){
		
		Calendar calendar = Calendar.getInstance();
		calendar.setTimeInMillis(Long.parseLong(dateInSeconds));
		int mYear = calendar.get(Calendar.YEAR);
		int mMonth = calendar.get(Calendar.MONTH)+1;
		int mDay = 1;
		StringBuilder sb = new StringBuilder();
		sb.append(mYear);
		if(mMonth<10)
			sb.append("0");
		sb.append(mMonth);
		if(mDay<10)
			sb.append("0");
		sb.append(mDay);
		return sb.toString();
	}
	
	public static String getAsOfDateMonthly(String yearMonthDate){
		StringBuilder sb = new StringBuilder();
		sb.append(yearMonthDate.substring(0,6));
		sb.append("00");
		return sb.toString();
	}
	
	public static String convertDataFormat(String asOfDate){
		Date date = null;
		try {
			if(asOfDate.contains("/")){

				date = df1.parse(asOfDate);
			}else{
				date = df.parse(asOfDate);
			}			
		} catch (ParseException e) {
			return asOfDate;
		}
		return df.format(date);
	}
	public static String convertMMyyyDataFormat(String asOfDate){
		Date date = null;
		try {
			if(asOfDate.contains("/")){
				date = dfmonthyear1.parse(asOfDate);
			}else{
				date = dfmonthyear.parse(asOfDate);
			}			
		} catch (ParseException e) {
			return asOfDate;
		}
		return df.format(date);
	}	
}
