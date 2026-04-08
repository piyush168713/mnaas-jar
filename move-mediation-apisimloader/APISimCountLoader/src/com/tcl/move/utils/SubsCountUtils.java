package com.tcl.move.utils;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;


public class SubsCountUtils {

	public static String getMonthStartDate() {
		
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
		
		Calendar today = Calendar.getInstance();
		today.set(Calendar.DAY_OF_MONTH, 1);		
		today.set(Calendar.HOUR_OF_DAY, 0);
		today.set(Calendar.MINUTE, 0);
		today.set(Calendar.SECOND, 0);
				
		return format.format(today.getTime());
	}	

	public static List<String> getMonthDates(boolean isPrev) {
		
		Calendar today = Calendar.getInstance();		
		int day = today.get(Calendar.DAY_OF_MONTH) - 1;
		
		if (isPrev) {
			today.set(Calendar.DAY_OF_MONTH, 1);
			today.add(Calendar.MONTH, -1);
			System.out.println(today.getTime());
			day = today.getActualMaximum(Calendar.DAY_OF_MONTH);
		}
		List<String> dates = new ArrayList<String>();
		
		for (int i=1; i<=day; i++) {
			String date = today.get(Calendar.YEAR) + "-" + String.format("%02d", today.get(Calendar.MONTH)+1) + "-" + String.format("%02d" , i);
			dates.add(date);
		}
		
		return dates;
	}

	public static String getMonthCurrentDate() {

		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
		
		Calendar today = Calendar.getInstance();
		today.add(Calendar.DAY_OF_MONTH, -1);		
		today.set(Calendar.HOUR_OF_DAY, 0);
		today.set(Calendar.MINUTE, 0);
		today.set(Calendar.SECOND, 0);
		
		return format.format(today.getTime());
	}

	public static String getPrevMonthStartDate() {
		
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
		
		Calendar today = Calendar.getInstance();
		today.set(Calendar.DAY_OF_MONTH, 1);
		today.add(Calendar.MONTH, -1);
		today.set(Calendar.HOUR_OF_DAY, 0);
		today.set(Calendar.MINUTE, 0);
		today.set(Calendar.SECOND, 0);
				
		return format.format(today.getTime());
	}
	
	public static String getPrevMonthLastDate() {
		
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
		
		Calendar today = Calendar.getInstance();
		today.set(Calendar.DAY_OF_MONTH, 1);
		today.add(Calendar.MONTH, -1);
		today.set(Calendar.DAY_OF_MONTH, today.getActualMaximum(Calendar.DAY_OF_MONTH));
		today.set(Calendar.HOUR_OF_DAY, 0);
		today.set(Calendar.MINUTE, 0);
		today.set(Calendar.SECOND, 0);
				
		return format.format(today.getTime());
	}

	public static boolean isFirstOfMonth() {
				
		Calendar today = Calendar.getInstance();
		if (today.get(Calendar.DAY_OF_MONTH) == 1)
			return true;
		else
			return false;
	}

}
