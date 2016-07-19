package com.qualitymap.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class UtilDate {

	/**
	 * 获取某一月的第一天（caendar的月份是从0开始的）
	 * 
	 * @param tdate
	 * @return
	 * @return String
	 */
	public static String getFirstDayOfMonth(String tdate) {
		Calendar cal = Calendar.getInstance();

		int year = Integer.parseInt(tdate.substring(0, 4));
		int month = Integer.parseInt(tdate.substring(4, tdate.length()));
		cal.set(Calendar.YEAR, year);
		cal.set(Calendar.MONTH, month - 1);
		cal.set(Calendar.DAY_OF_MONTH, cal.getMinimum(Calendar.DATE));
		return new SimpleDateFormat("yyyy-MM-dd ").format(cal.getTime());
	}

	/**
	 * 获取某一月的第一天（caendar的月份是从0开始的）
	 * 
	 * @param tdate
	 * @return
	 * @return String
	 */
	public static String getFirstDayOfNextMonth(String tdate) {
		Calendar cal = Calendar.getInstance();

		int year = Integer.parseInt(tdate.substring(0, 4));
		int month = Integer.parseInt(tdate.substring(4, tdate.length()));
		cal.set(Calendar.YEAR, year);
		cal.set(Calendar.MONTH, month);
		cal.set(Calendar.DAY_OF_MONTH, cal.getMinimum(Calendar.DATE));
		return new SimpleDateFormat("yyyy-MM-dd ").format(cal.getTime());
	}

	/**
	 * 获取上一月份
	 * 
	 * @param tdate
	 * @return
	 * @return String
	 */
	public static String getPreviousMonth(String tdate) {
		Calendar cal = Calendar.getInstance();

		int year = Integer.parseInt(tdate.substring(0, 4));
		int month = Integer.parseInt(tdate.substring(4, tdate.length()));
		cal.set(Calendar.YEAR, year);
		cal.set(Calendar.MONTH, month - 2);
		cal.set(Calendar.DAY_OF_MONTH, cal.getMinimum(Calendar.DATE));
		return new SimpleDateFormat("yyyyMM").format(cal.getTime());
	}
	
	
	public static void main(String[] args) {
		System.out.println(getPreviousMonth("201601"));
	}
	/**
	 * 获取月份
	 * 
	 * @param tdate
	 * @return
	 * @return String
	 */
	public static String getMonth() {
		 Calendar now = Calendar.getInstance();  
		 String mo = (now.get(Calendar.MONTH)+1)+"";
		 if(mo.length()==1){
			 mo = "0"+mo;
		 }
		 String month = now.get(Calendar.YEAR) +mo;
		return month;
	}

	/**
	 * 日期转换成字符串
	 * 
	 * @param mdate
	 * @param format
	 * @return
	 */
	public static String dateToStr(Date mdate, String format) {

		String str = null;
		SimpleDateFormat dformat = new SimpleDateFormat(format);
		str = dformat.format(mdate);
		return str;
	}

	/**
	 * 字符串转换成日期格式
	 * 
	 * @param str
	 * @param format
	 * @return
	 */
	public static Date strToDate(String str, String format) {

		try {
			SimpleDateFormat dformat = new SimpleDateFormat(format);
			return dformat.parse(str);
		} catch (ParseException e) {
			return null;
		}
	}

	
	 /**
	   * 将长时间格式时间转换为字符串 yyyy-MM-dd HH:mm:ss
	   * 
	   * @param dateDate
	   * @return
	   */
	public static String dateToStrLong(Date dateDate) {
	   SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	   String dateString = formatter.format(dateDate);
	   return dateString;
	}
}
