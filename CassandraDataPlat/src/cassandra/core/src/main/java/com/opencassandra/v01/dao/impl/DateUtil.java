package com.opencassandra.v01.dao.impl;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.GregorianCalendar;

/**
 * 日期转换工具类
 * @author wuxiaofeng
 *
 */
public class DateUtil {
	
	/**
	 * 日期转换成字符串
	 * @param mdate
	 * @param format
	 * @return
	 */
	public static String dateToStr(Date mdate,String format) {
		
		String str = null;
		SimpleDateFormat dformat = new SimpleDateFormat(format);
		str = dformat.format(mdate);
		return str;
	}
	
	/**
	 * 字符串转换成日期格式
	 * @param str
	 * @param format
	 * @return
	 */
	public static Date strToDate(String str,String format) {
		
		try {
			SimpleDateFormat dformat = new SimpleDateFormat(format);
			return dformat.parse(str);
		} catch(ParseException e){
			return null;
		}
	}
	
	public static String getDate(long time, String formatStr) {
		
		try {
			Date dat=new Date(time);  
	        GregorianCalendar gc = new GregorianCalendar();   
	        gc.setTime(dat);  
	        SimpleDateFormat format = new SimpleDateFormat(formatStr);  
	        String sb = format.format(gc.getTime());  
	        return sb;
		} catch (Exception e) {
			return null;
		}
	}
	
	public static void main(String[] args) {
		
		String s = "00100130";
		System.out.println(dateToStr(strToDate(s, "yyyyMMdd"), "yyyyMMdd"));
		System.out.println(getDate(System.currentTimeMillis(), "yyyyMMddHHmmss"));
	}
}
