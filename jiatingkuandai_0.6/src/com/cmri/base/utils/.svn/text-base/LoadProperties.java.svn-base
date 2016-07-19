package com.cmri.base.utils;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class LoadProperties {
	protected static Log logger =  LogFactory.getLog(LoadProperties.class);
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Properties p = LoadProperties.loadProperties("excelfile.properties");
		String path="";
		try {
			path = new String(p.getProperty("excelfile.path").getBytes("ISO-8859-1"),"utf8");
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		logger.debug(path);
	}
	public static Properties loadProperties(String path){
		Properties prop = new Properties();
	    InputStream fis;
		try {
			fis = LoadProperties.class.getClassLoader().getResourceAsStream(path);
			prop.load(fis);
		}catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    return prop;
	}
	public static String loadProperties(String path,String key){
		Properties prop = new Properties();
	    InputStream fis;
	    String result="";
		try {
			fis = LoadProperties.class.getClassLoader().getResourceAsStream(path);
			prop.load(fis);
			result = new String(prop.getProperty(key).getBytes("ISO-8859-1"),"utf8");
		}catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    return result;
	}
}
