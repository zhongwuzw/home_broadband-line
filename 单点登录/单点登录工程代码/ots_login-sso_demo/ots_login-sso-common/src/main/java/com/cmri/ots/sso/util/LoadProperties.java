package com.cmri.ots.sso.util;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.Properties;


public class LoadProperties {


	public static Properties loadProperties(String path) {
		Properties prop = new Properties();
		InputStream fis = null;
		try {
			fis = LoadProperties.class.getClassLoader().getResourceAsStream("properties" + File.separatorChar + path);
			prop.load(fis);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (fis != null) {
				try {
					fis.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return prop;
	}

	public static String loadProperties(String path, String key) {
		try {
			Properties prop = loadProperties(path);
			return new String(prop.getProperty(key).getBytes("ISO-8859-1"), "utf8");
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		return null;
	}
}
