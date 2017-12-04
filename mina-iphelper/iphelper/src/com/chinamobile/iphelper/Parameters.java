package com.chinamobile.iphelper;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Parameters {

	public static int PORT;
	public static int THREAD;
	public static String STARTIP;
	public static String LOCATION;
	
	static{
		try {
			InputStream in;
			in = new BufferedInputStream(new FileInputStream("conf/service.properties"));
			Properties p = new Properties();
			p.load(in);
			PORT = Integer.parseInt(p.getProperty("http.port"));
			THREAD = Integer.parseInt(p.getProperty("http.threads"));
			STARTIP = p.getProperty("service.ipstart");
			LOCATION = p.getProperty("service.location");
			
			
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	
}
