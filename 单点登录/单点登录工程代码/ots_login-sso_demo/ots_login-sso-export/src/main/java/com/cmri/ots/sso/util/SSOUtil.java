package com.cmri.ots.sso.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URL;
import java.net.URLConnection;
import java.util.Properties;

public class SSOUtil {

	public static String sendPost(String url ,String params){
		
		InputStream in = null;
		InputStreamReader isr = null;
		OutputStreamWriter osw = null;
		StringBuffer res = new StringBuffer();
		try {
			URL httpurl = new URL(url);
			URLConnection uc  = httpurl.openConnection();
			uc.setDoOutput(true);
			osw = new OutputStreamWriter(uc.getOutputStream());
			osw.write(params);
			osw.flush();
			in = uc.getInputStream();
			isr = new InputStreamReader(in,"UTF-8");
			int line =-1;
			while((line = isr.read())!=-1){
				res.append((char)line);
			}
			return res.toString();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			try {
				if(in!=null){
					in.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
			if(isr!=null){
				try {
					isr.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			if(osw!=null){
				try {
					osw.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
		}
		
		return null;
	}
	
	public static String uri = null;
	public static String geturl(){
		System.out.println("geturl");
		if(uri==null){
			Properties p = new Properties();
			InputStream in = null;
			try {
				in = SSOUtil.class.getClassLoader().getResourceAsStream("url.properties");
				p.load(in);
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}finally{
				try {
					in.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			uri=p.getProperty("url");
		}
		return uri;
	}
		public static String encodePassword(String password) {
			String newPwd = MD5Utils.MD5(password);
			String footPwd = newPwd.substring(0, newPwd.length() - 5);
			String headPwd = newPwd.substring(newPwd.length() - 5);
			String encPwd = headPwd + footPwd;
			return encPwd;
		}

		public static void main(String[] args) {
			System.out.println(SSOUtil.encodePassword("liyingcs"));
		}
}
