package com.chinamobile.util;

import java.io.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.log4j.Logger;

public class MD5Builder {
	private static MD5Builder instance = null;
	private static String MD5_AUTH_KEY = "liugang";
	
	public static String getMD5_AUTH_KEY() {
		return MD5_AUTH_KEY;
	}

	public static void setMD5_AUTH_KEY(String mD5_AUTH_KEY) {
		MD5_AUTH_KEY = mD5_AUTH_KEY;
	}

	public static MD5Builder getInstance()
	{
		if(instance == null)
		{
			return new MD5Builder();
		}
		return instance;
	}
	
	static Logger logger = Logger.getLogger(MD5Builder.class);
	
	//对文件全文生成MD5摘要
	public static String getMD5(File file)throws NoSuchAlgorithmException
	{
		FileInputStream fis = null;
		try
		{
			MessageDigest md = MessageDigest.getInstance("MD5");
			
			logger.info("MD5摘要长度： "+md.getDigestLength());
			fis = new FileInputStream(file);
			byte[] buffer = new byte[2048];
			int numRead = 0;
			logger.info("开始生成摘要");
			while((numRead = fis.read(buffer))>=0)
			{
				md.update(buffer,0,numRead);
			}
			logger.info("摘要生成成功！");
			
			
			byte[] b = md.digest();
			
			return byteToHexStringSingle(b);
		}
		catch(Exception e)
		{
			logger.error(e);
			e.printStackTrace();
			return null;
		}
		finally
		{
			try{
				fis.close();
			}
			catch(Exception e)
			{
				e.printStackTrace();
			}
		}
		
	}
	
	//对字符串生成MD5摘要
		public static String getMD5(StringBuffer value)throws NoSuchAlgorithmException
		{
			logger.info(value.toString());
			return getMD5(value.toString());
		}
	
	//对字符串生成MD5摘要
	public static String getMD5(String value)throws NoSuchAlgorithmException
	{
		try
		{
			MessageDigest md = MessageDigest.getInstance("MD5");
			
			logger.info("MD5摘要长度： "+md.getDigestLength());
			byte[] buffer = null;
			if(value!=null){
				buffer = value.getBytes("UTF-8");
			}else{
				logger.warn("转换字符为空！");
				return "";
			}
			logger.info("开始生成摘要");
			md.update(buffer);
			logger.info("摘要生成成功！");
			
			byte[] b = md.digest();
			
			return byteToHexStringSingle(b);
		}
		catch(Exception e)
		{
			logger.error(e);
			e.printStackTrace();
			return null;
		}		
	}
	
	public static String byteToHexStringSingle(byte[] byteArray)
	{
		 StringBuffer md5StrBuff = new StringBuffer();  
		 for (int i = 0; i < byteArray.length; i++) {
			 
	         if (Integer.toHexString(0xFF & byteArray[i]).length() == 1)  
	                md5StrBuff.append("0").append(  
	                        Integer.toHexString(0xFF & byteArray[i]));  
	            else  
	                md5StrBuff.append(Integer.toHexString(0xFF & byteArray[i]));  
	        }  
	  
	        return md5StrBuff.toString();  
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try
		{
			File file = new File("c://remove.log");
			String code = getMD5(file);
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		

	}

}
