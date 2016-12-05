package com.chinamobile.httpserver.data;

import java.io.File;

import com.chinamobile.activemq.consumer.ActivemqConsumer;
import com.chinamobile.activemq.producer.ActivemqProducer;
import com.chinamobile.util.IniReader;

public class GlobalPara {
	static String paraPath = "res/conf/filesystem/uploadpara.ini";
	static String uploadfile = "/OTS/";
	static public String backupPath = "/OTS1/";
	static public String reportZipTemp = "/tmp/";
	
	//ActivemqConsumer
	static public String brokerurlActivemqConsumer = "tcp://192.168.192.128:61616";
	static public String usernameActivemqConsumer = "admin";
	static public String passwordActivemqConsumer = "admin";
	
	//ActivemqProducer
	static public String brokerurlActivemqProducer = "tcp://192.168.192.128:61616";
	static public String usernameActivemqProducer = "admin";
	static public String passwordActivemqProducer = "admin";
	static public int maxconnectionsActivemqProducer = 5;
	static public int maximumactivesessionperconnectionActivemqProducer = 300;
	static public int threadPoolSizeActivemqProducer = 50;
	
	static{
		String mystr = System.getProperty("user.dir");;
		File myFile = new File(mystr+System.getProperty("file.separator")+GlobalPara.paraPath);
		if(myFile.exists()){
			String tempStr = "";
			tempStr = IniReader.getValue("UploadPara", "uploadpath", myFile.getAbsolutePath());
			if(tempStr!=null && !tempStr.equals("")){
				uploadfile = tempStr;
				if(!tempStr.endsWith("\\")&&!tempStr.endsWith("/")){
					uploadfile = tempStr + System.getProperty("file.separator");
				}
			}
			tempStr = IniReader.getValue("UploadPara", "backuppath", myFile.getAbsolutePath());
			if(tempStr!=null && !tempStr.equals("")){
				backupPath = tempStr;
				if(!tempStr.endsWith("\\")&&!tempStr.endsWith("/")){
					backupPath = tempStr + System.getProperty("file.separator");
				}
			}			
			tempStr = IniReader.getValue("DownloadReportTemp", "temppath", myFile.getAbsolutePath());
			if(tempStr!=null && !tempStr.equals("")){
				reportZipTemp = tempStr;
				if(!tempStr.endsWith("\\")&&!tempStr.endsWith("/")){
					reportZipTemp = tempStr + System.getProperty("file.separator");
				}
			}
			tempStr = IniReader.getValue("ActivemqConsumer", "brokerurl", myFile.getAbsolutePath());
			if(tempStr!=null && !tempStr.equals("")){
				brokerurlActivemqConsumer = tempStr;
				ActivemqConsumer.getInstance().setBrokerUrl(brokerurlActivemqConsumer);
			}
			tempStr = IniReader.getValue("ActivemqConsumer", "username", myFile.getAbsolutePath());
			if(tempStr!=null && !tempStr.equals("")){
				usernameActivemqConsumer = tempStr;
				ActivemqConsumer.getInstance().setUserName(usernameActivemqConsumer);
			}
			tempStr = IniReader.getValue("ActivemqConsumer", "password", myFile.getAbsolutePath());
			if(tempStr!=null && !tempStr.equals("")){
				passwordActivemqConsumer = tempStr;
				ActivemqConsumer.getInstance().setPassword(passwordActivemqConsumer);
			}
			
			tempStr = IniReader.getValue("ActivemqProducer", "brokerurl", myFile.getAbsolutePath());
			if(tempStr!=null && !tempStr.equals("")){
				brokerurlActivemqProducer = tempStr;
				ActivemqProducer.getInstance().setBrokerUrl(brokerurlActivemqProducer);
			}
			tempStr = IniReader.getValue("ActivemqProducer", "username", myFile.getAbsolutePath());
			if(tempStr!=null && !tempStr.equals("")){
				usernameActivemqProducer = tempStr;
				ActivemqProducer.getInstance().setUserName(usernameActivemqProducer);
			}
			tempStr = IniReader.getValue("ActivemqProducer", "password", myFile.getAbsolutePath());
			if(tempStr!=null && !tempStr.equals("")){
				passwordActivemqProducer = tempStr;
				ActivemqProducer.getInstance().setPassword(passwordActivemqProducer);
			}
			tempStr = IniReader.getValue("ActivemqProducer", "maxconnections", myFile.getAbsolutePath());
			if(tempStr!=null && !tempStr.equals("")){
				try{
					maxconnectionsActivemqProducer = Integer.valueOf(tempStr);
				}catch (Exception e){
					e.printStackTrace();
				}
				ActivemqProducer.getInstance().setMaxConnections(maxconnectionsActivemqProducer);
			}
			tempStr = IniReader.getValue("ActivemqProducer", "maximumactivesessionperconnection", myFile.getAbsolutePath());
			if(tempStr!=null && !tempStr.equals("")){
				try{
					maximumactivesessionperconnectionActivemqProducer = Integer.valueOf(tempStr);
				}catch (Exception e){
					e.printStackTrace();
				}
				ActivemqProducer.getInstance().setMaximumActiveSessionPerConnection(maximumactivesessionperconnectionActivemqProducer);
			}
			tempStr = IniReader.getValue("ActivemqProducer", "threadPoolSize", myFile.getAbsolutePath());
			if(tempStr!=null && !tempStr.equals("")){
				try{
					threadPoolSizeActivemqProducer = Integer.valueOf(tempStr);
				}catch (Exception e){
					e.printStackTrace();
				}
				ActivemqProducer.getInstance().setThreadPoolSize(threadPoolSizeActivemqProducer);
			}
			
			File uploadDirectory = new File(uploadfile);
			if(!uploadDirectory.exists()){
				uploadDirectory.mkdirs();
			}
			File backupDirectory = new File(backupPath);
			if(!backupDirectory.exists()){
				backupDirectory.mkdirs();
			}
			File tempDirectory = new File(reportZipTemp);
			if(!tempDirectory.exists()){
				tempDirectory.mkdirs();
			}
		}
	}
	public GlobalPara(){
		
	}
	public static void main(String[] args){
//		UploadPara myUlP = new UploadPara();
//		String mystr = System.getProperty("user.dir");
//		File myFile = new File(mystr+System.getProperty("file.separator")+UploadPara.paraPath);
//		if(!myFile.exists()){
//			System.exit(0);
//		}
//		System.out.println("----------"+IniReader.getValue("UploadPara", "uploadpath", UploadPara.paraPath));
		//System.out.println("----------"+IniReader.getValue("UploadPara", "uploadpath", UploadPara.paraPath));
		
//		UploadPara myUlP = new UploadPara();
		
		
		String mystr = System.getProperty("user.dir");;
		File myFile = new File(mystr+System.getProperty("file.separator")+GlobalPara.paraPath);
		if(myFile.exists()){
			String tempStr = "";
			tempStr = IniReader.getValue("UploadPara", "uploadpath", myFile.getAbsolutePath());
			if(tempStr!=null && !tempStr.equals("")){
				uploadfile = tempStr;
				if(!tempStr.endsWith("\\")&&!tempStr.endsWith("/")){
					uploadfile = tempStr + System.getProperty("file.separator");
				}
			}
			tempStr = IniReader.getValue("UploadPara", "backuppath", myFile.getAbsolutePath());
			if(tempStr!=null && !tempStr.equals("")){
				backupPath = tempStr;
				if(!tempStr.endsWith("\\")&&!tempStr.endsWith("/")){
					backupPath = tempStr + System.getProperty("file.separator");
				}
			}
			System.out.println("uploadfile::"+uploadfile);
			System.out.println("backupPath::"+backupPath);
		}
//		String mystr = System.getProperty("user.dir");;
//		File myFile = new File(mystr+System.getProperty("file.separator")+UploadPara.paraPath);
//		if(myFile.exists()){
//			uploadfile = IniReader.getValue("UploadPara", "uploadpath", myFile.getAbsolutePath());
//			if(uploadfile!=null && !uploadfile.equals("")){
//				if(!uploadfile.endsWith("\\")&&!uploadfile.endsWith("/")){
//					uploadfile += System.getProperty("file.separator");
//				}
//			}
//			backupPath = IniReader.getValue("UploadPara", "backuppath", myFile.getAbsolutePath());
//			if(backupPath!=null && !backupPath.equals("")){
//				if(!backupPath.endsWith("\\")&&!backupPath.endsWith("/")){
//					backupPath += System.getProperty("file.separator");
//				}
//			}
//			System.out.println("uploadfile::"+uploadfile);
//			System.out.println("backupPath::"+backupPath);
//		}
	}
}
