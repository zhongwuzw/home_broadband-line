package com.test.csv;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import net.sf.json.JSONObject;

import com.csvreader.CsvWriter;

public class InsertDataBylog {

//	private static String srcPath = "/opt/tomcat-saga02/webapps/ctp/terminal/heartbeattrace";
	private static String srcPath = "C:\\Users\\issuser\\Desktop\\云测试平台项目\\log日志样例";     // wuxiaofeng add
	private int count = 0;
	private int fileSize = 0;
	private int maxCount = 30000;
//	private String destPath = "/ctp/terminallog/terminal/test.csv";
//	private String destPathRal = "/ctp/terminallog/terminal/";
	private String destPath = "C:\\Users\\issuser\\Desktop\\云测试平台项目\\test\\test.csv";
	private String destPathRal = "C:\\Users\\issuser\\Desktop\\云测试平台项目\\test\\";     // wuxiaofeng add
	private String destPathFalg = "\\";

	private File csvFile = null;
	private CsvWriter cwriter = null;
	private BufferedWriter writer = null;
	
	public static void main(String[] args) {
		
		try {
			File file = new File(srcPath);
			InsertDataBylog test = new InsertDataBylog();
			test.readAll(file.getAbsolutePath());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	//读取目标路径的文件
	public void readAll(String src) {
		
		Date start = new Date();
		System.out.println("Start:"+start.toLocaleString());
		File file = new File(src);
		if (!file.exists()) {
			file.mkdirs();
		}
		if (file.isFile() && file.getName().endsWith(".log")) {
			read(file.getAbsolutePath(), "utf-8");
//			for (int j = 0; j < list.size(); j++) {
//				String dataStr = "";
//				String [] str = list.get(j);
//				for (int i = 0; i < str.length; i++) {
//					if(i==str.length-1){
//						dataStr = dataStr + str[i]+ "" ;
//					}else{
//						dataStr = dataStr + str[i] +",";					
//					}
//				}
//				write(dataStr);
//			}
		} else {
			File[] fileList = file.listFiles();
			for (int i = 0; i < fileList.length; i++) {
				File sonFile = fileList[i];
				if (sonFile.isDirectory()) {
					readAll(sonFile.getAbsolutePath());
				} else {
					if (!sonFile.getName().endsWith(".log")) {
						continue;
					}
					System.out.println("正在读取 "+sonFile.getParent()+" 文件夹中第"+(i+1)+"个文件，共"+fileList.length+"个文件");
					read(sonFile.getAbsolutePath(),"utf-8");
//					for (int j = 0; j < list.size(); j++) {
//						String dataStr = "";
//						String [] str = list.get(j);
//						for (int k = 0; k < str.length; k++) {
//							if(k==str.length-1){
//								dataStr = dataStr + str[k]+ "" ;
//							}else{
//								dataStr = dataStr + str[k] +",";					
//							}
//						}
//						write(dataStr);
//					}
				}
			}
		}
		Date end = new Date();
		System.out.println("Start:"+start.toLocaleString()+">>> End:"+end.toLocaleString());
	}
	
	private void mkdir(String filePath) {
		
		File file = new File(filePath);
		if(!file.exists()){
			file.mkdirs();
		}
	}
	
	 /** 
     *  读取文件
     * @param filename 目标文件 
     */  
    public void read(String filename, String charset) {
    	
		JSONObject json = new JSONObject();
		List<String[]> resultList = new ArrayList<String[]>();
		String jsonStr = "";
		File file = new File(filename);
		String imei = "";
		imei = file.getName().substring(0, file.getName().indexOf(".log"));

		fileSize = 0;
		
		// wuxiaofeng add begin
		mkdir(destPathRal + imei);
//		File folder = new File(destPathRal + imei);
//		if(!folder.exists()){
//			folder.mkdirs();
//		}
		// wuxiaofeng add end
		
//    	this.open(imei);  // wuxiaofeng zhushi
		
		// String result[] = new String[4];
		BufferedReader rf = null;
		try {
			InputStreamReader inputStreamReader = new InputStreamReader (new FileInputStream(file),charset);
            rf = new BufferedReader(inputStreamReader); 
//			rf = new BufferedReader(new FileReader(file));
			String testTime = "";
			String tempString = null;
			
			// wuxiaofeng add begin
			long count = 0l;
			String path = destPathRal + imei + destPathFalg + "flag-" + imei + ".txt";
            File flagfile = new File(path);
            if (flagfile.isFile() && flagfile.exists()) { 
                InputStreamReader read = new InputStreamReader(new FileInputStream(flagfile), charset); 
                BufferedReader bufferedReader = new BufferedReader(read);
                String lineTxt = null;
                while ((lineTxt = bufferedReader.readLine()) != null) {
                	
                	String[] lineInfo = lineTxt.split(",");
                	if (lineInfo.length == 3) {
                		count = Long.parseLong(lineInfo[1]);
                	}
                }
                read.close();
            }
			
			long flag = 0l;
			// wuxiaofeng add end
			
			System.out.println("===========" + count);
			// 一次读一行，读入null时文件结束
			while ((tempString = rf.readLine()) != null) {
				// wuxiaofeng add begin
				flag += 1;
				if (flag > count) {
				// wuxiaofeng add end
					if(tempString.indexOf("[DEBUG]")!=-1){
						StringBuffer result = new StringBuffer("");
						if (tempString != null && !tempString.equals("") && tempString.indexOf("{")!=-1 && tempString.indexOf("}")!=-1) {
							jsonStr = tempString.toString().substring(tempString.indexOf("{"),
									tempString.indexOf("}") + 1);
						}
						if (jsonStr.equals("")) {
							continue;
						}
						// 获取测试时间
						if (tempString.indexOf("(")!=-1) {
							testTime = tempString.substring(tempString.indexOf("[DEBUG]") + 7, tempString
									.indexOf("("));
						}else{
							continue;
						}
						try{
							json = JSONObject.fromObject(jsonStr);
							if(json==null){
								continue;
							}
						}catch(Exception e){
							e.printStackTrace();
							continue;
						}
						
						String longitude = "";
						String latitude = "";
						if (json.containsKey("latitude")) {
							latitude = json.getString("latitude");
						}
						if (json.containsKey("longitude")) {
							longitude = json.getString("longitude");
						}
						// 地理位置为空查询上一条
						if (latitude == null || latitude.equals("")
								|| longitude == null || longitude.equals("")) {
							continue;
						}
						result.append(testTime + ",");
						result.append(imei + ",");
						if (longitude.equals("")) {
							result.append(longitude + ",");
						} else {
							result.append(transGpsPoint(longitude)[0] + ",");
						}
						if (latitude.equals("")) {
							result.append(latitude + ",");
						} else {
							result.append(transGpsPoint(latitude)[1] + ",");
						}
						Set set = json.keySet();
						Iterator iter = set.iterator();
						while (iter.hasNext()) {
							String key = (String) iter.next();
							String value = json.getString(key);
							result.append(value + ",");
						}
						result = new StringBuffer(result.toString().substring(0,
								result.toString().length() - 1));
						
						// wuxiaofeng add begin
						String timeFolderName = DateUtil.dateToStr(DateUtil.strToDate(testTime, "yyyy-MM-dd"), "yyyyMMdd");
						String timeFolderPath = destPathRal + imei + destPathFalg + timeFolderName;
						mkdir(timeFolderPath);
//						File timeFolder = new File(timeFolderPath);
//						if (!timeFolder.exists()) {
//							timeFolder.mkdirs();
//						}
						String fileName = timeFolderPath + destPathFalg + imei + "-" + timeFolderName + ".csv";
						this.open(fileName);
						write(result.toString(), timeFolderPath);
						this.close();
						// wuxiaofeng add end
						
//						write(result.toString());
					}
				}
			}
			// wuxiaofeng add
		    File flagFile = new File(path);
		    BufferedWriter ow = new BufferedWriter(new FileWriter(flagFile));
		    String s = imei + ".log" + "," + flag + "," + new Date(); //写入内容
		    ow.write(s);
		    ow.close();
		    // wuxiaofeng add
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (rf != null) {
				try {
					rf.close();
				} catch (IOException e1) {
				}
			}
//			this.close();    // wuxiaofeng zhushi
		}
	}
    /**
	 * GPS坐标转换
	 * 
	 * @param meta
	 * @return
	 */
	private String[] transGpsPoint(String meta){

		try{
			String[] result = new String[2];
			String[] info = null;
			if(meta!=null && !"".equals(meta)){
				info = meta.split(" ");
			}else{
				return null;
			}
			
			String latitude = "";
			String longitude = "";
			
			String gpsPoint = "";
			
			for(int i=0; i<info.length&&i<2; i++){
				if(info[i].contains("°")){
					String degrees = info[i].substring(0,info[i].lastIndexOf("°"));
					String minutes = info[i].substring(info[i].lastIndexOf("°")+1,info[i].lastIndexOf("′"));
					String seconds = info[i].substring(info[i].lastIndexOf("′")+1,info[i].lastIndexOf("″"));
					
					//Long gpsLong = Long.parseLong(degrees)+Long.parseLong(minutes)/60+Long.parseLong(seconds)/3600;
					float gpsLong = Float.parseFloat(degrees)+Float.parseFloat(minutes)/60+Float.parseFloat(seconds)/3600;
					
					DecimalFormat decimalFormat=new DecimalFormat(".0000000");
					gpsPoint = decimalFormat.format(gpsLong);
					if(info[i].contains("E")){
						longitude = gpsPoint;
					}else if(info[i].contains("N")){
						latitude = gpsPoint;
					}else if(gpsLong>80.0){
						longitude = gpsPoint;
					}else{
						latitude = gpsPoint;
					}
				}else{
					if(info[i].contains("E")){
						gpsPoint = info[i].substring(0,info[i].lastIndexOf("E"));
						float gpsLong = Float.valueOf(gpsPoint);
						DecimalFormat decimalFormat=new DecimalFormat(".0000000");
						gpsPoint = decimalFormat.format(gpsLong);
						longitude = gpsPoint;
					}else if(info[i].contains("N")){
						gpsPoint = info[i].substring(0,info[i].lastIndexOf("N"));
						float gpsLong = Float.valueOf(gpsPoint);
						DecimalFormat decimalFormat=new DecimalFormat(".0000000");
						gpsPoint = decimalFormat.format(gpsLong);
						latitude = gpsPoint;
					}else{
						gpsPoint = info[i];
						float gpsLong = Float.valueOf(gpsPoint);
						DecimalFormat decimalFormat=new DecimalFormat(".0000000");
						gpsPoint = decimalFormat.format(gpsLong);
						if(gpsLong>80.0){
							longitude = gpsPoint;
						}else{
							latitude = gpsPoint;
						}
					}
				}
			}
			result[0] = longitude;
			result[1] = latitude;
			return result;
		}catch(Exception e){
			e.printStackTrace();
			return new String[]{meta,meta};
		}
		
	}
	
	/**
	 * wuxiaofeng update
	 * @param fileName
	 */
	public void open(String fileName){
		csvFile = new File(fileName);
		try {
			if(!csvFile.exists()){
				csvFile.getParentFile().mkdirs();
				csvFile.createNewFile();
			}
			writer = new BufferedWriter(new FileWriter(csvFile,true));
			cwriter = new CsvWriter(writer,',');
		} catch (FileNotFoundException e){
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
//	public void open(String fileName){
//		csvFile = new File(destPathRal+fileName+".csv");
//		try {
//			if(!csvFile.exists()){
//				csvFile.getParentFile().mkdirs();
//				csvFile.createNewFile();
//			}
//			writer = new BufferedWriter(new FileWriter(csvFile,true));
//			cwriter = new CsvWriter(writer,',');
//		} catch (FileNotFoundException e){
//			e.printStackTrace();
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//	}
	
	/**
	 * wuxiaofeng update
	 * @param dataStr
	 * @param timeFolderPath
	 */
	public void write(String dataStr, String timeFolderPath) {
		File newCsvFile = null;
		if (count >= maxCount) {
			fileSize++;
			count = 0;
			String newPath1 = timeFolderPath + destPathFalg + csvFile.getName().replace(".csv", "")+"."+fileSize+".csv";
			newCsvFile = new File(newPath1);
			if(!newCsvFile.exists()){
				destPath = newPath1;
			}
			while(newCsvFile.exists()){
				fileSize++;
				String newPath = timeFolderPath + destPathFalg + csvFile.getName().replace(".csv", "")+"."+fileSize+".csv";
				newCsvFile = new File(newPath);
				if(!newCsvFile.exists()){
					destPath = newPath;
					System.out.println(destPath);
					break;
				}
			}
			this.close();
			this.open(timeFolderPath + destPathFalg + csvFile.getName().replace(".csv", "")+"."+fileSize +".csv");
		}
		
		try {
            cwriter.writeRecord(dataStr.split(","), true);
            count++;
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
//	public void write(String dataStr){
//		File newCsvFile = null;
//		if(count>=maxCount){
//			fileSize++;
//			count = 0;
//			String newPath1 = destPathRal+csvFile.getName().replace(".csv", "")+"."+fileSize+".csv";
//			newCsvFile = new File(newPath1);
//			if(!newCsvFile.exists()){
//				destPath = newPath1;
//			}
//			while(newCsvFile.exists()){
//				fileSize++;
//				String newPath = destPathRal+csvFile.getName().replace(".csv", "")+"."+fileSize+".csv";
//				newCsvFile = new File(newPath);
//				if(!newCsvFile.exists()){
//					destPath = newPath;
//					System.out.println(destPath);
//					break;
//				}
//			}
//			this.close();
//			this.open(csvFile.getName().replace(".csv", "")+"."+fileSize);
//		}
//		
//		try {
//            cwriter.writeRecord(dataStr.split(","), true);
//            count++;
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//	}
	
	public void close(){
		try{
			if(writer!=null){
				try {
					writer.flush();
					writer.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			if(cwriter!=null){
				cwriter.flush();
				cwriter.close();
			}
		}catch (Exception e){
			e.printStackTrace();
		}
	}
	
}
