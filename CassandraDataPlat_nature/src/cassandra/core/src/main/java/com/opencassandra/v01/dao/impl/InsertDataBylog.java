package com.opencassandra.v01.dao.impl;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
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

	private static String srcPath = "C:\\Users\\issuser\\Desktop\\terminalheart";
	private int count = 0;
	private int fileSize = 0;
	private int maxCount = 30000;
	private String destPath = "C:\\Users\\issuser\\Desktop\\terminalheart\\test.csv";
	public static void main(String[] args){
		try {
			File file = new File(srcPath);
			InsertDataBylog test = new InsertDataBylog();
			test.readAll(file.getAbsolutePath());
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	//读取目标路径的文件
	public void readAll(String src){
		Date start = new Date();
		System.out.println("Start:"+start.toLocaleString());
		File file = new File(src);
		if(!file.exists()){
			file.mkdirs();
		}
		if(file.isFile() && file.getName().endsWith(".log")){
			read(file.getAbsolutePath(),"utf-8");
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
		}else
		{
			File[] fileList = file.listFiles();
			for (int i = 0; i < fileList.length; i++) {
				File sonFile = fileList[i];
				if(sonFile.isDirectory()){
					readAll(sonFile.getAbsolutePath());
				}else{
					if(!sonFile.getName().endsWith(".log")){
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

		// String result[] = new String[4];
		BufferedReader rf = null;
		try {
			InputStreamReader inputStreamReader = new InputStreamReader (new FileInputStream(file),charset);
            rf = new BufferedReader(inputStreamReader); 
//			rf = new BufferedReader(new FileReader(file));
			String testTime = "";
			String tempString = null;
			// 一次读一行，读入null时文件结束
			while ((tempString = rf.readLine()) != null) {
				StringBuffer result = new StringBuffer("");
				if (tempString != null && !tempString.equals("")) {
					jsonStr = tempString.toString().substring(tempString.indexOf("{"),
							tempString.indexOf("}") + 1);
				}
				if (jsonStr.equals("")) {
					continue;
				}
				// 获取测试时间
				if (testTime.equals(tempString.substring(tempString.indexOf("[DEBUG]") + 7,
						tempString.indexOf("(")))) {
					continue;
				}
				testTime = tempString.substring(tempString.indexOf("[DEBUG]") + 7, tempString
						.indexOf("("));
				json = JSONObject.fromObject(jsonStr);
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
				write(result.toString());
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (rf != null) {
				try {
					rf.close();
				} catch (IOException e1) {
				}
			}
		}
	}
    /**
	 * GPS坐标转换
	 * 
	 * @param meta
	 * @return
	 */
	private String[] transGpsPoint(String meta){

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
	}
	
	public void write(String dataStr){
		File csvFile = null;
		if(count>=maxCount){
			count = 0;
			String newPath1 = destPath.replace(".csv", fileSize+++".csv");
			csvFile = new File(newPath1);
			if(!csvFile.exists()){
				destPath = newPath1;
			}
			while(csvFile.exists()){
				String newPath = destPath.replace(".csv", fileSize+++".csv");
				csvFile = new File(newPath);
				if(!csvFile.exists()){
					destPath = newPath;
					System.out.println(destPath);
					break;
				}
			}
		}else{
			csvFile = new File(destPath);
		}
		BufferedWriter writer = null;
		CsvWriter cwriter = null;
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
		try {
            cwriter.writeRecord(dataStr.split(","), true);
            count = count +1;
		} catch (IOException e) {
			e.printStackTrace();
		}finally{
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
		}
	}
}
