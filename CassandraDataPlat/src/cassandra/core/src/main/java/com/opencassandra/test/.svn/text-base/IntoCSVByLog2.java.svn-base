package com.opencassandra.test;

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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.sf.json.JSONObject;

import com.csvreader.CsvWriter;

/**
 * 终端上线数据按IMEI和月拆分成不同文件,文件只有五列：时间，IMEI, 经度，维度，版本
 * @author wuxiaofeng
 *
 */
public class IntoCSVByLog2 {

//	private static String srcPath = "C:\\Users\\issuser\\Desktop\\云测试平台项目\\log日志样例";    
//	private String destPathRal = "C:\\Users\\issuser\\Desktop\\云测试平台项目\\logtest\\";    
//	private String destPathFalg = "\\";
	
	private static String srcPath = "/opt/tomcat-saga02/webapps/ctp/terminal/heartbeattrace";  // 终端上线数据存放路径
	private String destPathRal = "/ctp/terminallog/terminal-wuxf/";   // 按IMEI、月分文件后存放路径
	private String destPathFalg = "/";    // 路径分隔符
	
	// 结果保存Map (Map<date, Map<imei, List<csv文件保存信息字符串>>>)
	private static Map<String, Map<String, List<String>>> resultMap = new HashMap<String, Map<String, List<String>>>();
	
	private static Runtime r = Runtime.getRuntime();  // 程序运行状态类
	private static long total = r.totalMemory();  // Java虚拟机中内存总量

	private File csvFile = null;
	private CsvWriter cwriter = null;
	private BufferedWriter writer = null;
	
	public static void main(String[] args) {
		
		try {
			Date start = new Date();
			System.out.println("Start:"+start.toLocaleString());
			File file = new File(srcPath);
			IntoCSVByLog2 test = new IntoCSVByLog2();
			test.readAll(file.getAbsolutePath());
			
			// 文件读取完毕，将内存中数据写入csv文件中
			for (String date : resultMap.keySet()) {
        		Map<String, List<String>> result = resultMap.get(date);
        		for (String imeiKey : result.keySet()) {
        			String fileName = test.destPathRal + imeiKey + test.destPathFalg + date + test.destPathFalg + imeiKey + "-" + date + ".csv";
        			test.open(fileName);
        			for (String r : result.get(imeiKey)) {
        				test.write(r);
                	}
        		}
        		test.close();
        	}
			resultMap.clear();
			Date end = new Date();
			System.out.println("Start:"+start.toLocaleString()+">>> End:"+end.toLocaleString());
		} catch (Exception e) {
//			e.printStackTrace();
		}
	}
	
	/**
	 * 读取目标路径的文件
	 * @param src 目标路径
	 */
	public void readAll(String src) {
		
		File file = new File(src);
		if (!file.exists()) {
			file.mkdirs();
		}
		if (file.isFile() && file.getName().endsWith(".log")) {
			read(file.getAbsolutePath(), "utf-8");
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
					// 如果文件后缀为.log则读取该文件
					System.out.println("正在读取 " + sonFile.getParent() + " 文件夹中第" + (i + 1) + "个文件" + sonFile.getAbsolutePath() + "，共" + fileList.length + "个文件");
					read(sonFile.getAbsolutePath(), "utf-8");
				}
			}
		}
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
		String jsonStr = "";
		File file = new File(filename);
		// 获取imei，文件名即是，并以imei为名创建文件夹
		String imei = file.getName().substring(0, file.getName().indexOf(".log"));
		mkdir(destPathRal + imei);
		
		BufferedReader rf = null;
		try {
			
			// 如果Java虚拟机中剩余内存量小于总量的30%，则将内存中的数据写入csv文件中
            if (r.freeMemory() <= total * 0.5 ) {
            	for (String date : resultMap.keySet()) {
            		Map<String, List<String>> result = resultMap.get(date);
            		for (String imeiKey : result.keySet()) {
            			String fileName = destPathRal + imeiKey + destPathFalg + date + destPathFalg + imeiKey + "-" + date + ".csv";
                    	this.open(fileName);
                    	for (String r : result.get(imeiKey)) {
                    		this.write(r);
                    	}
            		}
    				this.close();
            	}
            	resultMap.clear();
    		}
            
            // 如果该文件的标志文件存在，则读取获得该文件上次读到第几行
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
            
            // 开始读取文件
            InputStreamReader inputStreamReader = new InputStreamReader (new FileInputStream(file),charset);
            rf = new BufferedReader(inputStreamReader); 
			String testTime = "";       // 测试时间
			String tempString = null;   // 保存每行数据
			long flag = 0l;  // 读取文件行标识
			
			while ((tempString = rf.readLine()) != null) {
				flag += 1;
				if (flag > count) { // 如果行数大于上次读到的行数，则处理该行日志
					if (tempString.indexOf("[DEBUG]") != -1) {
						StringBuffer result = new StringBuffer("");
						// 获取日志中的json串儿
						if (tempString != null && !tempString.equals("") && tempString.indexOf("{") != -1 && tempString.indexOf("}") != -1) {
							jsonStr = tempString.toString().substring(tempString.indexOf("{"),
									tempString.indexOf("}") + 1);
						}
						if (jsonStr.equals("")) {
							continue;
						}
						
						try {
							json = JSONObject.fromObject(jsonStr);
							if (json == null) {
								continue;
							}
						} catch (Exception e) {
							continue;
						}
						
						// 获取测试时间
						if (tempString.indexOf("(") != -1) {
							testTime = tempString.substring(tempString.indexOf("[DEBUG]") + 7, tempString.indexOf("("));
						} else {
							continue;
						}
						
						// 获取地理位置
						String longitude = "";
						String latitude = "";
						if (json.containsKey("latitude")) {
							latitude = json.getString("latitude");
						}
						if (json.containsKey("longitude")) {
							longitude = json.getString("longitude");
						}
						// 地理位置为空查询上一条
						if (latitude == null || latitude.equals("")	|| longitude == null || longitude.equals("")) {
							continue;
						}
						
						// 将测试时间和imei追加到result字符串中
						result.append(testTime + ",");
						result.append(imei + ",");
						
						// 将经纬度信息追加到result字符串中，经纬度如果不为空字符串，则通过GPS转换方法转换
						if (longitude.equals("")) {
							result.append(longitude + ",");
						} else {
							if (transGpsPoint(longitude) == null) {
								continue;
							}
							result.append(transGpsPoint(longitude)[0] + ",");
						}
						if (latitude.equals("")) {
							result.append(latitude + ",");
						} else {
							if (transGpsPoint(latitude) == null) {
								continue;
							}
							result.append(transGpsPoint(latitude)[1] + ",");
						}
						
						String toolSversion = "";
						if (json.containsKey("toolsversion")) {
							toolSversion = json.getString("toolsversion");
						}
						
						result.append(toolSversion);
						
						// 将测试时间转换成月格式，在imei文件夹中创建月文件夹
						String timeFolderName = DateUtil.dateToStr(DateUtil.strToDate(testTime, "yyyy-MM"), "yyyyMM");
						String timeFolderPath = destPathRal + imei + destPathFalg + timeFolderName;
						mkdir(timeFolderPath);
						
						// 将数据保存到内存中
						List<String> list = new ArrayList<String>();
						list.add(result.toString());
						if (resultMap.get(timeFolderName) == null) {
							Map<String, List<String>> tmpMap = new HashMap<String, List<String>>();
							tmpMap.put(imei, list);
							resultMap.put(timeFolderName, tmpMap);
						} else {
							Map<String, List<String>> tmpMap = resultMap.get(timeFolderName);
							if (tmpMap.get(imei) == null) {
								tmpMap.put(imei, list);
							} else {
								tmpMap.get(imei).add(result.toString());
							}
							resultMap.put(timeFolderName, tmpMap);
						}
					}
				}
			}
			// 写入文件读取行标识
		    File flagFile = new File(path);
		    BufferedWriter ow = new BufferedWriter(new FileWriter(flagFile));
		    String s = imei + ".log" + "," + flag + "," + new Date(); 
		    ow.write(s);
		    ow.close();
			
		} catch (Exception e) {
			System.out.println("出错了：" + e.toString());
//			e.printStackTrace();
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
	private String[] transGpsPoint(String meta) {

		try {
			String[] result = new String[2];
			String[] info = null;
			if (meta != null && !"".equals(meta)) {
				info = meta.split(" ");
			} else {
				return null;
			}
			
			String latitude = "";
			String longitude = "";
			
			String gpsPoint = "";
			//116°20′35.9″E"
			for (int i = 0; i < info.length && i < 2; i++) {
				if (info[i].contains("°")) {
					String degrees = info[i].substring(0, info[i].lastIndexOf("°"));
					String minutes = info[i].substring(info[i].lastIndexOf("°") + 1,info[i].lastIndexOf("′"));
					String seconds = info[i].substring(info[i].lastIndexOf("′") + 1,info[i].lastIndexOf("″"));
					
					//Long gpsLong = Long.parseLong(degrees)+Long.parseLong(minutes)/60+Long.parseLong(seconds)/3600;
					float gpsLong = Float.parseFloat(degrees) + Float.parseFloat(minutes) / 60 + Float.parseFloat(seconds)/3600;
					
					DecimalFormat decimalFormat=new DecimalFormat(".0000000");
					gpsPoint = decimalFormat.format(gpsLong);
					if (info[i].contains("E")) {
						longitude = gpsPoint;
					} else if (info[i].contains("N")) {
						latitude = gpsPoint;
					} else if (gpsLong > 80.0 ){
						longitude = gpsPoint;
					} else {
						latitude = gpsPoint;
					}
				} else {
					if (info[i].contains("E")) {
						gpsPoint = info[i].substring(0, info[i].lastIndexOf("E"));
						float gpsLong = Float.valueOf(gpsPoint);
						DecimalFormat decimalFormat=new DecimalFormat(".0000000");
						gpsPoint = decimalFormat.format(gpsLong);
						longitude = gpsPoint;
					} else if (info[i].contains("N")) {
						gpsPoint = info[i].substring(0, info[i].lastIndexOf("N"));
						float gpsLong = Float.valueOf(gpsPoint);
						DecimalFormat decimalFormat=new DecimalFormat(".0000000");
						gpsPoint = decimalFormat.format(gpsLong);
						latitude = gpsPoint;
					} else {
						gpsPoint = info[i];
						float gpsLong = Float.valueOf(gpsPoint);
						DecimalFormat decimalFormat=new DecimalFormat(".0000000");
						gpsPoint = decimalFormat.format(gpsLong);
						if (gpsLong > 80.0) {
							longitude = gpsPoint;
						} else {
							latitude = gpsPoint;
						}
					}
				}
			}
			result[0] = longitude;
			result[1] = latitude;
			return result;
		} catch(Exception e) {
//			e.printStackTrace();
//			return new String[]{meta, meta};
			return null;
		}
		
	}
	
	public void open(String fileName) {
		
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
	
	public void write(String dataStr) {
		
		try {
            cwriter.writeRecord(dataStr.split(","), true);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void close() {
		
		try {
			if (writer != null) {
				try {
					writer.flush();
					writer.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			if (cwriter != null) {
				cwriter.flush();
				cwriter.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}