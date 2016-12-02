package com.opencassandra.v01.dao.impl;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;
import com.opencassandra.descfile.ConfParser;
import net.sf.json.JSONObject;

public class TestDataAnalyzeMonitor {
	JSONObject requestBody = null;
	private String[] dateStr = new String[] { "yyyy", "MM", "dd", "HH", "mm", "ss", "sss" };
	public TestDataAnalyzeMonitor(JSONObject requestBody){
		this.requestBody = requestBody;
	} 
	public TestDataAnalyzeMonitor(){
	} 
	
	
	/**
	 * 读取CSV文件
	 */
	public ArrayList<String[]> readCsv(File filePath) {
		ArrayList<String[]> csvList = null;
		try {
			csvList = new ArrayList<String[]>(); // 用来保存数据
			String csvFilePath = filePath.getAbsolutePath();
			CsvReader reader = new CsvReader(csvFilePath, ',',
					Charset.forName("GB2312")); // 一般用这编码读就可以了

			// reader.readHeaders(); // 跳过表头 如果需要表头的话，不要写这句。

			while (reader.readRecord()) { // 逐行读入除表头的数据
				String[] values = null;
				values = reader.getValues();
				csvList.add(values);
			}
			reader.close();

		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {

		}
		return csvList;
	}
	/**
	 * 判断CSV文件中某一行是不是空行
	 * 
	 * @param values
	 * @return
	 */
	public boolean isBlankLine(String[] values) {
		boolean isBlank = true;
		if (values != null) {
			for (int i = 0; i < values.length; i++) {
				if (values[i] != null && !"".equals(values[i])) {
					isBlank = false;
				}
			}
		}
		return isBlank;
	}
	public String subTxt(String str) {
		if (str.startsWith("{")) {
			str = str.substring(1, str.length());
		}
		if (str.endsWith("}")) {
			str = str.substring(0, str.length() - 1);
		}
		return str;
	}
	private String convertFileSize(long fileS) {
		DecimalFormat df = new DecimalFormat("#.00");
		String fileSizeString = "";
		if (fileS < 1024) {
			fileSizeString = df.format((double) fileS) + "B";
		} else if (fileS < 1048576) {
			fileSizeString = df.format((double) fileS / 1024) + "K";
		} else if (fileS < 1073741824) {
			fileSizeString = df.format((double) fileS / 1048576) + "M";
		} else {
			fileSizeString = df.format((double) fileS / 1073741824) + "G";
		}
		return fileSizeString;
	}
	private String getLastModifies(File file){
		long modifiedTime = file.lastModified();
		Date date = new Date(modifiedTime);
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:MM");
		String dd = sdf.format(date);
		return dd;
	}
	public void test(){
		String src = ConfParser.srcPath;
		File file = new File(src);
		if(!file.exists()){
			file.mkdirs();
		}
		File[] fileList = file.listFiles();
		for (int i = 0; i < fileList.length; i++) {
			File sonFile = fileList[i];
			if(sonFile.isDirectory()){
				getAllDataByDir(sonFile);
			}else{
				if(sonFile.getName().endsWith(".monitor.csv")){
					getAllDataByFile(sonFile.getAbsoluteFile());	
				}
			}
		}
	}
	public void getAllDataByDir(File file){
		if(file.exists()&&file.isDirectory()){
			File[] fileList = file.listFiles();
			for (int i = 0; i < fileList.length; i++) {
				File sonFile = fileList[i];
				if(sonFile.isDirectory()){
					getAllDataByDir(sonFile.getAbsoluteFile());
				}else{
					if(sonFile.getName().endsWith(".monitor.csv")){
						getAllDataByFile(sonFile.getAbsoluteFile());	
					}
				}
			}
		}
	}
	public void getAllDataByFile(File file){
		try {
			ArrayList<String[]> mosHeader = new ArrayList<String[]>();
			ArrayList<String[]> mosBody = new ArrayList<String[]>();
			ArrayList<String[]> mosApp = new ArrayList<String[]>();
			ArrayList<String[]> mosEle = new ArrayList<String[]>();
			ArrayList<String[]> csvList = this.readCsv(file);
			String datetime = "";
			int dataEreaIndex = 0;
			for (int i = 0; i < csvList.size(); i++) {
				if (csvList.get(i) != null && !isBlankLine(csvList.get(i))) {
					if (csvList.get(i)[0] != null) {
						if ((csvList.get(0)[0].contains("时间") || csvList
								.get(0)[0].equals("测试时间"))
								&& csvList.get(0).length > 1
								&& csvList.get(0)[1] != null) {
							datetime = csvList.get(0)[1];
						}
						if (csvList.get(i)[0].equals("时间")
								&& csvList.size() > i + 1
								&& csvList.get(i + 1) != null
								&& csvList.get(i + 1).length >= 1
								&& csvList.get(i + 1)[0] != null
								&& csvList.get(i + 1)[0].contains("型号")) {

						} else if ((csvList.get(i)[0].equals("时间")
								|| csvList.get(i)[0].equals("测试时间"))
								&& csvList.get(i).length > 2
								&& (csvList.size() == (i + 1) 
										|| csvList.get(i + 1) != null
										&& csvList.get(i + 1).length > 2)
								&& csvList.get(i)[2]!=null && !csvList.get(i)[2].trim().equals("")) {
							
							dataEreaIndex = 1;
						} else if (csvList.get(i)[0].contains("应用名称")) {
							dataEreaIndex = 2;
						} else if (csvList.get(i)[0].contains("本次电量总消耗")){
							dataEreaIndex = 3;
						}
					}
					if (dataEreaIndex == 0) {
						mosHeader.add(csvList.get(i));
					} else if (dataEreaIndex == 1) {
						mosBody.add(csvList.get(i));
					} else if (dataEreaIndex == 2) {
						mosApp.add(csvList.get(i));
					} else if (dataEreaIndex == 3) {
						mosEle.add(csvList.get(i));
					}
				} else {
					dataEreaIndex++;
				}
			}
			// 获取测试类型Num
			Pattern pattern1 = Pattern.compile("[-_.]");
			String numStrs[] = pattern1.split(file.getName());

			// 要生成的TXT的文件内容
			StringBuffer str = new StringBuffer();
			// 头部Map
			Map mosheaderMap = new HashMap();
			// 主体Map
			Map mosbodyMap = new HashMap();
			// 应用名称Map
			Map mosAppMap = new HashMap();
			// 电池消耗
			Map mosEleMap = new HashMap();
			// columnFamily
			Map cfNameMap = new HashMap();
			String upIndex = "signal_Strength_" ; 
			if (mosBody != null && mosBody.size() > 0) {
				String[] key = mosBody.get(0);// 主体的表头
				if (key != null) {
					for (int i = 1; i < mosBody.size(); i++) {// 主体的数据
						try{					// 第i排数据，i从1开始
							String[] value = mosBody.get(i);
							if (!isBlankLine(value)) {
								Map map = new HashMap();
								boolean flag = true;
								String data_time = "";
								for (int j = 0; j < key.length; j++) {
									String sonKey = key[j];
									String sonValue = "";
									try {
										sonValue = value[j];	
									} catch (Exception e) {
										sonValue = "";
									}
									if(key[0].equals("时间")||key[0].equals("测试时间")){
										data_time = value[0];
									}
									map.put(sonKey, sonValue);
								}
								String dataLong = "";
								if(data_time!=null && !data_time.trim().isEmpty() && flag)
								{
									dataLong = getFormatTime(data_time);
									map.put("data_time", dataLong);
									flag = false;
								}else{
									dataLong = getFormatTime(datetime);
									map.put("data_time", dataLong);
									flag = false;
								}
								if(map.get("data_time")==null || map.get("data_time").toString().trim().equals("")){
									dataLong = getFormatTime(new Date().toLocaleString());
									map.put("data_time", dataLong);
								}
								mosbodyMap.put(i, map);
							}
						} catch (ArrayIndexOutOfBoundsException e) {
							mosbodyMap.put(key, "");
							e.printStackTrace();
						} catch (Exception e) {
							mosbodyMap.put(key, "");
							e.printStackTrace();
						}
					}
				}
			}
			for (int i = 0; i < mosHeader.size(); i++) {
				String key = mosHeader.get(i)[0];
				try {
					String value = "";
					if(mosHeader.get(i).length>=2){
						value = mosHeader.get(i)[1];
					}
					mosheaderMap.put(key, value);
				} catch (ArrayIndexOutOfBoundsException e) {
					mosheaderMap.put(key, "");
					e.printStackTrace();
				} catch (Exception e) {
					mosheaderMap.put(key, "");
					e.printStackTrace();
				}
			}
			mosheaderMap.put("测试类型Num", numStrs[1]);
			for (int i = 0; i < mosApp.size(); i++) {
				String key = mosApp.get(i)[0];
				try {
					if (key != null) {
						String value = "";
						if (i != 0) {
							if(mosApp.get(i).length>=2){
								value = mosApp.get(i)[1];	
							}
						}
						if (key.equals("")
								&& mosApp.get(i + 1).length >= 2
								&& mosApp.get(i + 1)[0] != null) {
							mosAppMap.put(key, value);
						} else if (!key.equals("")) {
							mosAppMap.put(key, value);
						}
					}
				} catch (ArrayIndexOutOfBoundsException e) {
					mosAppMap.put(key, "");
					e.printStackTrace();
				} catch (Exception e) {
					mosAppMap.put(key, "");
					e.printStackTrace();
				}
			}
			if(mosEle!=null && mosEle.size()>0 && mosEle.get(0)!=null)
			{
				String ele = mosEle.get(0).toString();
				if(ele.contains(":")){
					mosEleMap.put(ele.split(":")[0], ele.split(":")[1]);
				}
			}
			String prefix = ConfParser.org_prefix;
			String org = file.getAbsolutePath().replace(prefix, "");
			org = org.substring(0, org.indexOf(File.separator));
//			System.out.println("keyspace:" + org);
			String imei = file.getParentFile().getName();
			String filePath = file.getAbsolutePath().replace(ConfParser.org_prefix.replace(File.separator, "|"),"");
			String fileSize = convertFileSize(file.length());
			String file_lastModifies = getLastModifies(file);
			String fileName = file.getName();
			Map allMap = new HashMap();
			if (mosbodyMap != null) {
				if (mosbodyMap.size() == 1) {
					Map bodyNextMap = (Map) mosbodyMap.get(1);
					String dataLong = (String)bodyNextMap.get("data_time");
					String netWorkType1 = "";
					String netWorkType2 = "";
					if (bodyNextMap.containsKey("网络(2)网络制式") && bodyNextMap.containsKey("网络(1)网络制式"))
					{
						netWorkType1 = (String)bodyNextMap.get("网络(1)网络制式");
						netWorkType2 = (String)bodyNextMap.get("网络(2)网络制式");
					} else if (bodyNextMap.containsKey("网络(2)网络制式")){
						netWorkType1 = (String)bodyNextMap.get("网络(2)网络制式");
					} else if (bodyNextMap.containsKey("网络(1)网络制式")){
						netWorkType1 = (String)bodyNextMap.get("网络(1)网络制式");
					} else if (bodyNextMap.containsKey("网络制式")){
						netWorkType1 = (String)bodyNextMap.get("网络制式");
					} else if (bodyNextMap.containsKey("网络类型")){
						netWorkType1 = (String)bodyNextMap.get("网络类型");
					}
					if(!netWorkType1.trim().equals("")){
						netWorkType1 = replaceChar(netWorkType1);
						String fileIndex = imei+"|"+file.getName()+ "_" + 0 +"_"+0;
						String columnFamily = "signal_strength_"+netWorkType1;
						allMap = setMapData(mosheaderMap, allMap, file,
								fileIndex,dataLong, imei);
						allMap = setMapData(mosAppMap, allMap, file,
								fileIndex,dataLong, imei);
						allMap = setMapData(bodyNextMap, allMap, file,
								fileIndex,dataLong, imei);
						allMap.put("fileName", fileName);
						allMap.put("filePath", filePath);
						allMap.put("fileSize", fileSize);
						allMap.put("file_lastModifies", file_lastModifies);
						this.getAllDataByCsv(allMap, columnFamily, org, fileIndex);
//						insertCassandraDB(allMap, columnFamily, file, org,0);
					}
					if(!netWorkType2.trim().equals("")){
						netWorkType2 = replaceChar(netWorkType2);
						String fileIndex = imei+"|"+file.getName()+ "_" + 0 +"_"+1;
						String columnFamily = "signal_strength_"+netWorkType1;
						allMap = setMapData(mosheaderMap, allMap, file,
								fileIndex,dataLong, imei);
						allMap = setMapData(mosAppMap, allMap, file,
								fileIndex,dataLong, imei);
						allMap = setMapData(bodyNextMap, allMap, file,
								fileIndex,dataLong, imei);
						allMap.put("fileName", fileName);
						allMap.put("filePath", filePath);
						allMap.put("fileSize", fileSize);
						allMap.put("file_lastModifies", file_lastModifies);
						this.getAllDataByCsv(allMap, columnFamily, org, fileIndex);
//						insertCassandraDB(allMap, columnFamily, file, org,0);
					}
				} else if (mosbodyMap.size() > 1) {
					for (int i = 0; i < mosbodyMap.size(); i++) {
						Map bodyNextMap = (Map) mosbodyMap.get(1);
						String dataLong = (String)bodyNextMap.get("data_time");
						String netWorkType1 = "";
						String netWorkType2 = "";
						if (bodyNextMap.containsKey("网络(2)网络制式") && bodyNextMap.containsKey("网络(1)网络制式"))
						{
							netWorkType1 = (String)bodyNextMap.get("网络(1)网络制式");
							netWorkType2 = (String)bodyNextMap.get("网络(2)网络制式");
						} else if (bodyNextMap.containsKey("网络(2)网络制式")){
							netWorkType1 = (String)bodyNextMap.get("网络(2)网络制式");
						} else if (bodyNextMap.containsKey("网络(1)网络制式")){
							netWorkType1 = (String)bodyNextMap.get("网络(1)网络制式");
						} else if (bodyNextMap.containsKey("网络制式")){
							netWorkType1 = (String)bodyNextMap.get("网络制式");
						} else if (bodyNextMap.containsKey("网络类型")){
							netWorkType1 = (String)bodyNextMap.get("网络类型");
						}
						if(!netWorkType1.trim().equals("")){
							netWorkType1 = replaceChar(netWorkType1);
							String fileIndex = imei+"|"+file.getName()+ "_" + i +"_"+0;
							String columnFamily = "signal_strength_"+netWorkType1;
							allMap = setMapData(mosheaderMap, allMap, file,
									fileIndex,dataLong, imei);
							allMap = setMapData(mosAppMap, allMap, file,
									fileIndex,dataLong, imei);
							allMap = setMapData(bodyNextMap, allMap, file,
									fileIndex,dataLong, imei);
							allMap.put("fileName", fileName);
							allMap.put("filePath", filePath);
							allMap.put("fileSize", fileSize);
							allMap.put("file_lastModifies", file_lastModifies);
							this.getAllDataByCsv(allMap, columnFamily, org, fileIndex);
//							insertCassandraDB(allMap, columnFamily, file, org,0);
						}
						if(!netWorkType2.trim().equals("")){
							netWorkType2 = replaceChar(netWorkType2);
							String fileIndex = imei+"|"+file.getName()+ "_" + i +"_"+1;
							String columnFamily = "signal_strength_"+netWorkType2;
							allMap = setMapData(mosheaderMap, allMap, file,
									fileIndex,dataLong, imei);
							allMap = setMapData(mosAppMap, allMap, file,
									fileIndex,dataLong, imei);
							allMap = setMapData(bodyNextMap, allMap, file,
									fileIndex,dataLong, imei);
							allMap.put("fileName", fileName);
							allMap.put("filePath", filePath);
							allMap.put("fileSize", fileSize);
							allMap.put("file_lastModifies", file_lastModifies);
							this.getAllDataByCsv(allMap, columnFamily, org, fileIndex);
//							insertCassandraDB(allMap, columnFamily, file, org,0);
						}
					}
				}else{
					String fileIndex = imei+"|"+file.getName()+ "_" + 0+"_"+0;
					String columnFamily = "signal_strength_NA";
					String dataLong = "";
					if(mosheaderMap!=null && mosheaderMap.size()>0){
						dataLong = (String)mosheaderMap.get("data_time");
					}
					if(dataLong.isEmpty()){
						dataLong = getFormatTime(new Date().toLocaleString());
					}
					allMap = setMapData(mosheaderMap, allMap, file, fileIndex, dataLong, imei);
					allMap = setMapData(mosAppMap, allMap, file, fileIndex, dataLong, imei);
					allMap.put("fileName", fileName);
					allMap.put("filePath", filePath);
					allMap.put("fileSize", fileSize);
					allMap.put("file_lastModifies", file_lastModifies);
					this.getAllDataByCsv(allMap, columnFamily, org, fileIndex);
//					insertCassandraDB(allMap, columnFamily, file, org,0);
				}
			} else {
				String fileIndex = file.getAbsolutePath().replace(File.separator,
				"|")
				+ "_" + 0+"_"+0;
				String columnFamily = "signal_strength_NA";
				
				String dataLong = "";
				if(mosheaderMap!=null && mosheaderMap.size()>0){
					dataLong = (String)mosheaderMap.get("data_time");
				}
				if(dataLong.isEmpty()){
					dataLong = getFormatTime(new Date().toLocaleString());
				}
				allMap = setMapData(mosheaderMap, allMap, file, fileIndex, dataLong, imei);
				allMap = setMapData(mosAppMap, allMap, file,fileIndex, dataLong, imei);
				allMap.put("fileName", fileName);
				allMap.put("filePath", filePath);
				allMap.put("fileSize", fileSize);
				allMap.put("file_lastModifies", file_lastModifies);
				this.getAllDataByCsv(allMap, columnFamily, org, fileIndex);
//				insertCassandraDB(allMap, columnFamily, file, org,0);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	//替换创建columnFamily时会报错的字符
	private static String replaceChar(String str){
		str = str.replace("/", "");
		str = str.replace(",", "");
		str = str.replace("\\", "");
		str = str.replace("，", "");
		str = str.replace("-", "");
		str = str.replace(" ", "");
		return str;
	}
	public String getFormatTime(String datetime){
		//格式化时间
		Pattern pattern2 = Pattern.compile("\\d+");
		String dataLong = "";
		if (datetime.isEmpty()) {
			dataLong = Long.toString(new Date().getTime());
		} else {
			String timeStrs[] = pattern2.split(datetime);
			StringBuffer rex = new StringBuffer("");
			for (int i = 0; i < timeStrs.length; i++) {
				rex.append(timeStrs[i]);
				rex.append(dateStr[i]);
			}
			Date date1 = null;
			SimpleDateFormat formatter = new SimpleDateFormat(
					rex.toString());
			try {
				date1 = formatter.parse(datetime);
			} catch (ParseException e) {
				e.printStackTrace();
			}
			dataLong = Long.toString(date1.getTime());
		}
		return dataLong;
	}
	private Map setMapData(Map DataMap, Map allMap, File file,
			String fileIndex,String dataLong, String imei) {
		Set set = DataMap.keySet();
		Iterator iter = set.iterator();
		while (iter.hasNext()) {
			String key = (String) iter.next();
			String value = "";
			if (key == null || key.isEmpty()) {
				key = "";
			}
			try {
				value = (String) DataMap.get(key);
			} catch (Exception e) {
			}
			allMap.put(key, value);
		}
		String prefix = ConfParser.org_prefix.replace(File.separator, "|");
		fileIndex = fileIndex.replace(prefix, "");
		Date date = new Date(Long.valueOf(dataLong));
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		String dateStr = sdf.format(date);
		System.out.println("fileIndex:"+fileIndex+",dataLong:"+dataLong+",yearMonthDay:"+dateStr+",imei:"+imei);
		allMap.put("file_index", fileIndex);
		allMap.put("data_time", dataLong);
		allMap.put("year_month_day", dateStr);
		allMap.put("imei", imei);
		return allMap;
	}
	public static void main(String[] args) {
		try {
			SimpleDateFormat dateFormat = new SimpleDateFormat(
			"yyyy-MM-dd HH:mm:ss");
			Date start = new Date();
			String dateString = dateFormat.format(start);
			System.out.println(dateString + ">>> Start!");
			TestDataAnalyzeMonitor test = new TestDataAnalyzeMonitor();
			test.test();
			Date end = new Date();
			dateString = dateFormat.format(end);
			System.out.println("Start: "+dateString+",  End :  "+dateString);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	public void getAllDataByCsv(Map map,String fileType,String keySpace,String fileIndex){
		HashMap<String, String> myData = new HashMap<String, String>();
		List<Map<String, String>> columns = new ArrayList<Map<String, String>>() ;
		Set key = map.keySet();
		Iterator iter = key.iterator();
		while(iter.hasNext()){
			Map<String, String> hColumn = new HashMap<String,String>();
			try {
				String name = (String)iter.next();
				String value = (String)map.get(name);
				if(value!=null){
					hColumn.put("name",name);
					hColumn.put("value", value);
					columns.add(hColumn);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		int previGps = 0;
		int previNetType = 0;
		String gpsStr = "";
		String NetType1 = "";
		String NetType2 = "";
		String NetType = "";
		String signalStength1 = "";
		String signalStength2 = "";
		String signalStength = "";
		String sinr = "";
		
		//SIGNAL
		
		//SPEEDTEST
		String downloadSpeed = "";
		String uploadSpeed = "";
		String dlSpeedUnit = "";
		String ulSpeedUnit = "";
		String delaySpeedtest = "";
		String cellInfo1 = "";
		String cellInfo2 = "";
		String cellInfo = "";
		String lac = "";
		String cid = "";
		String tac = "";
		String pci = "";
		String tac_pci = "";
		String lac_cid = "";
		
		//HTTP
		String protocal = "";
		String testType = "";
		String meanSpeedUnit = "";
		String maxSpeedUnit = "";
		String meanSpeed = "";
		String maxSpeed = "";
		String delayHttp = "";
		
		//BROWSE
		String url = "";
		String delayBrowse = "";
		
		
		String province = "";
		String city = "";
		String district = "";
		String year_month_day = "";
		for (Map<String, String> hColumn : columns) {
			String name = hColumn.get("name");
			String value = hColumn.get("value");
			//获取网络制式及信号强度Start
			if((name.equals("网络(1)网络制式") || name.equals("网络（1）网络制式"))){
				NetType1 = value;
				continue;
			}else if(name.equals("网络(2)网络制式") || name.equals("网络（2）网络制式")){
				NetType2 = value;
				continue;
			}else if(name.equals("网络制式")){
				if(previNetType<=2){
					previNetType = 2;
					NetType = value;
				}
				continue;
			}else if(name.equals("网络类型")){
				if(previNetType<=1){
					previNetType = 1;
					NetType = value;
				}
				continue;
			}
			
			if(name.equals("网络(1)信号强度") || name.equals("网络（1）信号强度")){
				signalStength1 = value;
				continue;
			}else if(name.equals("网络(2)信号强度") || name.equals("网络（2）信号强度")){
				signalStength2 = value;
				continue;
			}else if(name.equals("信号强度")){
				if(previNetType<=1){
					previNetType = 1;
					signalStength = value;
				}
				continue;
			}
			//获取网络制式及信号强度End
			
			if(name.toLowerCase().equals("sinr")){
				sinr = value;
				continue;
			}
			
			//获取GPS位置信息Start
			if(name.equals("GPS信息")){
				if(previGps<5){
					previGps = 5;
					gpsStr = value;
				}
				continue;
			}else if(name.equals("GPS位置信息")){
				if(previGps<4){
					previGps = 4;
					gpsStr = value;
				}
				continue;
			}else if(name.equals("GPS位置")){
				if(previGps<3){
					previGps = 3;
					gpsStr = value;
				}
				continue;
			}else if(name.equals("测试位置")){
				if(previGps<2){
					previGps = 3;
					gpsStr = value;
				}
				continue;
			}else if(name.equals("测试GPS位置")){
				if(previGps<1){
					previGps = 3;
					gpsStr = value;
				}
				continue;
			}									
			//获取GPS位置信息End
			
			//获取速率及时延信息Start
			if(name.equals("下行速率") || name.equals("下行速率(Mbps)") || name.equals("下行速率（Mbps）") || name.equals("下行速率(Kbps)") || name.equals("下行速率（Kbps）")){
				downloadSpeed = value;
				dlSpeedUnit = name;
				continue;
			}else if(name.equals("上行速率") || name.equals("上行速率(Mbps)") || name.equals("上行速率(Kbps)") || name.equals("上行速率（Mbps）") || name.equals("上行速率（Kbps）")){
				uploadSpeed = value;
				ulSpeedUnit = name;
				continue;
			}else if(name.equals("时延") || name.equals("时延(ms)") || name.equals("时延（ms）")){
				delaySpeedtest = value;
				continue;
			}								
			//获取速率及时延信息End
			
			//获取SPEEDTEST小区信息Start
			if(name.equals("网络(1)小区信息") || name.equals("网络（1）小区信息")){
				cellInfo1 = value;
				continue;
			}else if(name.equals("网络(2)小区信息") || name.equals("网络（2）小区信息")){
				cellInfo2 = value;
				continue;
			}else if(name.equals("小区信息")){
				cellInfo = value;
				continue;
			}
			if(name.toLowerCase().equals("tac/pci")){
				tac_pci = value;
				continue;
			}else if(name.toLowerCase().equals("lac/cid")){
				lac_cid = value;
				continue;
			}else if(name.toLowerCase().equals("lac")){
				lac = value;
				continue;
			}else if(name.toLowerCase().equals("cid")){
				cid = value;
				continue;
			}else if(name.toLowerCase().equals("tac")){
				tac = value;
				continue;
			}else if(name.toLowerCase().equals("pci")){
				pci = value;
				continue;
			}
			//获取速率及时延信息End
			
			//获取HTTP速率及时延信息Start
			if(name.equals("平均速率(Mbps)") || name.equals("平均速率(Kbps)") || name.equals("平均速率（Mbps）") || name.equals("平均速率（Kbps）") || name.equals("平均速率")){
				meanSpeedUnit = name;
				meanSpeed = value;
				continue;
			}else if(name.equals("最大速率(Mbps)") || name.equals("最大速率(Kbps)") ||name.equals("最大速率（Mbps）") || name.equals("最大速率（Kbps）") || name.equals("最大速率")){
				maxSpeedUnit = name;
				maxSpeed = value;
				continue;
			}else if(name.equals("协议")){
				protocal = value;
				continue;
			}else if(name.equals("类型")){
				testType = value;
				continue;
			}else if(name.equals("平均时延(ms)") || name.equals("平均时延（ms）") || name.equals("平均时延") || name.equals("时延") || name.equals("时延(ms)")){
				delayHttp = value;
				continue;
			}							
			//获取速率及时延信息End
			
			//获取BROWSE时延信息Start
			if(name.equals("地址") || name.equals("网址")){
				url = value.trim();
				continue;
			}
			if(name.equals("平均时长(ms)") || name.equals("平均时延(ms)") ||name.equals("平均时长（ms）") || name.equals("平均时延（ms）")){
				delayBrowse = value.trim();
				continue;
			}else if(name.equals("平均时延")){
				delayBrowse = value.trim();
				continue;
			}else if(delayBrowse!=null && !delayBrowse.equals("") && (name.equals("时长(ms)") || name.equals("时长（ms）"))){
				delayBrowse = value.trim();
				continue;
			}							
			//获取时延信息End
			
			if(name.equals("cassandra_province")){
				province = value;
				continue;
			}
			if(name.equals("cassandra_city")){
				city = value;
				continue;
			}
			if(name.equals("cassandra_district")){
				district = value;
				continue;
			}
			if(name.equals("year_month_day")){
				year_month_day = value;
				continue;
			}
			
		}
		
		//数据格式整理
		String gpsStrResult = "";
		String[] gpsresult = new String[2];
		
		//SPEEDTEST
		String netResult = "";
		String signalResult = "";
		String sinrResult = "";
		String dlSpeedResult = "";
		String ulSpeedResult = "";
		String delayResultSpeedtest = "";
		String cellResult = "";
		String cellResult2G = "";
		
		//BROWSE
		String urlResult = "";
		String delayResultBrowse = "";

		//HTTP
		String meanSpeedResult = "";
		String maxSpeedResult = "";
		String delayResultHttp = "";
		String protocalResult = "";
		String testTypeResult = "";
		
		String[] gps = new String[2];
		if(gpsStr != null && !gpsStr.trim().equals("--") && !gpsStr.trim().equals("-") && !gpsStr.trim().equals("")){
			gpsStrResult = gpsStr;
			if(gpsStr.contains(" ")){
				gps = transGpsPoint(gpsStr);
				if(gps!=null && gps[0]!=null && gps[1]!=null){
					gpsresult = gps;
				}
			}
		}
		
		if(NetType1!=null && !NetType1.equals("") && !NetType1.trim().equals("-") && !NetType1.trim().equals("--") && !NetType1.trim().equals("N/A")){
			netResult = NetType1.trim();
			signalResult = signalStength1.trim();
			cellResult = cellInfo1.trim();
		}else if(NetType2!=null && !NetType2.equals("") && !NetType2.trim().equals("-") && !NetType2.trim().equals("--") && !NetType2.trim().equals("N/A")){
			netResult = NetType2.trim();
			signalResult = signalStength2.trim();
			cellResult = cellInfo2.trim();
		}else if(NetType!=null && !NetType.equals("") && !NetType.trim().equals("-") && !NetType.trim().equals("--") && !NetType.trim().equals("N/A")){
			netResult = NetType.trim();
			signalResult = signalStength.trim();
			cellResult = cellInfo.trim();
		}
		if(sinr!=null && !sinr.equals("") && !sinr.trim().equals("-") && !sinr.trim().equals("--") && !sinr.trim().equals("N/A") ){
			sinrResult = sinr.trim();
		}
		if(cellResult!=null && !cellResult.equals("") && !cellResult.trim().equals("-") && !cellResult.trim().equals("--") && !cellResult.trim().equals("N/A") && !cellResult.trim().equals("N/A/N/A")){
			
		}else{
			if(netResult.equals("LTE") && tac_pci!=null && !tac_pci.equals("") && !tac_pci.trim().equals("-") && !tac_pci.trim().equals("--") && !tac_pci.trim().equals("N/A") && !tac_pci.trim().equals("N/A/N/A")){
				cellResult = tac_pci.trim();
			}else if(netResult.equals("LTE") && tac!=null && !tac.equals("") && !tac.trim().equals("-") && !tac.trim().equals("--") && !tac.trim().equals("N/A") && !(tac.trim().toLowerCase().indexOf("n/a")!=-1) && pci!=null && !pci.equals("") && !pci.trim().equals("-") && !pci.trim().equals("--") && !pci.trim().equals("N/A") && !(pci.trim().toLowerCase().indexOf("n/a")!=-1)){
				if(tac.toLowerCase().indexOf("tac")!=-1){
					tac = tac.toLowerCase().replace("tac=", "");
				}
				if(pci.toLowerCase().indexOf("pci")!=-1){
					pci = pci.toLowerCase().replace("pci=", "");
				}
				cellResult = tac+"/"+pci;
			}else if(lac_cid!=null && !lac_cid.equals("") && !lac_cid.trim().equals("-") && !lac_cid.trim().equals("--") && !lac_cid.trim().equals("N/A") && !lac_cid.trim().equals("N/A/N/A")){
				cellResult2G = lac_cid.trim();
			}else if(lac!=null && !lac.equals("") && !lac.trim().equals("-") && !lac.trim().equals("--") && !lac.trim().equals("N/A") && !(lac.trim().toLowerCase().indexOf("n/a")!=-1) && cid!=null && !cid.equals("") && !cid.trim().equals("-") && !cid.trim().equals("--") && !cid.trim().equals("N/A") && !(cid.trim().toLowerCase().indexOf("n/a")!=-1)){
				if(lac.toLowerCase().indexOf("lac")!=-1){
					lac = lac.toLowerCase().replace("lac=", "");
				}
				if(cid.toLowerCase().indexOf("cid")!=-1){
					lac = lac.toLowerCase().replace("cid=", "");
				}
				cellResult2G = lac+"/"+cid;
			}
		}
		
		
		if(downloadSpeed!=null && !downloadSpeed.equals("") && !downloadSpeed.trim().equals("-") && !downloadSpeed.trim().equals("--") && !downloadSpeed.trim().equals("N/A") ){
			dlSpeedResult = downloadSpeed.trim();
		}
		if(uploadSpeed!=null && !uploadSpeed.equals("") && !uploadSpeed.trim().equals("-") && !uploadSpeed.trim().equals("--") && !uploadSpeed.trim().equals("N/A")){
			ulSpeedResult = uploadSpeed.trim();
		}
		if(delaySpeedtest!=null && !delaySpeedtest.equals("") && !delaySpeedtest.trim().equals("-") && !delaySpeedtest.trim().equals("--") && !delaySpeedtest.trim().equals("N/A")){
			delayResultSpeedtest = delaySpeedtest.trim();
		}
		
		if(netResult!=null && !netResult.trim().equals("LTE") && cellResult!=null && !cellResult.equals("") && !cellResult.trim().equals("-") && !cellResult.trim().equals("--") && !cellResult.trim().equals("N/A")){
			cellResult = cellResult;
		}else if(netResult!=null && !netResult.trim().equals("LTE") && cellResult2G!=null && !cellResult2G.equals("") && !cellResult2G.trim().equals("-") && !cellResult2G.trim().equals("--") && !cellResult2G.trim().equals("N/A")){
			cellResult = cellResult2G;
		}else if(cellResult!=null && !cellResult.equals("") && !cellResult.trim().equals("-") && !cellResult.trim().equals("--") && !cellResult.trim().equals("N/A")){
			cellResult = cellResult;
		}else if(cellResult2G!=null && !cellResult2G.equals("") && !cellResult2G.trim().equals("-") && !cellResult2G.trim().equals("--") && !cellResult2G.trim().equals("N/A")){
			cellResult = cellResult2G;
		}
		
		//HTTP参数
		if(meanSpeed!=null && !meanSpeed.equals("") && !meanSpeed.trim().equals("-") && !meanSpeed.trim().equals("--") && !meanSpeed.trim().equals("N/A") ){
			meanSpeedResult = meanSpeed.trim();
		}
		if(maxSpeed!=null && !maxSpeed.equals("") && !maxSpeed.trim().equals("-") && !maxSpeed.trim().equals("--") && !maxSpeed.trim().equals("N/A")){
			maxSpeedResult = maxSpeed.trim();
		}
		if(delayHttp!=null && !delayHttp.equals("") && !delayHttp.trim().equals("-") && !delayHttp.trim().equals("--") && !delayHttp.trim().equals("N/A")){
			delayResultHttp = delayHttp.trim();
		}
		if(protocal!=null && !protocal.equals("") && !protocal.trim().equals("-") && !protocal.trim().equals("--") && !protocal.trim().equals("N/A")){
			protocalResult = protocal.trim();
		}
		if(testType!=null && !testType.equals("") && !testType.trim().equals("-") && !testType.trim().equals("--") && !testType.trim().equals("N/A")){
			testTypeResult = testType.trim();
		}

		//Browse参数
		if(url!=null && !url.equals("") && !url.trim().equals("-") && !url.trim().equals("--") && !url.trim().equals("N/A")){
			urlResult = url.trim();
		}
		
		if(delayBrowse!=null && !delayBrowse.equals("") && !delayBrowse.trim().equals("-") && !delayBrowse.trim().equals("--") && !delayBrowse.trim().equals("N/A")){
			delayResultBrowse = delayBrowse.trim();
		}
										
		ArrayList<String> myDataList = new ArrayList<String>();
		myDataList.add(fileType==null?"":fileType);
		myDataList.add(gpsStrResult==null?"":gpsStrResult);
		myDataList.add(gpsresult[0]==null?"":gpsresult[0]);
		myDataList.add(gpsresult[1]==null?"":gpsresult[1]);
		myDataList.add(fileIndex==null?"":fileIndex);
		myDataList.add(netResult==null?"":netResult);
		myDataList.add(signalResult==null?"":signalResult);
		myDataList.add(dlSpeedResult==null?"":dlSpeedResult);
		myDataList.add(ulSpeedResult==null?"":ulSpeedResult);
		myDataList.add(delayResultSpeedtest==null?"":delayResultSpeedtest);
		myDataList.add(cellResult==null?"":cellResult);
		myDataList.add(cellResult2G==null?"":cellResult2G);
		myDataList.add(urlResult==null?"":urlResult);
		myDataList.add(delayResultBrowse==null?"":delayResultBrowse);
		myDataList.add(meanSpeedResult==null?"":meanSpeedResult);
		myDataList.add(maxSpeedResult==null?"":maxSpeedResult);
		
		myDataList.add(delayResultHttp==null?"":delayResultHttp);
		myDataList.add(protocalResult==null?"":protocalResult);
		myDataList.add(testTypeResult==null?"":testTypeResult);
		myDataList.add(province==null?"":province);
		myDataList.add(city==null?"":city);
		myDataList.add(district==null?"":district);
		myDataList.add(year_month_day==null?"":year_month_day);
		myDataList.add(sinrResult==null?"":sinrResult);
		String prefix = ConfParser.org_prefix;
		File csvFile = new File(ConfParser.csvwritepath+File.separator+keySpace+".csv");
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
//			try {
//				Thread.sleep(100000000);
//			} catch (InterruptedException e1) {
//				e1.printStackTrace();
//			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		try {
			List list = new ArrayList();
            for(int i = 0;i < myDataList.size();i++){
            	String str = myDataList.get(i).toString();
            	list.add(i, str);
            }
            String myStr = "";
            for (int i = 0; i < list.size(); i++) {
				String string = (String)list.get(i);
				myStr = myStr + string;
				if(i==list.size()-1){
					myStr = myStr + "";
				}else{
					myStr = myStr + ",";
				}
            }
			System.out.println(myStr);
        	cwriter.writeRecord(myStr.split(","), true);
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
	
	/**
	 * GPS坐标转换
	 * @param meta
	 * @return
	 */
		private static String[] transGpsPoint(String meta){
	
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
			try{
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
			}catch(Exception e){
				e.printStackTrace();
			}
			
			return result;
		}
	
}
