package com.opencassandra.test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;

/**
 * 终端上线数据按IMEI和月拆分成不同文件，从这部分文件中每个月文件取出最后一条数据，按月写入12个月文件中
 * @author wuxiaofeng
 *
 */
public class IntoCSVByCSV {

	private static String srcPath = "C:\\Users\\issuser\\Desktop\\云测试平台项目\\cvs文件样例";     
	private static String destPathRal = "C:\\Users\\issuser\\Desktop\\云测试平台项目\\cvstest\\";    
	private static String destPathFalg = "\\";
	
//	private static String srcPath = "/ctp/terminallog/terminal";      // 按IMEI、月分文件后存放路径
//	private static String destPathRal = "/ctp/terminallog/month/";    // 终端线上数据按月写入12个月文件的存放位置
//	private static String destPathFalg = "/";    // 路径分隔符
	
	private static Map<String, Map<String, List<String[]>>> resultMap = new HashMap<String, Map<String, List<String[]>>>();
	private static Map<String, List<Integer>> imeiMap = new HashMap<String, List<Integer>>();
	private static Set<String> imeiSet = new HashSet<String>();

	private static Runtime r = Runtime.getRuntime();   // 程序运行状态类
	private static long total = r.totalMemory();    // Java虚拟机中内存总量
	
	private long count = 0;      // 读取文件个数标识
	private String lastImei = "";   // 上一个读取的imei文件
	
	private File csvFile = null;
	private CsvWriter cwriter = null;
	private BufferedWriter writer = null;
	
	public static void main(String[] args) {
		
		try {
			Date start = new Date();
			System.out.println("Start:"+start.toLocaleString());
			File file = new File(srcPath);
			IntoCSVByCSV test = new IntoCSVByCSV();
			test.findFile(file.getAbsolutePath());
			if (resultMap.size() > 0) {
				System.out.println("=====test.resultMap.size():" + resultMap.size());
				for (String dateKey : resultMap.keySet()) {
					test.openCsv(destPathRal + dateKey + ".csv");
    				Map<String, List<String[]>> result = resultMap.get(dateKey);
    				for (String imeiKey : result.keySet()) {
    					List<String[]> resultList = result.get(imeiKey);
        				if (resultList != null) {
        					for (String[] infos : resultList) {
        						test.writeCsv(infos);
            				}
        				}
    				}
    				test.closeCsv();
    			}
    			resultMap.clear();
			}
			Date end = new Date();
			System.out.println("Start:"+start.toLocaleString()+">>> End:"+end.toLocaleString());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void findFile(String src) {
		
		File file = new File(src);
		if (!file.exists()) {
			file.mkdirs();
		}
		if (file.isFile() && file.getName().endsWith(".csv")) {
			read(file.getAbsolutePath());
		} else {
			File[] fileList = file.listFiles();
			for (int i = 0; i < fileList.length; i++) {
				File sonFile = fileList[i];
				if (sonFile.isDirectory()) {
					findFile(sonFile.getAbsolutePath());
				} else {
					if (!sonFile.getName().endsWith(".csv")) {
						continue;
					}
					read(sonFile.getAbsolutePath());
				}
			}
		}
	}
	
    public void read(String filename) {
    	
    	count++;
    	try {
    		System.out.println("正在处理第  " + count + " 个文件，文件名：" + filename);
    		
    		// 获取路径/imei/date/文件名
    		String info = filename.replace(srcPath, "");
        	String[] infos = info.split("\\" + destPathFalg);
//        	String[] infos = info.split(destPathFalg);
        	String date = "";
        	String imei = "";
        	System.out.println("======infos" +info);
        	if (infos != null && infos.length == 4) {  // 获取时间和IMEI信息
        		date = infos[2];
        		imei = infos[1];
        	}
        	
        	// 遍历imei文件夹下日期文件夹，将日期文件夹名放入list中排序，存入Map
        	if (imeiMap.get(imei) == null) {
        		List<Integer> imeiList = new ArrayList<Integer>();
            	File file = new File(srcPath + destPathFalg + imei);
            	File[] fileList = file.listFiles();
    			for (int i = 0; i < fileList.length; i++) {
    				File sonFile = fileList[i];
    				if (sonFile.isDirectory()) {
    					imeiList.add(Integer.parseInt(sonFile.getName()));
    					System.out.println("sonFile.getName():" + sonFile.getName());
    				}
    			}
    			Collections.sort(imeiList);
    			imeiMap.put(imei, imeiList);
        	}
        	
        	// 如果有新的imei，则将上个imei文件夹下的日期遍历，
        	if (imeiSet.add(imei)) {
        		if ("".equals(lastImei)) {
        			lastImei = imei;
        		} else {
        			List<Integer> dateList = imeiMap.get(lastImei);  //时间列表201401  201409
        			for (int i = 0; i < dateList.size(); i++) {
        				if (i > 0) {
        					if (dateList.get(i) - dateList.get(i - 1) > 1) {
        						int intDate = dateList.get(i - 1) + 1;
                    			String strDate = String.valueOf(intDate);
                    			Map<String, List<String[]>> tmpMap = resultMap.get(String.valueOf(dateList.get(i - 1)));
                    			List<String[]> list = tmpMap.get(imei);
        						tmpMap.put(imei, list);
                    			resultMap.put(strDate, tmpMap);
        					}
        				}
        			}
        			lastImei = imei;
        		}
        		if (r.freeMemory() <= total * 0.5 ) {
        			System.out.println("=====resultMap.size():" + resultMap.size());
        			if (resultMap.size() > 0) {
        				System.out.println("=====test.resultMap.size():" + resultMap.size());
        				for (String dateKey : resultMap.keySet()) {
            				openCsv(destPathRal + dateKey + ".csv");
            				Map<String, List<String[]>> result = resultMap.get(dateKey);
            				for (String imeiKey : result.keySet()) {
            					List<String[]> resultList = result.get(imeiKey);
                				if (resultList != null) {
                					for (String[] list : resultList) {
                    					writeCsv(list);
                    				}
                				}
            				}
            				closeCsv();
            			}
            			resultMap.clear();
        			}
        		}
        	}
        	System.out.println("===================imeiMap.size:" + imeiMap.size() + "=======imeiSet.size:" + imeiSet.size());
        	
        	ArrayList<String[]> csvList = readeDetailCsvUtf(filename);
        	if (csvList != null && csvList.size() > 0) {
            	String[] lastData = csvList.get(csvList.size() - 1);
            	if (lastData != null && lastData.length != 0) {
            		
            		List<String[]> list = new ArrayList<String[]>();
					list.add(lastData);
					if (resultMap.get(date) == null) {
						Map<String, List<String[]>> tmpMap = new HashMap<String, List<String[]>>();
						tmpMap.put(imei, list);
						resultMap.put(date, tmpMap);
					} else {
						Map<String, List<String[]>> tmpMap = resultMap.get(date);
						if (tmpMap.get(imei) == null) {
							tmpMap.put(imei, list);
						} else {
							tmpMap.get(imei).add(lastData);
						}
						resultMap.put(date, tmpMap);
					}
            	}
        	}
    	} catch (Exception e) {
    		System.out.println("出错了！" + e.toString());
    	}
	}
	
    public ArrayList<String[]> readeDetailCsvUtf(String filePath) {
		
		ArrayList<String[]> csvList = null;
		CsvReader reader = null;
		try {
			csvList = new ArrayList<String[]>();
			String csvFilePath = filePath;
			reader = new CsvReader(csvFilePath, ',',
					Charset.forName("UTF-8")); 
			while (reader.readRecord()) {
				csvList.add(reader.getValues());
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			if(reader!=null){
				reader.close();
			}
		}
		return csvList;
	}
    
    public void openCsv(String path) {
		
		csvFile = new File(path);
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
	
	public void writeCsv(String[] contents) {
		try {
			cwriter.writeRecord(contents, true);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void closeCsv() {
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