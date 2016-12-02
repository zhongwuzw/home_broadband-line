package com.test.csv;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Date;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;

/**
 * 按天月拆分cvs文件
 * @author wuxiaofeng
 *
 */
public class SplitCSVByDate {

//	private static String srcPath = "/home/user/liugang/OTS/result";
	private static String srcPath = "C:\\Users\\issuser\\Desktop\\云测试平台项目\\cvs文件样例";
	
//	private String destPathRal = "/ctp/terminallog/terminal/";
	private String destPathRal = "C:\\Users\\issuser\\Desktop\\云测试平台项目\\cvstest\\";
	private String destPathFalg = "\\";

	private File csvFile = null;
	private CsvWriter cwriter = null;
	private BufferedWriter writer = null;
	
	public static void main(String[] args) {
		
		try {
			File file = new File(srcPath);
			SplitCSVByDate test = new SplitCSVByDate();
			test.readAll(file.getAbsolutePath());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void readAll(String src) {
		
		Date start = new Date();
		System.out.println("Start:"+start.toLocaleString());
		File file = new File(src);
		if (!file.exists()) {
			file.mkdirs();
		}
		if (file.isFile() && file.getName().endsWith(".csv")) {
			read(file.getAbsolutePath(), "utf-8");
		} else {
			File[] fileList = file.listFiles();
			for (int i = 0; i < fileList.length; i++) {
				File sonFile = fileList[i];
				if (sonFile.isDirectory()) {
					readAll(sonFile.getAbsolutePath());
				} else {
					if (!sonFile.getName().endsWith(".csv")) {
						continue;
					}
					System.out.println("正在读取 "+sonFile.getParent()+" 文件夹中第"+(i+1)+"个文件，共"+fileList.length+"个文件");
					read(sonFile.getAbsolutePath(),"utf-8");
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
    	
    	File filePath = new File(filename);
    	String imei = "";
    	imei = filePath.getName().substring(0, filePath.getName().indexOf(".csv"));
    	
    	mkdir(destPathRal + imei);
		
    	ArrayList<String[]> csvList = readCsv(filePath);
    	for (String[] results : csvList) {
    		
    		String testTime = results[results.length - 1];
    		if (DateUtil.strToDate(testTime, "yyyyMMdd") == null) {
    			testTime = results[22];
    		}
    		
    		if (testTime.startsWith("0")) {
    			
    			System.out.println("============testTime:" + testTime);
    			this.open(destPathRal + imei + destPathFalg + imei + "-" + "errData.csv");
    			write(results);
    			this.close();
    		} else {
    			
    			String monthPath = destPathRal + imei + destPathFalg + "month";
    			mkdir(monthPath);
    			
    			String monthDatePath = monthPath + destPathFalg + DateUtil.dateToStr(DateUtil.strToDate(testTime, "yyyyMMdd"), "yyyyMM");
    			mkdir(monthDatePath);
    			
    			String monthFileName = monthDatePath + destPathFalg + imei + "-" + DateUtil.dateToStr(DateUtil.strToDate(testTime, "yyyyMMdd"), "yyyyMM") + ".csv";
    			this.open(monthFileName);
    			write(results);
    			this.close();
    			
    			String dayPath = destPathRal + imei + destPathFalg + "day";
    			mkdir(dayPath);
    			
    			String timeFolderPath = dayPath + destPathFalg + testTime;
    			mkdir(timeFolderPath);
    			
        		String fileName = timeFolderPath + destPathFalg + imei + "-" + testTime + ".csv";
    			this.open(fileName);
    			write(results);
    			this.close();
    		}
    	}
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

			long flag = 0l;
			while (reader.readRecord()) { // 逐行读入除表头的数据
				flag += 1;
				System.out.println(flag);
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
    
	private void mkdir(String filePath) {
		
		File file = new File(filePath);
		if(!file.exists()){
			file.mkdirs();
		}
	}
	
	public void open(String fileName) {
		
		csvFile = new File(fileName);
		try {
			if (!csvFile.exists()) {
				csvFile.getParentFile().mkdirs();
				csvFile.createNewFile();
			}
			writer = new BufferedWriter(new FileWriter(csvFile,true));
			cwriter = new CsvWriter(writer,',');
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	public void write(String[] results) {
		
		try {
            cwriter.writeRecord(results, true);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void close() {
		
		try {
			if (writer!=null) {
				try {
					writer.flush();
					writer.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			if (cwriter!=null) {
				cwriter.flush();
				cwriter.close();
			}
		} catch (Exception e){
			e.printStackTrace();
		}
	}
}