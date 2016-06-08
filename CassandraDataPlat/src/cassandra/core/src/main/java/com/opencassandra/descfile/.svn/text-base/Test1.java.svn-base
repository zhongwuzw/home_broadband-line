package com.opencassandra.descfile;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.text.DecimalFormat;
import java.text.Format;
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

import org.apache.commons.io.output.FileWriterWithEncoding;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;

public class Test1 {
	static String[] dateStr = new String[] { "yyyy", "MM", "dd", "HH", "mm",
			"ss", "sss" };
	private File csvFile = null;
	private CsvWriter cwriter = null;
	private BufferedWriter writer = null;

	public ArrayList<String[]> readCsv(File filePath) {
		ArrayList<String[]> csvList = null;
		try {
			csvList = new ArrayList<String[]>(); // 用来保存数据
			String csvFilePath = filePath.getAbsolutePath();
			CsvReader reader = new CsvReader(csvFilePath, ',', Charset
					.forName("GB2312")); // 一般用这编码读就可以了

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

	public void getCsv() {
		List<String[]> list = new ArrayList();
		List<String[]> list1 = new ArrayList();
		list = this.readCsv(new File("C:\\Users\\issuser\\Desktop\\five.csv"));// 现有的keyspace
		list1 = this.readCsv(new File("C:\\Users\\issuser\\Desktop\\four.csv"));// 数据库的org_key
		Map map = new HashMap();
		Map map1 = new HashMap();
		for (int i = 0; i < list.size(); i++) {
			map.put(list.get(i)[0], "");
		}
		for (int i = 0; i < list1.size(); i++) {
			map1.put(list1.get(i)[0], "");
		}
		Set set = map1.keySet();
		Iterator iter = set.iterator();
		while (iter.hasNext()) {
			String name = (String) iter.next();
			if (!map1.containsKey(name)) {
				System.out.println(name);
			}
		}
	}

	public static void main(String[] args) {
		Test1 test1 = new Test1();
		test1.writeCsv("01001", 1, 1);
	}
	
	public boolean writeCsv(String numType,double longitudeNum,double latitudeNum){
		Format fm = new DecimalFormat("#.######");
		String fileName = fm.format(longitudeNum)+"_"+fm.format(latitudeNum)+"_"+numType+".csv";
		String filePath = ConfParser.csvwritepath;
		String floderName = Math.floor(longitudeNum)+"_"+Math.floor(latitudeNum);
		String paths = filePath+File.separator+floderName+File.separator+fileName;
		if(filePath.isEmpty()){
//			filePath = ""; 
			System.err.println("未配置csv输出路径");
			return false;
		}
		try {
			File file = new File(paths);
			open(paths);
			for (int i = 0; i <100; i++) {
				if(i == 50){
					System.out.println("50");
				}
				write(new String[]{"1","2","3","4","5","6","7"});	
			}
			close();
		} catch (Exception e) {
			return false;
		}
		return true;
	}
	
	
	public void open(String fileName) {
		boolean flag = false;
		csvFile = new File(fileName);
		try {
			if(!csvFile.exists()){
				csvFile.getParentFile().mkdirs();
				csvFile.createNewFile();
			}
			writer = new BufferedWriter(new FileWriterWithEncoding(csvFile,"gbk", true));
			cwriter = new CsvWriter(writer,',');
		} catch (FileNotFoundException e){
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void write(String[] dataStr) {
		try {
            cwriter.writeRecord(dataStr, true);
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
	
	
	private static String convertFileSize(long fileS) {
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
	private static String getLastModifies(File file){
		FileInputStream fis;
		try {
			fis = new FileInputStream(file);
			long modifiedTime = file.lastModified();
			Date date = new Date(modifiedTime);
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:MM");
			String dd = sdf.format(date);
//			System.out.println("File name:" + file.getName() + " \tFile size: "
//					+ (double) ((double) fis.available() / 1024 / 1024) + "M"
//					+ " \tFile create Time: " + dd);
			return dd;
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			return "文件读取错误";
		} catch (IOException e) {
			e.printStackTrace();
			return "文件读取错误";
		}
	}

	private static String replaceChar(String str) {
		str = str.replace("/", "");
		str = str.replace(",", "");
		str = str.replace("\\", "");
		str = str.replace("，", "");
		str = str.replace("-", "");
		return str;
	}

	public static void test(String date) {
		String[] time = getLen(date);
		StringBuffer rex = new StringBuffer("");
		for (int i = 0; i < time.length; i++) {
			rex.append(time[i]);
			rex.append(dateStr[i]);
		}
		outDate(date, rex.toString());
	}

	public static void outDate(String time, String rex) {

		Date date = null;
		SimpleDateFormat formatter = new SimpleDateFormat(rex);
		try {
			date = formatter.parse(time);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println(date.toLocaleString());
	}

	private static String[] getLen(String date1) {
		if (!date1.isEmpty()) {
			Pattern pattern = Pattern.compile("\\d+");
			return pattern.split(date1);
		} else {
			return new String[] {};
		}
	}
}
