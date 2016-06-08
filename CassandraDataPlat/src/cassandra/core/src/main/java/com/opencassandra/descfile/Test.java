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

import sun.text.normalizer.CharTrie.FriendAgent;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;

public class Test {
	static String[] dateStr = new String[] { "yyyy", "MM", "dd", "HH", "mm",
			"ss", "sss" };
	private File csvFile = null;
	private CsvWriter cwriter = null;
	private BufferedWriter writer = null;

	
	public static void main(String[] args) {
		String updateSql = "insert into ping_new (model,signal_strength1,file_index,appid,detailreport,id,logversion,time,times,sinr1,lac_tac,gps,longitude,haschange,net_type1,max_rtt,sinr,device_org,lac_tac1,file_path,imei,net_type,android_ios,cid_pci1,signal_strength,avg_rtt,success_rate,latitude,cid_pci ) values ('iPhone 6(iPhone7,2)',-70,'3b7d67837cd10f057869f3039912042726e1e8fd|00000000_04002.000_0-2015_08_31_20_48_39_382.summary.csv_0','1708467f00836880517dad80e17b1d8c',1100,1630242,'2.1.1',1441025319374,5,30,18256,'121.962582E 43.431238N',121.97465576908745,1,'LTE',178,30,'CMCC_NEIMENGGU_ZHIDONGBOCE_TL',18256,'/opt/TestResult_Cascade/1708467f00836880517dad80e17b1d8c/CMCC/CMCC_NEIMENGGU/CMCC_NEIMENGGU_ZHIDONGBOCE/CMCC_NEIMENGGU_ZHIDONGBOCE_TL/Default/ping/Mobile/iPhone 6(iPhone7,2)/3b7d67837cd10f057869f3039912042726e1e8fd/00000000_04002.000_0-2015_08_31_20_48_39_382.summary.csv','3b7d67837cd10f057869f3039912042726e1e8fd','LTE','ios',9,-70,115,100,43.43923614670469,9 )";
		StringBuffer sql = new StringBuffer("");
		sql.append(",").append(updateSql.substring(updateSql.indexOf(" values (")+8,updateSql.length()));
		System.out.println(sql.toString());
		System.out.println(" ".equals(""));
		
	}
	
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

	
	public boolean writeCsv(String numType,double longitudeNum,double latitudeNum){
		Format fm = new DecimalFormat("#.######");
		String fileName = fm.format(longitudeNum)+"_"+fm.format(latitudeNum)+"_"+numType+".csv";
		String filePath = ConfParser.csvwritepath;
		String floderName = Math.floor(longitudeNum)+"_"+Math.floor(latitudeNum);
		String paths = filePath+File.separator+floderName+File.separator+fileName;
		if(filePath.isEmpty()){
			System.err.println("未配置csv输出路径");
			return false;
		}
		Test1 test1 = new Test1();
		try {
			File file = new File(paths);
			open(paths);
			for (int i = 0; i <100; i++) {
				if(i == 50){
					System.out.println("50");
					test1.writeCsv("01001", 1, 1);
				}
				write(new String[]{"a","b","c","d","e","f","g"});	
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
