package cn.speed.history.process.impl;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;

public class DealWithSpeedtest {

	/**
	 * 读取deviceInfo.CSV文件
	 */
	public String[] readeInfoCsv(String filePath) {
		String[] contents = new String[39];
		for(int i=0; i<39; i++){
			contents[i] = "";
		}
		try {
			ArrayList<String[]> csvList = new ArrayList<String[]>(); // 用来保存数据
			String csvFilePath = filePath;
			CsvReader reader = new CsvReader(csvFilePath, ',',
					Charset.forName("GB2312")); // 一般用这编码读就可以了

			// reader.readHeaders(); // 跳过表头 如果需要表头的话，不要写这句。

			while (reader.readRecord()) { // 逐行读入除表头的数据
				csvList.add(reader.getValues());
			}
			reader.close();

			contents[0] = csvList.get(1)[1] != null ? csvList.get(1)[1] : "";
			contents[1] = csvList.get(2)[1] != null ? csvList.get(2)[1] : "";
			contents[2] = csvList.get(3)[1] != null ? csvList.get(3)[1] : "";
			contents[3] = csvList.get(4)[1] != null ? csvList.get(4)[1] : "";
			contents[4] = csvList.get(5)[1] != null ? csvList.get(5)[1] : "";
			contents[5] = csvList.get(6)[1] != null ? csvList.get(6)[1] : "";

			for(int i=6; i+3<csvList.size(); i++){
				contents[i] = csvList.get(i+3)[1] != null ? csvList.get(i+3)[1] : "";
			}

		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {

		}
		return contents;
	}
	
	/**
	 * 读取Detail.CSV文件
	 */
	public ArrayList<String[]> readeDetailCsv(String filePath) {
		ArrayList<String[]> csvList = null;
		try {
			csvList = new ArrayList<String[]>(); // 用来保存数据
			String csvFilePath = filePath;
			CsvReader reader = new CsvReader(csvFilePath, ',',
					Charset.forName("GB2312")); // 一般用这编码读就可以了

			// reader.readHeaders(); // 跳过表头 如果需要表头的话，不要写这句。

			while (reader.readRecord()) { // 逐行读入除表头的数据
				csvList.add(reader.getValues());
			}
			reader.close();

		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {

		}
		return csvList;
	}

	/**
	 * 读取Detail.CSV文件
	 */
	public void insertData(String style, String infoCsv, String detailCsv){
		if(!(new File(infoCsv)).exists() || !(new File(detailCsv)).exists()){
			return;
		}
		
		String[] contents = readeInfoCsv(infoCsv);
		contents[38] = style;
		ArrayList<String[]> csvList = readeDetailCsv(detailCsv);
		for(int i=0,j=20; i<10; i++,j++){
			contents[j] = csvList.get(i)[1] != null ? csvList.get(i)[1] : "";
		}
		
		for(int i=13; i<csvList.size()&&i<28; i++){
			contents[30] = csvList.get(i)[0] != null ? csvList.get(i)[0] : "";
			if(csvList.get(i)[1] != null){
				if(csvList.get(i)[1].contains("MB")){
					contents[31] = csvList.get(i)[1] != null ? String.valueOf(Float.valueOf(csvList.get(i)[1].replace("MB", "").replace("KB", ""))*8) : "";
				}else if(csvList.get(i)[1].contains("KB")){
					contents[31] = csvList.get(i)[1] != null ? String.valueOf(Float.valueOf(csvList.get(i)[1].replace("MB", "").replace("KB", ""))*8/1024) : "";
				}
			}
			contents[32] = csvList.get(i)[2] != null ? csvList.get(i)[2].replace("dBm", "") : "";
			contents[33] = csvList.get(i)[3] != null ? csvList.get(i)[3] : "";
			this.writeCsv(contents);
		}
		for(int i=29; i<csvList.size()&&i<47; i++){
			contents[30] = "";
			contents[31] = "";
			contents[32] = "";
			contents[33] = "";
			contents[34] = csvList.get(i)[0] != null ? csvList.get(i)[0] : "";
			if(csvList.get(i)[1] != null){
				if(csvList.get(i)[1].contains("MB")){
					contents[35] = csvList.get(i)[1] != null ? String.valueOf(Float.valueOf(csvList.get(i)[1].replace("MB", "").replace("KB", ""))*8) : "";
				}else if(csvList.get(i)[1].contains("KB")){
					contents[35] = csvList.get(i)[1] != null ? String.valueOf(Float.valueOf(csvList.get(i)[1].replace("MB", "").replace("KB", ""))/1024) : "";
				}
			}
			contents[36] = csvList.get(i)[2] != null ? csvList.get(i)[2].replace("dBm", "") : "";
			contents[37] = csvList.get(i)[3] != null ? csvList.get(i)[3] : "";
			this.writeCsv(contents);
		}
		
		
	}
	/**
	 * 写入CSV文件
	 */
	public void writeCsv(String[] contents) {
		try {
			String csvFilePath = "c:/test(removeUnit).csv";
			CsvWriter wr = new CsvWriter(csvFilePath, ',',
					Charset.forName("GB2312"));
			wr.writeRecord(contents);
			wr.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	static public void main(String[] args) {
		// String[] contents = {"aaaaa","bbbbb","cccccc","ddddddddd"};
		//String infoCsv = "c:/2013-06-21_13_52_26-HTTP-deviceInfo.csv";
		//String detailCsv = "c:/2013-06-21_13_52_26-HTTP-deviceInfo.csv";
		DealWithSpeedtest mydwc = new DealWithSpeedtest();
		//mydwc.insertData(infoCsv, detailCsv);
		//String[] contents = mydwc.readeCsv();
		//mydwc.writeCsv(contents);
		
		mydwc.findFile("C:/Users/cmcc/Desktop/shanghai/shanghaidata/speedtest/Mobile");
	}
	
	public void findFile(String rootDirectory){
		File[] list = (new File(rootDirectory)).listFiles();
		for(int i=0; i<list.length; i++){
			if(list[i].isDirectory()){
				findFile(list[i].getAbsolutePath());
			}else if(list[i].getName().trim().endsWith("TCP.csv")){
				this.insertData("TCP",list[i].getAbsolutePath().trim().replace("TCP.csv", "TCP-deviceInfo.csv"), list[i].getAbsolutePath().trim());
				System.out.println(list[i].getAbsolutePath());
			}else if(list[i].getName().trim().endsWith("UDP.csv")){
				this.insertData("UDP",list[i].getAbsolutePath().trim().replace("UDP.csv", "UDP-deviceInfo.csv"), list[i].getAbsolutePath().trim());
				System.out.println(list[i].getAbsolutePath());
			}else if(list[i].getName().trim().endsWith("HTTP.csv")){
				this.insertData("HTTP",list[i].getAbsolutePath().trim().replace("HTTP.csv", "HTTP-deviceInfo.csv"), list[i].getAbsolutePath().trim());
				System.out.println(list[i].getAbsolutePath());
			}
		}
	}
}
