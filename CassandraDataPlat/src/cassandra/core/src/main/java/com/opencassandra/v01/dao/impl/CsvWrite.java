package com.opencassandra.v01.dao.impl;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import com.csvreader.CsvWriter;

public class CsvWrite {
	public static File filePath = new File("C:\\abc\\");
	public static BufferedWriter writer = null;
	public static CsvWriter cwriter = null;
	
	public CsvWrite() {
		
	}
	public CsvWrite(String filePath) {
		CsvWrite.filePath = new File(filePath);
	}
	
	public static void start(String fileName){
		try {
			String realFile = filePath.getAbsolutePath()+File.separator+fileName+".csv";
			File file = new File(realFile);
			if(!file.exists()){
				file.getParentFile().mkdirs();
				file.createNewFile();
			}
			writer = new BufferedWriter(new FileWriter(file,true));
			cwriter = new CsvWriter(writer,',');
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	public static void write(List list){
		try {
			if(list!=null && list.size()>0){
				String[] myStr = new String[list.size()];
				for(int i = 0;i < list.size();i++){
	            	String str = list.get(i).toString();
	            	str = "\""+str+ "\"";
//	            	list.set(i, str);
	            	myStr[i] = str;
	            }
	        	cwriter.writeRecord(myStr, true);
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public static void stop(){
		if(cwriter!=null){
			cwriter.flush();//刷新数据
			cwriter.close();			
		}
	}
	
}
