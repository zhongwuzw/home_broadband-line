package com.opencassandra.descfile;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import com.csvreader.CsvWriter;

public class CsvWrite {
	public static File file = new File("C:\\Users\\issuser\\Desktop\\test.csv");
	public static BufferedWriter writer = null;
	public static CsvWriter cwriter = null;
	
	public CsvWrite() {
		
	}
	public CsvWrite(String filePath) {
		CsvWrite.file = new File(filePath);
	}
	
	public static void start(){
		try {
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
            for(int i = 0;i < list.size();i++){
            	String str = list.get(i).toString();
            	cwriter.writeRecord(str.split(","), true);
            }
		} catch (IOException e) {
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
