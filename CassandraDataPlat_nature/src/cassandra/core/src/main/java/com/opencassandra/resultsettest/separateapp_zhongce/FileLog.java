package com.opencassandra.resultsettest.separateapp_zhongce;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class FileLog {
	static String fileLogPath = "logs/file.log";
	static File file = new File(fileLogPath);
	static{
		if (!file.getParentFile().exists()) {
			file.getParentFile().mkdirs();
		}
		try {
			file.createNewFile();
		} catch (IOException e) {
			// TODO: handle exception
		}
	}
	
	public static void writeToLogFile(String logStr) {
		  try {
			   FileWriter fw = new FileWriter(file, true);
			   BufferedWriter bw = new BufferedWriter(fw);
			   bw.write(logStr + "\n");
			   bw.flush();
			   bw.close();
			   fw.close();
			  } catch (IOException e) {
			   e.printStackTrace();
			  }
	}
}
