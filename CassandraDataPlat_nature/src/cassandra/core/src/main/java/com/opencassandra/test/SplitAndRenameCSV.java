package com.opencassandra.test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;

/**
 * 重命名CSV文件，并按月天存放到不同文件夹中
 * @author wuxiaofeng
 *
 */
public class SplitAndRenameCSV {

//	private static String srcPath = "C:\\Users\\issuser\\Desktop\\liugang";
//	private String destPathRal = "C:\\Users\\issuser\\Desktop\\云测试平台项目\\cvstest\\";
//	private String destPathFalg = "\\";
	
	private static String srcPath = "/OTS";
	private String destPathRal = "/OTS_BACKUP/DATE/";
	private String destPathFalg = "/";
	
	private String fileSplitFalg = "-_-";
	private String monthPath = destPathRal + "month";
	private String dayPath = destPathRal + "day";
	
	private File csvFile = null;
	private CsvWriter cwriter = null;
	private BufferedWriter writer = null;
	
	private static Map<String, String> orgKeyMap = new HashMap<String, String>();
	
	public static void main(String[] args) {
		
		try {
			Date start = new Date();
			System.out.println("Start:"+start.toLocaleString());
			SplitAndRenameCSV test = new SplitAndRenameCSV();
			test.readAll(srcPath);
			for (String s : orgKeyMap.keySet()) {
				System.out.println(s + "====" + orgKeyMap.get(s));
			}
			Date end = new Date();
			System.out.println("Start:"+start.toLocaleString()+">>> End:"+end.toLocaleString());
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void readAll(String src) {
		
		File file = new File(src);
		if (!file.exists()) {
			file.mkdirs();
		}
		mkdir(monthPath);
		mkdir(dayPath);
		
		if (file.isFile() && file.getName().endsWith(".csv")) {
			read(file.getAbsolutePath());
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
					read(sonFile.getAbsolutePath());
				}
			}
		}
	}
	
    public void read(String filePath) {
    	
    	try {
    		if (filePath.toString().endsWith(".csv")) {
        		String splitStr = "";
//        		String[] splitStrs =  filePath.split((new File(srcPath)).getName() + "\\" + destPathFalg);
            	String[] splitStrs =  filePath.split((new File(srcPath)).getName() + destPathFalg);
            	
            	String folderPath = splitStrs[1];
//            	String[] folderPaths = folderPath.split("\\" + destPathFalg);
            	String[] folderPaths = folderPath.split(destPathFalg);
            	String orgKey = folderPaths[0];
            	
            	if (folderPaths.length == 6) {
            		splitStr = splitStr.concat(fileSplitFalg + folderPaths[0] + fileSplitFalg + folderPaths[1] + fileSplitFalg + 
            				folderPaths[2] + fileSplitFalg + folderPaths[3] + fileSplitFalg + "EMPTY" + fileSplitFalg + folderPaths[4]);
            	} else {
            		for (int i = 0; i < folderPaths.length - 1; i++) {
            			splitStr = splitStr.concat(fileSplitFalg + folderPaths[i]);
            		}
                	if (folderPaths.length < 6) {
                		int flag = 7 - folderPaths.length;
                		for (int i = 0; i < flag; i++) {
                			splitStr = splitStr.concat(fileSplitFalg + "EMPTY");
                		}
                	}
            	}
            	
            	String orgId = orgKeyMap.get(orgKey.trim());
            	if (orgId == null || "".equals(orgId)) {
            		Connection conn = getConn();
            		try {
            			String sql = "SELECT id FROM t_org WHERE org_key = '" + orgKey.trim() + "'";
            			PreparedStatement stmt= conn.prepareStatement(sql);
            			ResultSet rs = stmt.executeQuery(sql);
            			while (rs.next()) {
            				orgId = String.valueOf(rs.getInt("id"));
            				orgKeyMap.put(orgKey.trim(), orgId);
            			}
            		} catch (SQLException e) {
            			e.printStackTrace();
            		} finally {
            			if (conn != null) {
            				try {
            					conn.close();
            				} catch (SQLException e) {
            					e.printStackTrace();
            				}
            			}
            		}
            	}
            	if (orgId == null || "".equals(orgId)) {
            		orgId = "0";
            		orgKeyMap.put(orgKey.trim(), orgId);
            	}
            	System.out.println("====================orgId:" + orgId);
            	splitStr = splitStr.concat(fileSplitFalg + orgId);
            	
            	File srcFile = new File(filePath);
            	String fileName = srcFile.getName();
            
            	String[] fileNames = fileName.split("-");
            	String month = "";
            	String day = "";
            	System.out.println("==============fileName:" + fileName);
            	if (fileNames.length == 2) {
            		
            		String timeInfo = fileNames[1];
            		String time = timeInfo.split("\\.")[0];
            		time = time.replaceAll("_", "");
            		System.out.println("=========time:" + time);
            		day = time.substring(0, 8);
            		month = time.substring(0, 6);
            	} else if (fileNames.length > 2) {
            		
            		String timeInfo = fileNames[1];
            		String time = timeInfo.replaceAll("_", "");
            		System.out.println("=========time:" + time);
            		day = time.substring(0, 8);
            		month = time.substring(0, 6);
            	}
            	
            	mkdir(monthPath + destPathFalg + month);
            	mkdir(dayPath + destPathFalg + day);
            	
            	String newFileName = fileName + splitStr;
            	
            	File destFile = new File(monthPath + destPathFalg + month + destPathFalg + newFileName);
            	copyFile(srcFile, destFile);
            	
            	destFile = new File(dayPath + destPathFalg + day + destPathFalg + newFileName);
            	copyFile(srcFile, destFile);
    		}
    	} catch (Exception e) {
    		e.printStackTrace();
    		return;
    	}
	}
	
    public boolean copyFile(File srcFile, File destFile) {
    	
		InputStream inStream = null;
		FileOutputStream fs = null;
		try {
			if (destFile.createNewFile()) {
				int byteread = 0;
				inStream = new FileInputStream(srcFile);
				fs = new FileOutputStream(destFile);
				byte[] buffer = new byte[4096];
				while ((byteread = inStream.read(buffer)) != -1) {
					fs.write(buffer, 0, byteread);
				}
				return true;
			} else {
				System.out.println("目标文件创建失败: " + destFile.getAbsolutePath());
				return false;
			}
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("目标文件创建失败: " + destFile.getAbsolutePath());
			return false;
			
		} finally {
			try {
				if (inStream != null) {
					inStream.close();
				}
				if (fs != null) {
					fs.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
    
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
	
	private Connection getConn() {
		
		String driver = "com.mysql.jdbc.Driver";
		String url = "jdbc:mysql://192.168.85.234:3306/auth";
		String user = "root";
		String password = "cmrictpdata";
		try {
			Class.forName(driver);
			Connection conn = DriverManager.getConnection(url, user, password);
			return conn;
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}
}