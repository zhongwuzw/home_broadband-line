package com.opencassandra.test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import net.sf.json.JSONObject;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;

/**
 * 终端上线数据按IMEI和月拆分成不同文件，从这部分文件中每个月文件取出最后一条数据，写入数据库
 * @author wuxiaofeng
 *
 */
public class IntoDBByCSV {

//	private static String srcPath = "C:\\Users\\issuser\\Desktop\\云测试平台项目\\logtest";     
//	private static String destPathFalg = "\\";
	
	private static String srcPath = "/ctp/terminallog/terminal-wuxf";      // 按IMEI、月分文件后存放路径
	private static String destPathFalg = "/";    // 路径分隔符
	
	private static List<DevicePositionBean> devicePositionList = new ArrayList<DevicePositionBean>();

	private static Runtime r = Runtime.getRuntime();   // 程序运行状态类
	private static long total = r.totalMemory();    // Java虚拟机中内存总量
	
	private long count = 0;      // 读取文件个数标识
	
	private File csvFile = null;
	private CsvWriter cwriter = null;
	private BufferedWriter writer = null;
	
	public static void main(String[] args) {
		
		try {
			Date start = new Date();
			System.out.println("Start:"+start.toLocaleString());
			File file = new File(srcPath);
			IntoDBByCSV test = new IntoDBByCSV();
			test.findFile(file.getAbsolutePath());
			test.intoDB();
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
    		
    		if (r.freeMemory() <= total * 0.5 ) {
    			intoDB();
    		}
        	
        	ArrayList<String[]> csvList = readeDetailCsvUtf(filename);
        	if (csvList != null && csvList.size() > 0) {
            	String[] lastData = csvList.get(csvList.size() - 1);
            	if (lastData != null && !"".equals(lastData) && lastData.length == 5) {
					DevicePositionBean devicePositionBean = new DevicePositionBean(lastData[0].trim(), lastData[1].trim(), Float.parseFloat(lastData[2].trim()), Float.parseFloat(lastData[3].trim()), lastData[4].trim());
					String jsonStr = BaiduAPI.testPost(lastData[3].trim(), lastData[2].trim());
					//{"status":"OK","result":{"location":{"lng":117.630028,"lat":26.276112},"formatted_address":"福建省三明市梅列区工业北路","business":"","addressComponent":{"city":"三明市","direction":"","distance":"","district":"梅列区","province":"福建省","street":"工业北路","street_number":""},"cityCode":254}}
					JSONObject json = new JSONObject();
					json = JSONObject.fromObject(jsonStr);
					
					if (json.containsKey("result")) {
						json = JSONObject.fromObject(json.getString("result"));
						if (json.containsKey("addressComponent")) {
							json = JSONObject.fromObject(json.getString("addressComponent"));
							if (json.containsKey("city")) {
								devicePositionBean.setClusterCity(json.getString("city"));
								System.out.println(json.getString("city"));
							}
							if (json.containsKey("province")) {
								devicePositionBean.setClusterProvince(json.getString("province"));
								System.out.println(json.getString("province"));
							}
							if (json.containsKey("district")) {
								devicePositionBean.setClusterDistrict(json.getString("district"));
								System.out.println(json.getString("district"));
							}
						}
					}
					
					// 获取路径/imei/date/文件名
		    		String info = filename.replace(srcPath, "");
//		        	String[] infos = info.split("\\" + destPathFalg);
		        	String[] infos = info.split(destPathFalg);
		        	String imei = "";
		        	if (infos != null && infos.length == 4) {  // 获取时间和IMEI信息
		        		imei = infos[1];
		        	}
					
					int deviceOrgId = 0;
		        	String deviceOrg = "";
		        	Connection conn = getDeviceConn();
		    		try {
		    			String sql = "SELECT device_org_id, device_org FROM d_device WHERE device_imei='"+ imei +"'";
		    			PreparedStatement stmt= conn.prepareStatement(sql);
		    			ResultSet rs = stmt.executeQuery(sql);
		    			while (rs.next()) {
		    				if (!"".equals(rs.getString("device_org_id")) && rs.getString("device_org_id") != null) {
		    					deviceOrgId = Integer.parseInt(rs.getString("device_org_id"));
		    				}
		    				deviceOrg = rs.getString("device_org");
		    			}
		    		} catch (SQLException e) {
		    			System.out.println("出错了！" + e.toString());
		    		} finally {
		    			if (conn != null) {
		    				try {
		    					conn.close();
		    				} catch (SQLException e) {
		    					e.printStackTrace();
		    				}
		    			}
		    		}
		    		System.out.println("========deviceOrgId:" + deviceOrgId +  "========deviceOrg: " + deviceOrg);
		    		devicePositionBean.setDeviceOrgId(deviceOrgId);
	    			devicePositionBean.setDeviceOrg(deviceOrg);
	    			
					devicePositionList.add(devicePositionBean);
            	}
        	}
    	} catch (Exception e) {
    		System.out.println("出错了！" + e.toString());
    	}
	}

	private void intoDB() {
		
		for (DevicePositionBean devicePositionBean : devicePositionList) {
			Connection conn = getConn();
			try {
				String sql = "INSERT INTO device_position(last_heart_time, imei, longitude, latitude, toolversion, cluster_city, cluster_province, cluster_district, device_org, device_org_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ? )";
				PreparedStatement stmt= conn.prepareStatement(sql);
				stmt.setString(1, devicePositionBean.getLastHeartTime());
				stmt.setString(2, devicePositionBean.getImei());
				stmt.setFloat(3, devicePositionBean.getLongitude());
				stmt.setFloat(4, devicePositionBean.getLatitude());
				stmt.setString(5, devicePositionBean.getToolVersion());
				stmt.setString(6, devicePositionBean.getClusterCity());
				stmt.setString(7, devicePositionBean.getClusterProvince());
				stmt.setString(8, devicePositionBean.getClusterDistrict());
				stmt.setString(9, devicePositionBean.getDeviceOrg());
				stmt.setInt(10, devicePositionBean.getDeviceOrgId());
		        int result = stmt.executeUpdate();
		        if (result > 0) {
		        	 System.out.println("插入成功！");
		        }

			} catch (SQLException e) {
				System.out.println("==================" + devicePositionBean.getDeviceOrgId());
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
		devicePositionList.clear();
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
	
	private Connection getConn() {
		
		String driver = "com.mysql.jdbc.Driver";
		String url = "jdbc:mysql://192.168.85.233:3306/testdataanalyse";
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
	
	private Connection getDeviceConn() {
		
		String driver = "com.mysql.jdbc.Driver";
		String url = "jdbc:mysql://192.168.85.234:3306/device";
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