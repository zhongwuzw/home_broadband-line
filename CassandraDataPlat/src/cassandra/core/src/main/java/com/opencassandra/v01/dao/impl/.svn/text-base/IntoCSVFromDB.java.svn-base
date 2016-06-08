package com.opencassandra.v01.dao.impl;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.output.FileWriterWithEncoding;

import net.sf.json.JSONObject;

import com.csvreader.CsvWriter;
import com.opencassandra.v01.bean.CSVDataBean;

/**
 * 数据导出功能:按照附件的表头格式将测试报告数据从mysql数据库中导出
 * 导出条件：指定分组和操作系统类型，测试类型包括speedtest，WebBrowse，Download，Upload
 * @author wuxiaofeng
 *
 */
public class IntoCSVFromDB {
	
//	private String filePath = "C:\\Users\\issuser\\Desktop\\云测试平台项目\\cvstest\\";  
	private String filePath = "/ctp/terminallog/wuxf/";  
	
	private File csvFile = null;
	
	private CsvWriter cwriter = null;
	
	private BufferedWriter writer = null;
	
	private static Map<String, String> testTypeMap = new HashMap<String, String>();
	
	private static String[] header = {"testtime","logversion","TestType","TestTypeCode","longitude","latitude","model","IMEI","Network","SignalStrength(dBm)","lac_tac","cid_pci",		
			"Protocal","Average Download Speed(Mbps)","Average Upload Speed(Mbps)","Max Download Speed(Mbps)","Max Upload Speed(Mbps)","Lantency(ms)",
			"WebBrowse URL","WebBrowse Loading Time(ms)","WebBrowse Average Speed(Kbps)","WebBrowse 80% Avg Time(ms)","WebBrowse 80% Avg Speed(Mbps)","WebBrowse Success Ratio(%)",
			"Download URL","Download File size(MB)","Download duration(ms)","Download Average Speed(Mbps)","Download Max Speed(Mbps)","Download Average Latency (ms)","Download Success Ratio(%)",	
			"Upload URL","Upload File size(MB)","Upload duration(ms)","Upload Average Speed(Mbps)","Upload Max Speed(Mbps)","Upload Average Latency (ms)","Upload Success Ratio(%)"};
	
	static {
		testTypeMap.put("speed_test_new", "01001");
		testTypeMap.put("web_browsing_new", "02001");
		testTypeMap.put("http_test_new", "03001");
	}
	
	JSONObject requestBody = null;
	
	public IntoCSVFromDB(JSONObject requestBody) {
		this.requestBody = requestBody;
	} 
	
	public IntoCSVFromDB() {
		
	}
	
	public Map<String, Object> searchFromDB() throws Exception {
		
//		String deviceOrg = "EN_CMCC_CMPAK_DEFAULT";
//		String osType = "android";
		String deviceOrg = "";
		String osType = "";
		String callback = "";
		Map<String, Object> list = new HashMap<String, Object>();
		
		if (requestBody != null) {
			deviceOrg = (String)requestBody.get("deviceOrg");
			osType = (String)requestBody.get("osType");
			callback = (String)requestBody.get("callback");
			System.out.println(callback);
		}
		System.out.println("=========deviceOrg:" + deviceOrg + ", =========osType:" + osType);
		String result = "fail";
		if (deviceOrg != null && !"".equals(deviceOrg) && osType != null && !"".equals(osType)) {
			System.out.println("========进入方法========");
			result = intoCSVFromDB(deviceOrg, osType);
			if(callback != null && !"".equals(callback)){
	           list.put("callback",""+callback+""+result+"");  
	        }
		}
		System.out.println("============result:" +result);
		list.put("result", result);
		System.out.println("执行完毕");
		return list;
	}
	
	public static void main(String[] args) {
		IntoCSVFromDB test = new IntoCSVFromDB();
		try {
			test.searchFromDB();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public String intoCSVFromDB(String deviceOrg, String osType) {
		
		Connection conn = getConn();
		try {
			String fileName = filePath + deviceOrg + "-" + osType + "-" + DateUtil.getDate(System.currentTimeMillis(), "yyyyMMddHHmmss") + ".csv";
			open(fileName);
			write(header);
			close();
			
			for (String tableName : testTypeMap.keySet()) {
				String typeNo = testTypeMap.get(tableName);
				String sql = "SELECT * From " + tableName + " WHERE device_org='" + deviceOrg + "' AND android_ios='" + osType + "'";
				System.out.println(sql);
				PreparedStatement stmt = conn.prepareStatement(sql);
				ResultSet rs = stmt.executeQuery(sql);
				
				List<CSVDataBean> csvDataBeanList = new ArrayList<CSVDataBean>();
				while (rs.next()) {
					CSVDataBean csvDataBean = new CSVDataBean(tableName, typeNo);
					String time = DateUtil.getDate(Long.parseLong(rs.getString("time")), "yyyy-MM-dd HH:mm:ss");
					if (time != null) {
						csvDataBean.setTime(time);
					} else {
						csvDataBean.setTime(rs.getString("time"));
					}
					csvDataBean.setLogversion(rs.getString("logversion"));
					csvDataBean.setGps(rs.getString("gps"));
					csvDataBean.setLongitude(rs.getString("longitude"));
					csvDataBean.setLatitude(rs.getString("latitude"));
					csvDataBean.setModel(rs.getString("model"));
					csvDataBean.setImei(rs.getString("imei"));
					csvDataBean.setNetType(rs.getString("net_type"));
					csvDataBean.setSignalStrength(rs.getString("signal_strength"));
					csvDataBean.setLacTac(rs.getString("lac_tac"));
					csvDataBean.setCidPci(rs.getString("cid_pci"));
					
					if (tableName.equals("speed_test_new")) {
						csvDataBean.setProtocal(rs.getString("protocol"));
						csvDataBean.setAvgDown(rs.getString("avg_down"));
						csvDataBean.setAvgUp(rs.getString("avg_up"));
						csvDataBean.setMaxDown(rs.getString("max_down"));
						csvDataBean.setMaxUp(rs.getString("max_up"));
						csvDataBean.setDelay(rs.getString("delay"));
					}
					if (tableName.equals("web_browsing_new")) {
						csvDataBean.setWebSit(rs.getString("web_sit"));
						csvDataBean.setFullComplete(rs.getString("full_complete"));
						csvDataBean.setReference(rs.getString("reference"));
						csvDataBean.setEightyLoading(rs.getString("eighty_loading"));
						csvDataBean.setEightyRate(rs.getString("eighty_rate"));
						csvDataBean.setWebBrowseSuccessRate(rs.getString("success_rate"));
					}
					if (tableName.equals("http_test_new")) {
						String fileType = rs.getString("file_type");
						if (fileType != null && !"".equals(fileType)) {
							if (fileType.trim().equals("下载") || fileType.trim().toLowerCase().equals("download")) {
								csvDataBean.setDownloadUrl(rs.getString("url"));
								csvDataBean.setDownloadResourceSize(rs.getString("resource_size"));
								csvDataBean.setDownloadDuration(rs.getString("duration"));
								csvDataBean.setDownloadAvgRate(rs.getString("avg_rate"));
								csvDataBean.setDownloadMaxRate(rs.getString("max_rate"));
								csvDataBean.setDownloadAvgLatency(rs.getString("avg_latency"));
								csvDataBean.setDownloadSuccessRate(rs.getString("success_rate"));
							}
							if (fileType.trim().equals("上传") || fileType.trim().toLowerCase().equals("upload")) {
								csvDataBean.setUploadUrl(rs.getString("url"));
								csvDataBean.setUploadResourceSize(rs.getString("resource_size"));
								csvDataBean.setUploadDuration(rs.getString("duration"));
								csvDataBean.setUploadAvgRate(rs.getString("avg_rate"));
								csvDataBean.setUploadMaxRate(rs.getString("max_rate"));
								csvDataBean.setUploadAvgLatency(rs.getString("avg_latency"));
								csvDataBean.setUploadSuccessRate(rs.getString("success_rate"));
							}
						}
						
					}
					csvDataBeanList.add(csvDataBean);
				}
				open(fileName);
				for (CSVDataBean csvData : csvDataBeanList) {
					String[] str = new String[38];
					str[0] = csvData.getTime();
					str[1] = csvData.getLogversion();
					str[2] = csvData.getTableName();
					str[3] = csvData.getTypeNo();
//					str[4] = csvData.getGps();
					str[4] = csvData.getLongitude();
					str[5] = csvData.getLatitude();
					str[6] = csvData.getModel();
					str[7] = csvData.getImei();
					str[8] = csvData.getNetType();
					str[9] = csvData.getSignalStrength();
					str[10] = csvData.getLacTac();
					str[11] = csvData.getCidPci();
					
					str[12] = csvData.getProtocal();
					str[13] = csvData.getAvgDown();
					str[14] = csvData.getAvgUp();
					str[15] = csvData.getMaxDown();
					str[16] = csvData.getMaxUp();
					str[17] = csvData.getDelay();
					
					str[18] = csvData.getWebSit();
					str[19] = csvData.getFullComplete();
					str[20] = csvData.getReference();
					str[21] = csvData.getEightyLoading();
					str[22] = csvData.getEightyRate();
					str[23] = csvData.getWebBrowseSuccessRate();
					
					str[24] = csvData.getDownloadUrl();
					str[25] = csvData.getDownloadResourceSize();
					str[26] = csvData.getDownloadDuration();
					str[27] = csvData.getDownloadAvgRate();
					str[28] = csvData.getDownloadMaxRate();
					str[29] = csvData.getDownloadAvgLatency();
					str[30] = csvData.getDownloadSuccessRate();
					
					str[31] = csvData.getUploadUrl();
					str[32] = csvData.getUploadResourceSize();
					str[33] = csvData.getUploadDuration();
					str[34] = csvData.getUploadAvgRate();
					str[35] = csvData.getUploadMaxRate();
					str[36] = csvData.getUploadAvgLatency();
					str[37] = csvData.getUploadSuccessRate();
//					for (String s : str) {
//						System.out.print(s + ",");
//					}
//					System.out.println();
					write(str);
				}
				close();
			}
			return fileName;
		} catch (SQLException e) {
			e.printStackTrace();
			return "fail";
		}
	}
	
	private Connection getConn() {
		
		String driver = "com.mysql.jdbc.Driver";
		String url = "jdbc:mysql://218.206.179.109:3306/testdataanalyse";
		String user = "root";
		String password = "Bi123456";  //cmrictpdata
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
	
	public void open(String fileName) {
		
		csvFile = new File(fileName);
		try {
			if(!csvFile.exists()){
				csvFile.getParentFile().mkdirs();
				csvFile.createNewFile();
			}
//			writer = new BufferedWriter(new FileWriter(csvFile,true));
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
}