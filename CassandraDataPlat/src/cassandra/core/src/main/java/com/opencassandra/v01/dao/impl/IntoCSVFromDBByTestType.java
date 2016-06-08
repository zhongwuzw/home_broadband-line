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

public class IntoCSVFromDBByTestType {
	
//	private String filePath = "C:\\Users\\issuser\\Desktop\\云测试平台项目\\cvstest\\";  
	private String filePath = "/ctp/terminallog/wuxf/";  
	
	private File csvFile = null;
	
	private CsvWriter cwriter = null;
	
	private BufferedWriter writer = null;
	
	private static Map<String, String> tableNameAndCodeMap = new HashMap<String, String>();
	
	private static Map<String, String> testTypeMap = new HashMap<String, String>();
	
	private static Map<String, String> headerMap = new HashMap<String, String>();
	
	private static String publicStr = "testtime,logversion,TestType,TestTypeCode,longitude,latitude,model,IMEI,Network,SignalStrength(dBm),lac_tac,cid_pci";
	
	private static String speedTestStr = "Protocal,Average Download Speed(Mbps),Average Upload Speed(Mbps),Max Download Speed(Mbps),Max Upload Speed(Mbps),Latency(ms)";
	
	private static String downloadStr = "Download URL,Download File size(MB),Download duration(ms),Download Average Speed(Mbps),Download Max Speed(Mbps),Download Average Latency (ms),Download Success Ratio(%)";
	
	private static String uploadStr = "Upload URL,Upload File size(MB),Upload duration(ms),Upload Average Speed(Mbps),Upload Max Speed(Mbps),Upload Average Latency (ms),Upload Success Ratio(%)";
	
	private static String webBrowseStr = "WebBrowse URL,WebBrowse Loading Time(ms),WebBrowse Average Speed(Kbps),WebBrowse 80% Avg Time(ms),WebBrowse 80% Avg Speed(Mbps),WebBrowse Success Ratio(%)";
	
	private static String pingStr = "Domain Address,Max RTT(ms),Min RTT(ms),Avg RTT(ms),Ping Success Ratio(%)";
	
	private static String callStr = "Setupdelay(ms),success/fail,Call Success Ratio(%)";
	
	private static String csfbStr = "Setupdelay(ms),fallbackToCs(ms),backToLte(ms),CSFB Success Ratio(%)";
	
	private static String handOverStr = "Latency(ms)";
	
	private static String smsStr = "Sendingdelay(ms),success/fail,SMS Success Ratio(%)";
	
	private static String specialDevice1 = "EN_CMCC_CMPAK_DEFAULT";
	
	private static String specialDevice2 = "EN_CMCC_CMPAK_CEM";
	
	private static String specialPublicStr = "testtime,logversion,TestType,TestTypeCode,model,IMEI,Network,SignalStrength(dBm)";

	private static String specialSpeedTestStr = "Average Download Speed(Mbps),Average Upload Speed(Mbps),Max Download Speed(Mbps),Max Upload Speed(Mbps)";
	
	static {
		headerMap.put("Speed Test", speedTestStr);
		headerMap.put("Web Browsing", webBrowseStr);
		headerMap.put("Internet Downloading", downloadStr);
		headerMap.put("HTTP(upload)", uploadStr);
		headerMap.put("Ping", pingStr);
		headerMap.put("Call", callStr);
		headerMap.put("CSFB", csfbStr);
		headerMap.put("Handover", handOverStr);
		headerMap.put("SMS", smsStr);
	}
	
	static {
		tableNameAndCodeMap.put("speed_test_new", "01001");
		tableNameAndCodeMap.put("web_browsing_new", "02001");
		tableNameAndCodeMap.put("http_test_new", "03001");
		tableNameAndCodeMap.put("ping_new", "04002");
		tableNameAndCodeMap.put("call_test_new", "05001");
//		tableNameAndCodeMap.put("call_test_new", "05001.C");
		tableNameAndCodeMap.put("hand_over_new", "04006");
		tableNameAndCodeMap.put("sms_new", "05002");
	}
	
	static {
		testTypeMap.put("Speed Test", "speed_test_new");
		testTypeMap.put("Web Browsing", "web_browsing_new");
		testTypeMap.put("Internet Downloading", "http_test_new");
		testTypeMap.put("HTTP(upload)", "http_test_new");
		testTypeMap.put("Ping", "ping_new");
		testTypeMap.put("Call", "call_test_new");
		testTypeMap.put("CSFB", "call_test_new");
		testTypeMap.put("Handover", "hand_over_new");
		testTypeMap.put("SMS", "sms_new");
//		testTypeMap.put("Streaming", "");
//		testTypeMap.put("VoLTE", "");
	}
	
	JSONObject requestBody = null;
	
	public IntoCSVFromDBByTestType(JSONObject requestBody) {
		this.requestBody = requestBody;
	} 
	
	public IntoCSVFromDBByTestType() {
		
	}
	
	public Map<String, Object> searchFromDB() throws Exception {
		
//		String deviceOrg = "CMCC_CMRI_DEPT_CESHI_YEWUSHI";
//		String osType = "android";
//		String testType = "Speed Test,Web Browsing,Internet Downloading,HTTP(upload),Ping,CSFB,Call,Hand Over,SMS";
		String deviceOrg = "";
		String osType = "";
		String testType = "";
		String callback = "";
		Map<String, Object> list = new HashMap<String, Object>();
		
		if (requestBody != null) {
			deviceOrg = (String)requestBody.get("deviceOrg");
			osType = (String)requestBody.get("osType");
			testType = (String)requestBody.get("testType");
			callback = (String)requestBody.get("callback");
			System.out.println(callback);
		}
		System.out.println("=========deviceOrg:" + deviceOrg + ", =========osType:" + osType);
		String result = "fail";
		if (testType != null && !"".equals(testType)) {
			String[] testTypes = testType.split(",");
			
			if (deviceOrg != null && !"".equals(deviceOrg) && osType != null && !"".equals(osType)) {
				System.out.println("========进入方法========");
				result = intoCSVFromDB(deviceOrg, osType, testTypes);
				if(callback != null && !"".equals(callback)){
		           list.put("callback",""+callback+""+result+"");  
		        }
			}
		}
		
		System.out.println("============result:" +result);
		list.put("result", result);
		System.out.println("执行完毕");
		return list;
	}
	
	public static void main(String[] args) {
		
//		String testType = "kfjdaksfj";
//		String[] testTypes = testType.split(",");
//		for (String s : testTypes) {
//			System.out.println(s);
//		}
//		System.out.println(testTypes.length);
		IntoCSVFromDBByTestType test = new IntoCSVFromDBByTestType();
		try {
			test.searchFromDB();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public String intoCSVFromDB(String deviceOrg, String osType, String[] testTypes) {
		
		Connection conn = getConn();
		try {
			
			List<CSVDataBean> csvDataBeanList = new ArrayList<CSVDataBean>();
			StringBuffer headerStr = new StringBuffer();
			if (deviceOrg.equals(specialDevice1) || deviceOrg.equals(specialDevice2)) {
				headerStr.append(specialPublicStr);
			} else {
				headerStr.append(publicStr);
			}
			
			for (String testType : testTypes) {
				if (testType.trim().equals("Speed Test")) {
					if (deviceOrg.equals(specialDevice1) || deviceOrg.equals(specialDevice2)) {
						headerStr.append("," +specialSpeedTestStr);
					} else {
						headerStr.append("," + headerMap.get(testType.trim()));
					}
				} else {
					headerStr.append("," + headerMap.get(testType.trim()));
				}
				
				String tableName = testTypeMap.get(testType.trim());
			    if (tableName == null) {
			    	continue;
			    }
				String typeNo = tableNameAndCodeMap.get(tableName);
				String sql = "SELECT * From " + tableName + " WHERE device_org='" + deviceOrg + "' AND android_ios='" + osType + "'";
				System.out.println(sql);
				PreparedStatement stmt = conn.prepareStatement(sql);
				ResultSet rs = stmt.executeQuery(sql);
				
				while (rs.next()) {
					CSVDataBean csvDataBean = new CSVDataBean();
					if (testType.trim().equals("Speed Test")) {
						csvDataBean = getCSVDataBean(tableName, typeNo,	rs);
						if (!deviceOrg.equals(specialDevice1) && !deviceOrg.equals(specialDevice2)) {
							csvDataBean.setProtocal(rs.getString("protocol"));
						}
						csvDataBean.setAvgDown(rs.getString("avg_down"));
						csvDataBean.setAvgUp(rs.getString("avg_up"));
						csvDataBean.setMaxDown(rs.getString("max_down"));
						csvDataBean.setMaxUp(rs.getString("max_up"));
						csvDataBean.setDelay(rs.getString("delay"));
						csvDataBeanList.add(csvDataBean);
					}
					if (testType.trim().equals("Web Browsing")) {
						csvDataBean = getCSVDataBean(tableName, typeNo,	rs);
						csvDataBean.setWebSit(rs.getString("web_sit"));
						csvDataBean.setFullComplete(rs.getString("full_complete"));
						csvDataBean.setReference(rs.getString("reference"));
						csvDataBean.setEightyLoading(rs.getString("eighty_loading"));
						csvDataBean.setEightyRate(rs.getString("eighty_rate"));
						csvDataBean.setWebBrowseSuccessRate(rs.getString("success_rate"));
						csvDataBeanList.add(csvDataBean);
					}
					
					if (testType.trim().equals("Internet Downloading")) {
						String fileType = rs.getString("file_type");
						if (fileType != null && !"".equals(fileType)) {
							if (fileType.trim().equals("下载") || fileType.trim().toLowerCase().equals("download")) {
								csvDataBean = getCSVDataBean(tableName, typeNo,	rs);
								csvDataBean.setDownloadUrl(rs.getString("url"));
								csvDataBean.setDownloadResourceSize(rs.getString("resource_size"));
								csvDataBean.setDownloadDuration(rs.getString("duration"));
								csvDataBean.setDownloadAvgRate(rs.getString("avg_rate"));
								csvDataBean.setDownloadMaxRate(rs.getString("max_rate"));
								csvDataBean.setDownloadAvgLatency(rs.getString("avg_latency"));
								csvDataBean.setDownloadSuccessRate(rs.getString("success_rate"));
								csvDataBeanList.add(csvDataBean);
							}
						}
					} 
					if (testType.trim().equals("HTTP(upload)")) {
						String fileType = rs.getString("file_type");
						if (fileType != null && !"".equals(fileType)) {
							if (fileType.trim().equals("上传") || fileType.trim().toLowerCase().equals("upload")) {
								csvDataBean = getCSVDataBean(tableName, typeNo,	rs);
								csvDataBean.setUploadUrl(rs.getString("url"));
								csvDataBean.setUploadResourceSize(rs.getString("resource_size"));
								csvDataBean.setUploadDuration(rs.getString("duration"));
								csvDataBean.setUploadAvgRate(rs.getString("avg_rate"));
								csvDataBean.setUploadMaxRate(rs.getString("max_rate"));
								csvDataBean.setUploadAvgLatency(rs.getString("avg_latency"));
								csvDataBean.setUploadSuccessRate(rs.getString("success_rate"));
								csvDataBeanList.add(csvDataBean);
							}
						}
					}
					
					if (testType.trim().equals("Ping")) {
						csvDataBean = getCSVDataBean(tableName, typeNo,	rs);
						csvDataBean.setDomainAddress(rs.getString("domain_address"));
						csvDataBean.setMaxRtt(rs.getString("max_rtt"));
						csvDataBean.setMinRtt(rs.getString("min_rtt"));
						csvDataBean.setAvgRtt(rs.getString("avg_rtt"));
						csvDataBean.setPingSuccessRate(rs.getString("success_rate"));
						csvDataBeanList.add(csvDataBean);
					}
					if (testType.trim().equals("CSFB")) {
						String csfb = rs.getString("csfb");
						if (csfb != null && !"".equals(csfb)) {
							if (csfb.equals("1")) {
								csvDataBean = getCSVDataBean(tableName, typeNo,	rs);
								csvDataBean.setCsfbSetupdelay(rs.getString("setup_delay"));
								csvDataBean.setCsfbFallbackToCs(rs.getString("fallbacktocs"));
								csvDataBean.setBackToLte(rs.getString("backtolte"));
								csvDataBean.setCsfbSuccessRate(rs.getString("success_rate"));
								csvDataBeanList.add(csvDataBean);
							}
						}
					}
					if (testType.trim().equals("Call")) {
						csvDataBean = getCSVDataBean(tableName, typeNo,	rs);
						csvDataBean.setCallSetupdely(rs.getString("setup_delay"));
						csvDataBean.setCallSuccessOrFail(rs.getString("success_failure"));
						csvDataBean.setCallSuccessRate(rs.getString("success_rate"));
						csvDataBeanList.add(csvDataBean);
					}
					if (testType.trim().equals("Handover")) {
						csvDataBean = getCSVDataBean(tableName, typeNo,	rs);
						csvDataBean.setHandOverAvgLatency(rs.getString("avg_latency"));
						csvDataBeanList.add(csvDataBean);
					}
					if (testType.trim().equals("SMS")) {
						csvDataBean = getCSVDataBean(tableName, typeNo,	rs);
						csvDataBean.setSmsSendingDelay(rs.getString("sending_delay"));
						csvDataBean.setSmsSuccessOrFail(rs.getString("success_failure"));
						csvDataBean.setSmsSuccessRate(rs.getString("success_rate"));
						csvDataBeanList.add(csvDataBean);
					}
					
				}
			}
			
			String fileName = filePath + deviceOrg + "-" + osType + "-" + DateUtil.getDate(System.currentTimeMillis(), "yyyyMMddHHmmss") + ".csv";
			String[] header = headerStr.toString().split(",");
			open(fileName);
			write(header);
			close();
			
			open(fileName);
			for (CSVDataBean csvData : csvDataBeanList) {
				String[] str = new String[header.length];
				int falg = 12;
				if (deviceOrg.equals(specialDevice1) || deviceOrg.equals(specialDevice2)) {
					falg = 8;
					str[0] = csvData.getTime();
					str[1] = csvData.getLogversion();
					str[2] = csvData.getTableName();
					str[3] = csvData.getTypeNo();
					str[4] = csvData.getModel();
					str[5] = csvData.getImei();
					str[6] = csvData.getNetType();
					str[7] = csvData.getSignalStrength();
				} else {
					str[0] = csvData.getTime();
					str[1] = csvData.getLogversion();
					str[2] = csvData.getTableName();
					str[3] = csvData.getTypeNo();
					str[4] = csvData.getLongitude();
					str[5] = csvData.getLatitude();
					str[6] = csvData.getModel();
					str[7] = csvData.getImei();
					str[8] = csvData.getNetType();
					str[9] = csvData.getSignalStrength();
					str[10] = csvData.getLacTac();
					str[11] = csvData.getCidPci();
				}
				
				for (int i = falg; i < header.length; i++) {
					for (String testType : testTypes) {
						if (testType.trim().equals("Speed Test")) {
							if (deviceOrg.equals(specialDevice1) || deviceOrg.equals(specialDevice2)) {
								str[i] = csvData.getAvgDown();
								str[i+1] = csvData.getAvgUp();
								str[i+2] = csvData.getMaxDown();
								str[i+3] = csvData.getMaxUp();
								i = i + 4;
							} else {
								str[i] = csvData.getProtocal();
								str[i+1] = csvData.getAvgDown();
								str[i+2] = csvData.getAvgUp();
								str[i+3] = csvData.getMaxDown();
								str[i+4] = csvData.getMaxUp();
								str[i+5] = csvData.getDelay();
								i = i + 6;
							}
						}
						
						if (testType.trim().equals("Web Browsing")) {
							str[i] = csvData.getWebSit();
							str[i+1] = csvData.getFullComplete();
							str[i+2] = csvData.getReference();
							str[i+3] = csvData.getEightyLoading();
							str[i+4] = csvData.getEightyRate();
							str[i+5] = csvData.getWebBrowseSuccessRate();
							i = i + 6;
						}
						if (testType.trim().equals("Internet Downloading")) {
							str[i] = csvData.getDownloadUrl();
							str[i+1] = csvData.getDownloadResourceSize();
							str[i+2] = csvData.getDownloadDuration();
							str[i+3] = csvData.getDownloadAvgRate();
							str[i+4] = csvData.getDownloadMaxRate();
							str[i+5] = csvData.getDownloadAvgLatency();
							str[i+6] = csvData.getDownloadSuccessRate();
							i = i + 7;
						}
						if (testType.trim().equals("HTTP(upload)")) {
							str[i] = csvData.getUploadUrl();
							str[i+1] = csvData.getUploadResourceSize();
							str[i+2] = csvData.getUploadDuration();
							str[i+3] = csvData.getUploadAvgRate();
							str[i+4] = csvData.getUploadMaxRate();
							str[i+5] = csvData.getUploadAvgLatency();
							str[i+6] = csvData.getUploadSuccessRate();
							i = i + 7;
						}
						if (testType.trim().equals("Ping")) {
							str[i] = csvData.getDomainAddress();
							str[i+1] = csvData.getMaxRtt();
							str[i+2] = csvData.getMinRtt();
							str[i+3] = csvData.getAvgRtt();
							str[i+4] = csvData.getPingSuccessRate();
							i = i + 5;
						}
						if (testType.trim().equals("Call")) {
							str[i] = csvData.getCallSetupdely();
							str[i+1] = csvData.getCallSuccessOrFail();
							str[i+2] = csvData.getCallSuccessRate();
							i = i + 3;
						}
						if (testType.trim().equals("CSFB")) {
							str[i] = csvData.getCsfbSetupdelay();
							str[i+1] = csvData.getCsfbFallbackToCs();
							str[i+2] = csvData.getBackToLte();
							str[i+3] = csvData.getCsfbSuccessRate();
							i = i + 4;
						}
						if (testType.trim().equals("Handover")) {
							str[i] = csvData.getHandOverAvgLatency();
							i = i + 1;
						}
						if (testType.trim().equals("SMS")) {
							str[i] = csvData.getSmsSendingDelay();
							str[i+1] = csvData.getSmsSuccessOrFail();
							str[i+2] = csvData.getSmsSuccessRate();
							i = i + 3;
						}
					}
				}
				write(str);
			}
			close();
			return fileName;
		} catch (SQLException e) {
			e.printStackTrace();
			return "fail";
		}
	}

	private CSVDataBean getCSVDataBean(String tableName, String typeNo,
			ResultSet rs) throws SQLException {
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
		return csvDataBean;
	}
	
	private Connection getConn() {
		
		String driver = "com.mysql.jdbc.Driver";
//		String url = "jdbc:mysql://192.168.85.233:3306/testdataanalyse";
//		String user = "root";
//		String password = "cmrictpdata";  
		String url = "jdbc:mysql://218.206.179.109:3306/testdataanalyse";
		String user = "root";
		String password = "Bi123456"; 
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