package com.opencassandra.v01.dao.impl;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URL;
import java.net.URLConnection;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.text.Format;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.sf.json.JSONObject;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.output.FileWriterWithEncoding;
import org.apache.commons.lang.StringUtils;

import com.csvreader.CsvWriter;
import com.mysql.jdbc.DatabaseMetaData;
import com.opencassandra.descfile.ConfParser;
import com.sun.org.apache.xpath.internal.operations.And;


public class InsertMysqlByDataToGPS{
	Statement statement;
	Connection conn;
	StringBuffer sql = new StringBuffer();
	private static String ak = ConfParser.ak;
	private static double LNGinterval = 0;//ConfParser.LNGinterval;
	private static double LATinterval = 0;//ConfParser.LATinterval;
	private static String mapLevel[] = ConfParser.mapLevel;
	private static String dataStart = ConfParser.dataStart;
	private static UpdateGPSDateDao updateGPSDateDao = new UpdateGPSDateDao();
	private File csvFile = null;
	private CsvWriter cwriter = null;
	private BufferedWriter writer = null;
	InsertCSVByDataToGPS insertCSVByDataToGPS = new InsertCSVByDataToGPS();
	double m_maxspeed = 150;
	double m_opacitymax = 0.1;
	double m_opacitymin = 0.8;
	double m_maxtestnum = 20;
	
	public static void main(String[] args) {
		/*Format fm = new DecimalFormat("#.######");
		String gpsStr = "123.098053E 33.169751N";
		String [] str = transGpsPoint(gpsStr);
		String longitude = str[0];
		String latitude = str[1];
		System.out.print(longitude+"   ");
		System.out.println(latitude);
		double[] em = Earth2Mars.transform(Double.parseDouble(longitude), Double.parseDouble(latitude));
		double[] bf = BdFix.bd_encrypt(em[0], em[1]);
		double longitudeNum = bf[0];
		double latitudeNum = bf[1];
		System.out.print("longitudeNum:"+longitudeNum);
		System.out.println(";   latitudeNum:"+latitudeNum);
		double lng = 0.0;
		double lat = 0.0;
		BigDecimal LNGBD = new BigDecimal(LNGinterval);
		BigDecimal LATBD = new BigDecimal(LATinterval);
		BigDecimal longBD = new BigDecimal(longitudeNum);
		BigDecimal latBD = new BigDecimal(latitudeNum);
		
		lng = new BigDecimal(longBD.divide(LNGBD,6).intValue()).multiply(LNGBD).add(LNGBD).divide(new BigDecimal(2.0)).doubleValue();
		lat = new BigDecimal(latBD.divide(LATBD,6).intValue()).multiply(LATBD).add(LATBD).divide(new BigDecimal(2.0)).doubleValue();
		System.out.print("lng:"+fm.format(lng));
		System.out.println(";     lat:"+fm.format(lat));*/
//		String result = "";
//		try {
//			result = testPost(str[0], str[1]);
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		System.out.println(subLalo1(result));
		InsertMysqlByDataToGPS insert = new InsertMysqlByDataToGPS();
//		boolean flag = insert.queryTableExist("'http_20150201_1_1'");
//		System.out.println(flag);
//		Integer i = 1;
//		System.out.println(i.toString().length());
//		double lng = 0.0;
//		double lat = 0.0;
//		double longitudeNum = 116.4054757;
//		double latitudeNum = 39.97518239;
//		Format fm = new DecimalFormat("#.######");
//		for (int i = 0; i < mapLevel.length; i++) {
//			List list = insert.getLNGLATByLevel(mapLevel[i]);
//			LNGinterval = (Double) list.get(0);
//			LATinterval = (Double) list.get(1);
//			lng = ((int) (longitudeNum / LNGinterval)) * LNGinterval
//			+ LNGinterval / 2;
//			lat = ((int) (latitudeNum / LATinterval)) * LATinterval
//			+ LATinterval / 2;
//			System.out.println(LNGinterval+","+LATinterval+","+fm.format(lng)+","+fm.format(lat));
//		}
	}

	public void start() {
		String driver = "com.mysql.jdbc.Driver";
//		String url = "jdbc:mysql://192.168.85.233:3306/testdataanalyse";
		String url = ConfParser.url;
		String user = ConfParser.user;
//		String password = "cmrictpdata";
		String password = ConfParser.password;
		try {
			Class.forName(driver);
			conn = DriverManager.getConnection(url, user, password);
			statement = conn.createStatement();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void close() throws SQLException {
		if(statement!=null){
			statement.close();
		}
		if (conn != null) {
			conn.close();
		}
	}
	public boolean insert(String sql) {
		
		System.out.println("sql:" + sql);
		boolean flag = false;
		if(sql.isEmpty()){
			flag = false;
		}else{
			try {
				flag = statement.execute(sql);
			} catch (SQLException e) {
				e.printStackTrace();
				return false;
			}	
		}
		return flag;
	}
	
	public void insertMysqlDB(Map dataMap,String keyspace,String numType,File file,String detailreport){
		System.out.println("insertMysqlDB--------------"+new Date().toLocaleString());
		StringBuffer sql = new StringBuffer("");
		String destFilePath = ConfParser.backReportPath;
		String filePath = file.getAbsolutePath();
		String dataOrg = dataMap.get("dataOrg")==null?"":dataMap.get("dataOrg").toString();
		String dateYear = 1900+new Date().getYear()+"";//获取当前年份
		String dateMonth = new Date().getMonth()+1+"";//获取当前月份
		if(dateMonth.length()==1){
			dateMonth = "0"+dateMonth;
		}
		String dateNow = dateYear+dateMonth;
		//网格化入库 为当天时间 报告解析解除注释
//		if(dataOrg.isEmpty() || dataOrg.equals(dateNow)){
			dataOrg = dateNow;
//		}
//		String dataOrg = filePath.replace(ConfParser.org_prefix, "");
//		while(dataOrg.startsWith(File.separator)){
//			dataOrg = dataOrg.substring(1, dataOrg.length());
//		}
//		dataOrg = dataOrg.substring(dataOrg.indexOf(File.separator)+1, dataOrg.length());
//		dataOrg = dataOrg.substring(0, dataOrg.indexOf(File.separator));
//		String path1 = filePath.substring(0, filePath.lastIndexOf("."));
//		String filenameorg = path1.substring(path1.lastIndexOf("."), path1.length());
//		String []files = new String[]{filenameorg,".detail",".deviceInfo",".monitor"};
//		String []filepaths = new String[]{filePath};
/*		if(detailreport.equals("")){
			for (int i = 0; i < files.length; i++) {
				String newfilePath = file.getAbsolutePath().replace(filenameorg, files[i]);
				filepaths[i] = newfilePath;
				File newFile = new File(newfilePath);
				if(newFile.exists()){
					detailreport += "1";
				}else{
					detailreport += "0";
				}
			}	
		}
*/		
		boolean flag = false;
		if(!numType.isEmpty()){
			if(numType.equals("01001")){
				System.out.println("正在处理文件："+file.getAbsolutePath());
				insertSpeedTest(dataMap,keyspace,numType,detailreport,dataOrg);
			}else if(numType.equals("03001") || numType.equals("03011") ){
				System.out.println("正在处理文件："+file.getAbsolutePath());
				insertHTTP(dataMap,keyspace,numType,detailreport,dataOrg);
			}else if (numType.equals("04002")) {
				System.out.println("正在处理文件："+file.getAbsolutePath());
				insertPING(dataMap,keyspace,numType,detailreport,dataOrg);
			}
			
		/*try {
//			insert(sql.toString());
//			flag = true;
			for (int i = 0; i < filepaths.length; i++) {
				String newfilePath = filepaths[i];
				if (newfilePath == null || newfilePath.isEmpty()) {
					continue;
				}
				File destFile = new File(newfilePath.replace(ConfParser.org_prefix, destFilePath));
				File newFile = new File(newfilePath);
				if(!newFile.exists()){
					continue;
				}
				if(destFile.exists()){
					destFile.delete();
				}
				if(!destFile.exists()){
					if (!destFile.getParentFile().exists()) {
						destFile.getParentFile().mkdirs();
					}
					try {
						destFile.createNewFile();
						FileUtils.copyFile(newFile, destFile);	
					} catch (Exception e) {
						e.printStackTrace();
					}finally{
						newFile.delete();	
					}
				}
			}
			
		} catch (Exception e) {
			flag = false;
			System.out.println("执行sql语句错误");
			e.printStackTrace();
		} finally{
			try {
				close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}*/
		System.out.println("更新数据："+flag);
		}
	}
	
	/**
	 * 插入speedTest数据
	 * @param dataMap 拼装过后的csv问价数据
	 * @param keyspace CMCC_GUANGXI_PINZHIBU23G
	 * @param numType 05001 确定数据要插入的表
	 * @param detailreport
	 * @param dataOrg 当前时间
	 * @return void
	 */
	public void insertSpeedTest(Map dataMap,String keyspace,String numType,String detailreport,String dataOrg){
		System.out.println("insertSpeedTest-----------------"+new Date().toLocaleString());
		boolean flag = false;
		List nameList = new ArrayList();
		List valueList = new ArrayList();
		Map map = new HashMap();
		Set keySet = dataMap.keySet();
		Iterator iter = keySet.iterator();
		Format fm = new DecimalFormat("#.######");
		String longitude = "";
		String latitude = "";
		String top_rate = "";//
		String down_rate = "";
		String location = "";//测试地点
		String gpsStr = "";
		
		String spaceGpsStr = "";//测试地点
		//获取GPS位置信息----start
		if(dataMap.containsKey("GPS信息")){
			gpsStr = (String)(dataMap.get("GPS信息")==null?"":dataMap.get("GPS信息"));
		}else if(dataMap.containsKey("GPS位置信息")){
			gpsStr = (String)(dataMap.get("GPS位置信息")==null?"":dataMap.get("GPS位置信息"));
		}else if(dataMap.containsKey("GPS位置")){
			gpsStr = (String)(dataMap.get("GPS位置")==null?"":dataMap.get("GPS位置"));
		}else if(dataMap.containsKey("测试位置")){
			gpsStr = (String)(dataMap.get("测试位置")==null?"":dataMap.get("测试位置"));
		}else if(dataMap.containsKey("测试GPS位置")){
			gpsStr = (String)(dataMap.get("测试GPS位置")==null?"":dataMap.get("测试GPS位置"));
		}else{
			gpsStr = "";
		}
		if(gpsStr.equals("--")){
//			return new StringBuffer();
			return ;
		}
//		map.put("gps", gpsStr);
		if(gpsStr.contains(" ")){
			String[] gps = transGpsPoint(gpsStr);
			if(gps!=null && gps[0]!=null && gps[1]!=null){
				longitude = gps[0];
				latitude = gps[1];
			}else{
				System.out.println("经纬度错误 不插入此数据");
				return ;
			}
		}
		if(longitude.isEmpty() || latitude.isEmpty()){
			System.out.println("经纬度错误 不插入此数据");
			return ;
		}
		//这段代码算出来的是什么？
		double lng[] = new double[mapLevel.length];
		double lat[] = new double[mapLevel.length];
		double[] em = Earth2Mars.transform(Double.parseDouble(longitude), Double.parseDouble(latitude));
		double[] bf = BdFix.bd_encrypt(em[0], em[1]);
		double longitudeNum = bf[0];
		double latitudeNum = bf[1];
		
		String table[] = new String[mapLevel.length];
		StringBuffer sql[] = new StringBuffer[mapLevel.length];
		for (int i = 0; i < mapLevel.length; i++) {
			List list = this.getLNGLATByLevel(mapLevel[i]);
			LNGinterval = (Double) list.get(0);
			LATinterval = (Double) list.get(1);
			lng[i] = ((int) (longitudeNum / LNGinterval)) * LNGinterval
					+ LNGinterval / 2;
			lat[i] = ((int) (latitudeNum / LATinterval)) * LATinterval
					+ LATinterval / 2;
			// map.put("longitude",longitudeNum);
			// map.put("latitude",latitudeNum);
			while (iter.hasNext()) {
				String key = iter.next() + "";
				if (key == null || key.isEmpty()) {
					continue;
				}
				if("网络类型".equals(key) || "网络制式".equals(key) || "network type".equals(key.toLowerCase())){
					String net_type = (String) (dataMap.get(key)==null?"":dataMap.get(key));
					if(!net_type.contains("LTE") && !net_type.contains("lte")){
						return ;
					}
				}
				if("上行速率".equals(key)||"上行速率(Mbps)".equals(key) || "Up Speed".equals(key) || "Up Speed(Mbps)".equals(key) || "Up Speed(Kbps)".equals(key) || "Upload Speed".equals(key) || "Upload Speed(Mbps)".equals(key)){
					top_rate = (String) (dataMap.get(key)==null?"":dataMap.get(key));
					try {
						double top_num = 0;
						if(top_rate.toLowerCase().endsWith("(kbps)") || top_rate.toLowerCase().endsWith("（kbps）") ){
							top_rate = top_rate.toLowerCase().replaceAll("(kbps)", "").replaceAll("（kbps）","");
							top_num = Double.parseDouble(top_rate)/1024;
						}else{
							top_rate = top_rate.toLowerCase().replaceAll("(mbps)", "").replaceAll("（mbps）","");
							top_num = Double.parseDouble(top_rate);
						}
						if(top_num>100 || top_num<0.1){
							System.out.println("上行速率不在适合范围内");
							return;
						}else{
							top_rate = top_num +"";
							map.put("top_rate", top_rate);	
						}
					} catch (Exception e) {
						System.out.println("top_rate 不为纯数字");
						e.printStackTrace();
						return;
					}
				}else if("下行速率".equals(key)||"下行速率(Mbps)".equals(key)  || "Down Speed".equals(key) || "Down Speed(Mbps)".equals(key) || "Down Speed(Kbps)".equals(key) || "Download Speed".equals(key) || "Download Speed(Mbps)".equals(key)){
					down_rate = (String) (dataMap.get(key)==null?"":dataMap.get(key));
					try {
						double down_num = 0 ;
						if(down_rate.toLowerCase().endsWith("(kbps)") || down_rate.toLowerCase().endsWith("（kbps）") ){
							down_rate = down_rate.toLowerCase().replaceAll("(kbps)", "").replaceAll("（kbps）","");
							down_num = Double.parseDouble(down_rate)/1024;
						}else{
							down_rate = down_rate.toLowerCase().replaceAll("(mbps)", "").replaceAll("（mbps）","");
							down_num = Double.parseDouble(down_rate);
						}
						if(down_num>100 || down_num<0.1){
							System.out.println("下行速率不在适合范围内");
							return;
						}else{
							down_rate = down_num +"";
							map.put("down_rate", down_rate);	
						}
					} catch (Exception e) {
						System.out.println("down_rate 不为纯数字");
						e.printStackTrace();
						return;
					}
				}
			}
		}
		for (int j = 0; j < mapLevel.length; j++) {
			map.put("lng", fm.format(lng[j]));
			map.put("lat", fm.format(lat[j]));
			map.put("down_rate", down_rate);
			map.put("top_rate",top_rate);
			// 拼接表名

			if (numType.equals("01001")) {
				table[j] = "speed_test_" + dataOrg + "_" + mapLevel[j];
			} else if (numType.equals("02001")) {
				table[j] = "web_browsing";
			} else if (numType.equals("02011")) {
				table[j] = "video_test";
			} else if (numType.equals("03001") || numType.equals("03011") ) {
//				table[j] = "http_test_" + dataOrg + "_" + mapLevel[j];
				table[j] = "http_" + dataOrg + "_" + mapLevel[j];
			} else if (numType.equals("04002")) {
				table[j] = "ping_" + dataOrg + "_" + mapLevel[j];
			} else if (numType.equals("04006")) {
				table[j] = "hand_over";
			} else if (numType.equals("05001")) {
				table[j] = "call_test";
			} else if (numType.equals("05002")) {
				table[j] = "sms";
			} else {
//				return new StringBuffer("");
				return ;
			}
			appendSql(map, numType, dataOrg, table[j]);
		}
		try {
			if(ConfParser.writeCsvOpen.equals("open")){
				insertCSVByDataToGPS.insertCsvGPSData(dataMap, numType, keyspace, detailreport);	
			}
			System.out.println(ConfParser.url);
			start();
			for (int j = 0; j < sql.length; j++) {
				if (sql[j].toString().isEmpty()) {
					flag = false;
				} else {
					System.out.println("insertSpeedTest  插入数据Start--------------："+new Date().toLocaleString());
					System.out.println(sql[j].toString());
					insert(sql[j].toString());
					System.out.println("insertSpeedTest  插入数据END--------------："+new Date().toLocaleString());
					flag = true;
				}
			}
		} catch (Exception e) {
			flag = false;
			e.printStackTrace();
		} finally{
			try {
				close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		
	}
	
	/**
	 * 插入HTTP数据
	 */
	public void insertHTTP(Map dataMap,String keyspace,String numType,String detailreport,String dataOrg){
		System.out.println("insertHTTP-----------------"+new Date().toLocaleString());
		boolean flag = false;
		List nameList = new ArrayList();
		List valueList = new ArrayList();
		Map map = new HashMap();
		Set keySet = dataMap.keySet();
		Iterator iter = keySet.iterator();
		Format fm = new DecimalFormat("#.######");
		String longitude = "";
		String latitude = "";
		String avg_rate = "";//平均速率
		String location = "";//测试地点
		String gpsStr = "";
		String file_type = "";
		
		String spaceGpsStr = "";//测试地点
		//获取GPS位置信息----start
		if(dataMap.containsKey("GPS信息")){
			gpsStr = (String)(dataMap.get("GPS信息")==null?"":dataMap.get("GPS信息"));
		}else if(dataMap.containsKey("GPS位置信息")){
			gpsStr = (String)(dataMap.get("GPS位置信息")==null?"":dataMap.get("GPS位置信息"));
		}else if(dataMap.containsKey("GPS位置")){
			gpsStr = (String)(dataMap.get("GPS位置")==null?"":dataMap.get("GPS位置"));
		}else if(dataMap.containsKey("测试位置")){
			gpsStr = (String)(dataMap.get("测试位置")==null?"":dataMap.get("测试位置"));
		}else if(dataMap.containsKey("测试GPS位置")){
			gpsStr = (String)(dataMap.get("测试GPS位置")==null?"":dataMap.get("测试GPS位置"));
		}else{
			gpsStr = "";
		}
		if(gpsStr.equals("--")){
//			return new StringBuffer();
			return ;
		}
//		map.put("gps", gpsStr);
		if(gpsStr.contains(" ")){
			String[] gps = transGpsPoint(gpsStr);
			if(gps!=null && gps[0]!=null && gps[1]!=null){
				longitude = gps[0];
				latitude = gps[1];
			}else{
				System.out.println("经纬度错误 不插入此数据");
				return ;
			}
		}
		if(longitude.isEmpty() || latitude.isEmpty()){
			System.out.println("经纬度错误 不插入此数据");
			return ;
		}
		double lng[] = new double[mapLevel.length];
		double lat[] = new double[mapLevel.length];
		double[] em = Earth2Mars.transform(Double.parseDouble(longitude), Double.parseDouble(latitude));
		double[] bf = BdFix.bd_encrypt(em[0], em[1]);
		double longitudeNum = bf[0];
		double latitudeNum = bf[1];
		
		String table[] = new String[mapLevel.length];
		StringBuffer sql[] = new StringBuffer[mapLevel.length];
		for (int i = 0; i < mapLevel.length; i++) {
			List list = this.getLNGLATByLevel(mapLevel[i]);
			LNGinterval = (Double) list.get(0);
			LATinterval = (Double) list.get(1);
			lng[i] = ((int) (longitudeNum / LNGinterval)) * LNGinterval
					+ LNGinterval / 2;
			lat[i] = ((int) (latitudeNum / LATinterval)) * LATinterval
					+ LATinterval / 2;
			// map.put("longitude",longitudeNum);
			// map.put("latitude",latitudeNum);
			while (iter.hasNext()) {
				String key = iter.next() + "";
				if (key == null || key.isEmpty()) {
					continue;
				}
				if ("平均速率(Kbps)".equals(key) || "平均速率(Mbps)".equals(key)
						|| "平均速率（Mbps）".equals(key)
						|| "average speed (mbps)".equals(key.toLowerCase())
						|| "average speed (kbps)".equals(key.toLowerCase())
						|| "avg speed(kbps)".equals(key.toLowerCase())
						|| "avg speed(mbps)".equals(key.toLowerCase())) {
					avg_rate = (String) (dataMap.get(key) == null ? ""
							: dataMap.get(key));
					map.put("avg_rate", avg_rate);
					try {
						double avg_num = Double.parseDouble(avg_rate);
						if(avg_num>100 || avg_num<0.1){
							System.out.println("平均速率不在适合范围内");
							return;
						}else{
							map.put("avg_rate", avg_rate);	
						}
					} catch (Exception e) {
						System.out.println("avg_rate 不为纯数字");
						return;
					}
				}else if("类型".equals(key) || "Type".equals(key) || "Test Type".equals(key) || "测试类型".equals(key)){
					file_type = (String) (dataMap.get(key)==null?"":dataMap.get(key));
					if(file_type.equals("上传") || file_type.toLowerCase().equals("upload") || file_type.toLowerCase().equals("post")){
						return ;
					}
				}
			}
		}
		for (int j = 0; j < mapLevel.length; j++) {
			map.put("lng", fm.format(lng[j]));
			map.put("lat", fm.format(lat[j]));
			map.put("avg_rate", avg_rate);
			// 拼接表名

			if (numType.equals("01001")) {
				table[j] = "speed_test";
			} else if (numType.equals("02001")) {
				table[j] = "web_browsing";
			} else if (numType.equals("02011")) {
				table[j] = "video_test";
			} else if (numType.equals("03001") || numType.equals("03011") ) {
//				table[j] = "http_test_" + dataOrg + "_" + mapLevel[j];
				table[j] = "http_" + dataOrg + "_" + mapLevel[j];
			} else if (numType.equals("04002")) {
				table[j] = "ping_" + dataOrg + "_" + mapLevel[j];
			} else if (numType.equals("04006")) {
				table[j] = "hand_over";
			} else if (numType.equals("05001")) {
				table[j] = "call_test";
			} else if (numType.equals("05002")) {
				table[j] = "sms";
			} else {
//				return new StringBuffer("");
				return ;
			}
			appendSql(map, numType, dataOrg, table[j]);
		}
		try {
			if(ConfParser.writeCsvOpen.equals("open")){
				insertCSVByDataToGPS.insertCsvGPSData(dataMap, numType, keyspace, detailreport);	
			}
			System.out.println(ConfParser.url);
			/*start();
			for (int j = 0; j < sql.length; j++) {
				if (sql[j].toString().isEmpty()) {
					flag = false;
				} else {
					System.out.println("insertHTTP  插入数据Start--------------："+new Date().toLocaleString());
					System.out.println(sql[j].toString());
					insert(sql[j].toString());
					System.out.println("insertHTTP  插入数据END--------------："+new Date().toLocaleString());
					flag = true;
				}
			}*/
		} catch (Exception e) {
			flag = false;
			e.printStackTrace();
		} /*finally{
			try {
				close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}*/
	}

	/**
	 * 插入PING数据
	 */
	public void insertPING(Map dataMap,String keyspace,String numType,String detailreport,String dataOrg){
		System.out.println("insertPING-----------------"+new Date().toLocaleString());
		boolean flag = false;
		List nameList = new ArrayList();
		List valueList = new ArrayList();
		Map map = new HashMap();
		Set keySet = dataMap.keySet();
		Iterator iter = keySet.iterator();
		Format fm = new DecimalFormat("#.######");
		String longitude = "";
		String latitude = "";
		String avg_rate = "";//平均速率
		String location = "";//测试地点
		String gpsStr = "";
		String spaceGpsStr = "";//测试地点
		//获取GPS位置信息----start
		if(dataMap.containsKey("GPS信息")){
			gpsStr = (String)(dataMap.get("GPS信息")==null?"":dataMap.get("GPS信息"));
		}else if(dataMap.containsKey("GPS位置信息")){
			gpsStr = (String)(dataMap.get("GPS位置信息")==null?"":dataMap.get("GPS位置信息"));
		}else if(dataMap.containsKey("GPS位置")){
			gpsStr = (String)(dataMap.get("GPS位置")==null?"":dataMap.get("GPS位置"));
		}else if(dataMap.containsKey("测试位置")){
			gpsStr = (String)(dataMap.get("测试位置")==null?"":dataMap.get("测试位置"));
		}else if(dataMap.containsKey("测试GPS位置")){
			gpsStr = (String)(dataMap.get("测试GPS位置")==null?"":dataMap.get("测试GPS位置"));
		}else{
			gpsStr = "";
		}
		if(gpsStr.equals("--")){
//			return new StringBuffer();
			return ;
		}
//		map.put("gps", gpsStr);
		if(gpsStr.contains(" ")){
			String[] gps = transGpsPoint(gpsStr);
			if(gps!=null && gps[0]!=null && gps[1]!=null){
				longitude = gps[0];
				latitude = gps[1];
			}else{
				System.out.println("经纬度错误 不插入此数据");
				return ;
			}
		}
		
		if(longitude.isEmpty() || latitude.isEmpty()){
			System.out.println("经纬度错误 不插入此数据");
			return ;
		}
		double lng[] = new double[mapLevel.length];
		double lat[] = new double[mapLevel.length];
		double[] em = Earth2Mars.transform(Double.parseDouble(longitude), Double.parseDouble(latitude));
		double[] bf = BdFix.bd_encrypt(em[0], em[1]);
		double longitudeNum = bf[0];
		double latitudeNum = bf[1];
		
		String table[] = new String[mapLevel.length];
		StringBuffer sql[] = new StringBuffer[mapLevel.length];
		for (int i = 0; i < mapLevel.length; i++) {
			List list = this.getLNGLATByLevel(mapLevel[i]);
			LNGinterval = (Double) list.get(0);
			LATinterval = (Double) list.get(1);
			lng[i] = ((int) (longitudeNum / LNGinterval)) * LNGinterval
					+ LNGinterval / 2;
			lat[i] = ((int) (latitudeNum / LATinterval)) * LATinterval
					+ LATinterval / 2;
			// map.put("longitude",longitudeNum);
			// map.put("latitude",latitudeNum);
			if(dataMap.containsKey("成功率") || dataMap.containsKey("success rate") || dataMap.containsKey("Success Rate") ){
				
				if(dataMap.get("成功率")!=null && dataMap.get("成功率").toString().equals("0%"))
				{
					return ;
				}
				if(dataMap.get("success rate")!=null && dataMap.get("success rate").toString().equals("0%")){
					return ;
				}
				if(dataMap.get("Success Rate")!=null && dataMap.get("Success Rate").toString().equals("0%"))
				{	
					return ;
				}
			}
			while (iter.hasNext()) {
				String key = iter.next() + "";
				if (key == null || key.isEmpty()) {
					continue;
				}
				if ("平均时延(ms)".equals(key) || "平均时延".equals(key) || "average delay(ms)".equals(key.toLowerCase()) || "avg latency(ms)".equals(key.toLowerCase()) || "avg time(ms)".equals(key.toLowerCase()) || "avg latency".equals(key.toLowerCase()) || "delay".equals(key.toLowerCase()) || "delay(ms)".equals(key.toLowerCase())) {
					avg_rate = (String) (dataMap.get(key) == null ? ""
							: dataMap.get(key));
					if(avg_rate.isEmpty()){
						return ;
					}
					try {
						if(avg_rate.endsWith("ms")){
							avg_rate = avg_rate.replaceAll("ms", "");
						}
						if(avg_rate.endsWith("s")){
							avg_rate = avg_rate.replaceAll("s", "");
							double avg_num = Double.parseDouble(avg_rate);
							avg_rate = avg_num*1000+"";
						}
						double avg_num = Double.parseDouble(avg_rate);
						map.put("avg_rate", avg_rate);	
					} catch (Exception e) {
						System.out.println("delay 不为纯数字");
						return;
					}
				}
			}
		}
		for (int j = 0; j < mapLevel.length; j++) {
			map.put("lng", fm.format(lng[j]));
			map.put("lat", fm.format(lat[j]));
			map.put("avg_rate", avg_rate);
			// 拼接表名

			if (numType.equals("01001")) {
				table[j] = "speed_test";
			} else if (numType.equals("02001")) {
				table[j] = "web_browsing";
			} else if (numType.equals("02011")) {
				table[j] = "video_test";
			} else if (numType.equals("03001") || numType.equals("03011") ) {
				table[j] = "http_test_" + dataOrg + "_" + mapLevel[j];
			} else if (numType.equals("04002")) {
				table[j] = "ping_" + dataOrg + "_" + mapLevel[j];
			} else if (numType.equals("04006")) {
				table[j] = "hand_over";
			} else if (numType.equals("05001")) {
				table[j] = "call_test";
			} else if (numType.equals("05002")) {
				table[j] = "sms";
			} else {
//				return new StringBuffer("");
				return ;
			}
//			sql[j] = appendSqlNormal(map, numType, dataOrg, table[j]);
			appendSql(map, numType, dataOrg, table[j]);
		}
		try {
			if(ConfParser.writeCsvOpen.equals("open")){
				insertCSVByDataToGPS.insertCsvGPSData(dataMap, numType, keyspace, detailreport);	
			}
			System.out.println(ConfParser.url);
			/*start();
			conn.setAutoCommit(false);
			for (int j = 0; j < sql.length; j++) {
				if (sql[j].toString().isEmpty()) {
					flag = false;
				} else {
					System.out.println("insertPING  插入数据Start--------------："+new Date().toLocaleString());
					System.out.println(sql[j].toString());
					insert(sql[j].toString());
					System.out.println("insertPING  插入数据END--------------："+new Date().toLocaleString());
					flag = true;
				}
			}*/
		} catch (Exception e) {
			flag = false;
			e.printStackTrace();
		}/* finally{
			try {
				conn.setAutoCommit(true);
				close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}*/
	}
	
	private StringBuffer appendSqlNormal(Map map, String numType,String dataOrg,String table) {
		System.out.println("appendSQL------------------start :"+new Date().toLocaleString());
		boolean flag = true;
		Format fm = new DecimalFormat("#.######");
		String lnginterval = fm.format(LNGinterval);
		//处理经纬度
		if(lnginterval.contains(".")){
			lnginterval = lnginterval.substring(lnginterval.indexOf(".")+1, lnginterval.length());	
		}
		String latinterval = fm.format(LATinterval);
		if(latinterval.contains(".")){
			latinterval = latinterval.substring(latinterval.indexOf(".")+1, latinterval.length());	
		}
		StringBuffer sql = new StringBuffer("");
		String lng = fm.format(Double.parseDouble(map.get("lng").toString()));
		String lat = fm.format(Double.parseDouble(map.get("lat").toString()));
		String avg_rate = map.get("avg_rate")==null?"":map.get("avg_rate").toString();
		if(avg_rate.isEmpty()){
			return new StringBuffer("");
		}
		StringBuffer columnStr = new StringBuffer("(");
		StringBuffer valueStr = new StringBuffer(" values (");
		StringBuffer updateStr = new StringBuffer("");
		if(Integer.parseInt(dataOrg) < Integer.parseInt(dataStart) || Integer.parseInt(dataOrg) == Integer.parseInt(dataStart)){
			return new StringBuffer("");
		}
		System.out.println("appendSql 处理经纬度 日期等 END  查询数据库表是否存在 开始 -----------------："+new Date().toLocaleString());
		// 判断数据库表是否存在 不存在则执行创建表的存储过程
		boolean tableFlag = this.queryTableExist(table);
		System.out.println("appendsql  查询数据库表是否存在 END -------------------："+new Date().toLocaleString());
		if (!tableFlag) {
			System.out.println("appendsql 创建表Start--------------："+new Date().toLocaleString());
//			this.execute("pro_createtable_gps",table);
			this.createGPS(table,numType);
			System.out.println("appendsql 创建表END--------------："+new Date().toLocaleString());
			Integer month_num = Integer.parseInt(dataOrg.substring(4,6));
			Integer year_num = Integer.parseInt(dataOrg.substring(0,4));
			if(month_num==01){
				year_num = year_num - 1;
				month_num = 12;
			}else{
				month_num = month_num - 1;
			}
			String dataOrgUp = "";
			if(month_num.toString().length() == 1){
				 dataOrgUp = year_num+"0"+month_num;
			}else{
				 dataOrgUp = year_num+""+month_num;
			}
			String table_up = table.replace(dataOrg, dataOrgUp);
			System.out.println("appendsql 查询表是否存在Start--------------："+new Date().toLocaleString());
			tableFlag = this.queryTableExist(table_up);
			System.out.println("appendsql 查询表是否存在END--------------："+new Date().toLocaleString());
			//上个月份的表格数据存在 进行复制操作
			if(tableFlag){
				System.out.println("appendsql 复制上月份表Start--------------："+new Date().toLocaleString());
				this.copyTable(table, table_up);
				System.out.println("appendsql 复制上月份表END--------------："+new Date().toLocaleString());
			}else{
				String StartTable = table;
				String startDataOrg = dataOrg;
				String dataEnd = "";
				while(flag){
					month_num = Integer.parseInt(dataOrg.substring(4,6));
					year_num = Integer.parseInt(dataOrg.substring(0,4));
					if(month_num==01){
						year_num = year_num - 1;
						month_num = 12;
					}else{
						month_num = month_num - 1;
					}
					String dataOrgUp1 = "";
					if(month_num.toString().length() == 1){
						dataOrgUp1 = year_num+"0"+month_num;
					}else{
						dataOrgUp1 = year_num+""+month_num;
					}
					String table_up1 = table.replace(dataOrg, dataOrgUp1);
					dataOrg = dataOrgUp1;
					table = table_up1;
					//查询到有上个月份的表时 将此表至当前月份的表都创建一遍
					if(this.queryTableExist(table_up1)){
						System.out.println(table_up1);
						dataEnd = dataOrgUp1;
						String table_down = "";
						month_num = Integer.parseInt(dataEnd.substring(4,6));
						year_num = Integer.parseInt(dataEnd.substring(0,4));
						while(!table_down.equals(StartTable)){
							String dataOrgdown = "";
							if(month_num==12){
								year_num = year_num + 1;
								month_num = 1;
							}else{
								month_num = month_num + 1;
							}
							if(month_num.toString().length() == 1){
								dataOrgdown = year_num+"0"+month_num;
							}else{
								dataOrgdown = year_num+""+month_num;
							}
							table_down = table.replace(dataOrg, dataOrgdown);
							// 判断数据库表是否存在 不存在则执行创建表的存储过程
							System.out.println("appendsql 判断数据库表是否存在Start--------------："+new Date().toLocaleString());
							if(!this.queryTableExist(table_down)){
								System.out.println("appendsql 判断数据库表是否存在END   创建gps表Start--------------："+new Date().toLocaleString());
//								this.execute("pro_createtable_gps",table_down);
								this.createGPS(table_down,numType);
								System.out.println("appendsql 创建gps表END--------------："+new Date().toLocaleString());
							}
							System.out.println("appendsql 复制上月份表Start--------------："+new Date().toLocaleString());
							this.copyTable(table_down, table);
							System.out.println("appendsql 复制上月份表END--------------："+new Date().toLocaleString());
							dataOrg = dataOrgdown;
							table = table_down;
							System.out.println(table_down);
						}
						flag = false;
					}
					if(Integer.parseInt(dataOrg) < Integer.parseInt(dataStart) || Integer.parseInt(dataOrg) == Integer.parseInt(dataStart)){
						table = StartTable;
						dataOrg = startDataOrg;
						flag = false;
						break;
					}
				}
			}
		}
		String numFlag = "";
		List<String> dataList  = this.queryExist( table, lng, lat);// 有数据则为true
		numFlag = dataList.get(0);
		String id = dataList.get(4);
		// 执行更新
		if (!numFlag.equals("0")) {
			start();
			try {
				conn.setAutoCommit(false);
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
			List list = this.getTestTimesByGPS(table, lng, lat,id);
			//计算次数
			Integer test_times = (Integer) list.get(0);
			String avg = (String) list.get(1);
			test_times = test_times + 1;
			//计算平均速率
			if (avg.isEmpty()) {
				if (!avg_rate.isEmpty()) {
					map.put("avg_rate", fm.format(Double.parseDouble(avg_rate)
							/ test_times));
				} else {
					map.put("avg_rate", 0);
					return new StringBuffer("");
				}
			} else {
				map.put("avg_rate", fm.format((Double.parseDouble(avg)
						* (test_times - 1) + Double.parseDouble(avg_rate))
						/ test_times));
			}
			map.put("test_times", test_times);
			//拼装sql语句
			sql.append("update " + table + " set ");
			try {
				Set set = map.keySet();
				Iterator iter = set.iterator();
				while (iter.hasNext()) {
					String name = (String) iter.next();
					if (name.isEmpty()) {
						continue;
					}
					String value = map.get(name) + "";
					if (value.isEmpty()) {
						continue;
					}
					updateStr.append(name + " = '" + value + "', ");
				}

				while (updateStr.toString().trim().endsWith(",")) {
					updateStr = new StringBuffer(
							updateStr.toString().substring(0,
									updateStr.toString().lastIndexOf(",")));
				}
				sql.append(updateStr);
				sql.append(" where lng = '" + lng + "' and lat = '" + lat
								+ "'");
				System.out.println(sql.toString());
				statement.executeUpdate(sql.toString());
				conn.commit();
			} catch (Exception e) {
				e.printStackTrace();
				try {
					conn.rollback();
				} catch (SQLException e1) {
					e1.printStackTrace();
				}
			} finally{
				try {
					conn.setAutoCommit(true);
					close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		} else {//拼装新增数据的sql语句
			map.put("test_times", 1);
			sql.append("insert into " + table + " ");
			try {
				Set set = map.keySet();
				Iterator iter = set.iterator();
				while (iter.hasNext()) {
					String name = (String) iter.next();
					if (name.isEmpty()) {
						continue;
					}
					String value = map.get(name) + "";
					if (value.isEmpty()) {
						continue;
					}
					columnStr.append(name + ",");
					valueStr.append("'" + value + "',");
				}
				while (columnStr.toString().trim().endsWith(",")) {
					columnStr = new StringBuffer(
																																																																																																																																																											columnStr.toString().substring(0,
									columnStr.toString().lastIndexOf(",")));
				}
				while (valueStr.toString().trim().endsWith(",")) {
					valueStr = new StringBuffer(valueStr.toString().substring(
							0, valueStr.toString().lastIndexOf(",")));
				}
				columnStr.append(" )");
				valueStr.append(" )");
				sql.append(columnStr);
				sql.append(valueStr);
			} catch (Exception e) {
				System.out.println("拼装语句错误：" + sql);
				sql = new StringBuffer("");
				e.printStackTrace();
			}
		}
		return sql;
	}
	
	private void appendSql(Map map, String numType,String dataOrg,String table) {
		System.out.println("appendSQL------------------start :"+new Date().toLocaleString());
		boolean flag = true;
		Format fm = new DecimalFormat("#.######");//去0
		String lnginterval = fm.format(LNGinterval);
		//处理经纬度
		if(lnginterval.contains(".")){
			lnginterval = lnginterval.substring(lnginterval.indexOf(".")+1, lnginterval.length());	
		}
		String latinterval = fm.format(LATinterval);
		if(latinterval.contains(".")){
			latinterval = latinterval.substring(latinterval.indexOf(".")+1, latinterval.length());	
		}
		StringBuffer sql = new StringBuffer("");
		String lng = fm.format(Double.parseDouble(map.get("lng").toString()));
		String lat = fm.format(Double.parseDouble(map.get("lat").toString()));
		String avg_rate = map.get("avg_rate")==null?"":map.get("avg_rate").toString();
		String top_rate = map.get("top_rate")==null?"":map.get("top_rate").toString();
		String down_rate = map.get("down_rate")==null?"":map.get("down_rate").toString();
		if(avg_rate.isEmpty()){
			if(top_rate.isEmpty() && down_rate.isEmpty()){
				return ;	
			}
		}
		StringBuffer columnStr = new StringBuffer("(");
		StringBuffer valueStr = new StringBuffer(" values (");
		StringBuffer updateStr = new StringBuffer("");
		if(Integer.parseInt(dataOrg) < Integer.parseInt(dataStart) || Integer.parseInt(dataOrg) == Integer.parseInt(dataStart)){
			return ;
		}
		System.out.println("appendSql 处理经纬度 日期等 END  查询数据库表是否存在开始 -----------------："+new Date().toLocaleString());
		// 判断数据库表是否存在 不存在则执行创建表的存储过程
		// table表是该文件插入数据所需的表，在判断是否存在后直接操作，如果存在说明已经创建过了，它之前月份的数据也已经存在，
		// 果没有则需要创建该表并查询之前月份的表是否存在，并把它上月份表中的数据拷贝过来
		boolean tableFlag = this.queryTableExist(table);
		System.out.println("appendsql  查询数据库表是否存在 END -------------------："+new Date().toLocaleString());
		if (!tableFlag) {
			System.out.println("appendsql 创建表Start--------------："+new Date().toLocaleString());
//			this.execute("pro_createtable_gps",table);
			this.createGPS(table,numType);
			System.out.println("appendsql 创建表END--------------："+new Date().toLocaleString());
			//201510
			Integer month_num = Integer.parseInt(dataOrg.substring(4,6));
			Integer year_num = Integer.parseInt(dataOrg.substring(0,4));
			if(month_num==01){
				year_num = year_num - 1;
				month_num = 12;
			}else{
				month_num = month_num - 1;
			}
			String dataOrgUp = "";
			if(month_num.toString().length() == 1){
				 dataOrgUp = year_num+"0"+month_num;
			}else{
				 dataOrgUp = year_num+""+month_num;
			}
			String table_up = table.replace(dataOrg, dataOrgUp);
			System.out.println("appendsql 查询表是否存在Start--------------："+new Date().toLocaleString());
			tableFlag = this.queryTableExist(table_up);
			System.out.println("appendsql 查询表是否存在END--------------："+new Date().toLocaleString());
			//上个月份的表格数据存在 进行复制操作
			if(tableFlag){
				System.out.println("appendsql 复制上月份表Start--------------："+new Date().toLocaleString());
				this.copyTable(table, table_up);
				System.out.println("appendsql 复制上月份表END--------------："+new Date().toLocaleString());
			}else{
				String StartTable = table;
				String startDataOrg = dataOrg;
				String dataEnd = "";
				while(flag){
					month_num = Integer.parseInt(dataOrg.substring(4,6));
					year_num = Integer.parseInt(dataOrg.substring(0,4));
					if(month_num==01){
						year_num = year_num - 1;
						month_num = 12;
					}else{
						month_num = month_num - 1;
					}
					String dataOrgUp1 = "";
					if(month_num.toString().length() == 1){
						dataOrgUp1 = year_num+"0"+month_num;
					}else{
						dataOrgUp1 = year_num+""+month_num;
					}
					String table_up1 = table.replace(dataOrg, dataOrgUp1);
					dataOrg = dataOrgUp1;
					table = table_up1;
					//查询到有上个月份的表时 将此表至当前月份的表都创建一遍
					if(this.queryTableExist(table_up1)){
						System.out.println(table_up1);
						dataEnd = dataOrgUp1;
						String table_down = "";
						month_num = Integer.parseInt(dataEnd.substring(4,6));
						year_num = Integer.parseInt(dataEnd.substring(0,4));
						while(!table_down.equals(StartTable)){
							String dataOrgdown = "";
							if(month_num==12){
								year_num = year_num + 1;
								month_num = 1;
							}else{
								month_num = month_num + 1;
							}
							if(month_num.toString().length() == 1){
								dataOrgdown = year_num+"0"+month_num;
							}else{
								dataOrgdown = year_num+""+month_num;
							}
							table_down = table.replace(dataOrg, dataOrgdown);
							// 判断数据库表是否存在 不存在则执行创建表的存储过程
							System.out.println("appendsql 判断数据库表是否存在Start--------------："+new Date().toLocaleString());
							if(!this.queryTableExist(table_down)){
								System.out.println("appendsql 判断数据库表是否存在END   创建gps表Start--------------："+new Date().toLocaleString());
//								this.execute("pro_createtable_gps",table_down);
								this.createGPS(table_down,numType);
								System.out.println("appendsql 创建gps表END--------------："+new Date().toLocaleString());
							}
							System.out.println("appendsql 复制上月份表Start--------------："+new Date().toLocaleString());
							this.copyTable(table_down, table);
							System.out.println("appendsql 复制上月份表END--------------："+new Date().toLocaleString());
							dataOrg = dataOrgdown;
							table = table_down;
							System.out.println(table_down);
						}
						flag = false;
					}
					if(Integer.parseInt(dataOrg) < Integer.parseInt(dataStart) || Integer.parseInt(dataOrg) == Integer.parseInt(dataStart)){
						table = StartTable;
						dataOrg = startDataOrg;
						flag = false;
						break;
					}
				}
			}
		}
		String numFlag = "";
		//处理区域图数据表
		//只根据lng、lat两个条件查询会出现多条数据情况，处理需求？
		List<String> dataList  = this.queryExist( table, lng, lat);// 有数据则为true
		numFlag = dataList.get(0);
		String id = dataList.get(4);
		// 执行更新
		//numFlag为0时数据不存在， 为1时说明数据存在但是省份字段的值为空 ， 为2时说明数据存在并且省份字段值不为空
		if (!numFlag.equals("0")) {
			start();
			try {
				conn.setAutoCommit(false);
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
			List list = this.getTestTimesByGPS(table, lng, lat,id);
			//计算次数
			Integer test_times = (Integer) list.get(0);
			test_times = test_times + 1;
			//计算平均速率
			if(numType.equals("01001")){
				String top = (String) list.get(1);
				String down = (String)list.get(2);
				if (top.isEmpty()) {
					if (!top_rate.isEmpty()) {
						map.put("top_rate", fm.format(Double.parseDouble(top_rate)
								/ test_times));
					} else {
						map.put("top_rate", 0);
						return ;
					}
				} else {
					map.put("top_rate", fm.format((Double.parseDouble(top)
							* (test_times - 1) + Double.parseDouble(top_rate))
							/ test_times));
				}
				if (down.isEmpty()) {
					if (!down_rate.isEmpty()) {
						map.put("down_rate", fm.format(Double.parseDouble(down_rate)
								/ test_times));
					} else {
						map.put("down_rate", 0);
						return ;
					}
				} else {
					map.put("down_rate", fm.format((Double.parseDouble(down)
							* (test_times - 1) + Double.parseDouble(down_rate))
							/ test_times));
				}
			}else{
				String avg = (String) list.get(1);
				if (avg.isEmpty()) {
					if (!avg_rate.isEmpty()) {
						map.put("avg_rate", fm.format(Double.parseDouble(avg_rate)
								/ test_times));
					} else {
						map.put("avg_rate", 0);
						return ;
					}
				} else {
					map.put("avg_rate", fm.format((Double.parseDouble(avg)
							* (test_times - 1) + Double.parseDouble(avg_rate))
							/ test_times));
				}
			}
			
			map.put("test_times", test_times);
			String maplevel = table.split("_").length>2?table.split("_")[2]:"";
			if(maplevel.equals("1")){
				if(ConfParser.areaDataOpen.equals("open")){
					if(numFlag.equals("2")){
						String dbProvince = dataList.get(1);
						String dbCity = dataList.get(2);
						String dbDistrict = dataList.get(3);
						System.out.println("appendsql 处理区域数据Start--------------："+new Date().toLocaleString());
						updateGPSDateDao.dealAreaTable(table,dbProvince,dbCity,dbDistrict);
						System.out.println("appendsql 处理区域数据END  更新区域数据START --------------："+new Date().toLocaleString());
						updateGPSDateDao.updateAreaData(table, test_times, Double.parseDouble(avg_rate), Double.parseDouble(top_rate), Double.parseDouble(down_rate), dbProvince, dbCity, dbDistrict);
						System.out.println("appendsql  更新区域数据END--------------："+new Date().toLocaleString());
					}
				}
			}
			
			//拼装sql语句
			sql.append("update " + table + " set ");
			try {
				Set set = map.keySet();
				Iterator iter = set.iterator();
				while (iter.hasNext()) {
					String name = (String) iter.next();
					if (name.isEmpty()) {
						continue;
					}
					String value = map.get(name) + "";
					if (value.isEmpty()) {
						continue;
					}
					updateStr.append(name + " = '" + value + "', ");
				}

				while (updateStr.toString().trim().endsWith(",")) {
					updateStr = new StringBuffer(
							updateStr.toString().substring(0,
									updateStr.toString().lastIndexOf(",")));
				}
				sql.append(updateStr);
				sql.append(" where lng = '" + lng + "' and lat = '" + lat
								+ "'");
				System.out.println(sql.toString());
				statement.executeUpdate(sql.toString());
				conn.commit();
			} catch (Exception e) {
				e.printStackTrace();
				try {
					conn.rollback();
				} catch (SQLException e1) {
					e1.printStackTrace();
				}
			} finally{
				try {
					conn.setAutoCommit(true);
					close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		} else {//拼装新增数据的sql语句		为numFlag=0的情况
			map.put("test_times", 1);
			sql.append("insert into " + table + " ");
			try {
				Set set = map.keySet();
				Iterator iter = set.iterator();
				while (iter.hasNext()) {
					String name = (String) iter.next();
					if (name.isEmpty()) {
						continue;
					}
					String value = map.get(name) + "";
					if (value.isEmpty()) {
						continue;
					}
					columnStr.append(name + ",");
					valueStr.append("'" + value + "',");
				}
				while (columnStr.toString().trim().endsWith(",")) {
					columnStr = new StringBuffer(
																																																																																																																																																											columnStr.toString().substring(0,
									columnStr.toString().lastIndexOf(",")));
				}
				while (valueStr.toString().trim().endsWith(",")) {
					valueStr = new StringBuffer(valueStr.toString().substring(
							0, valueStr.toString().lastIndexOf(",")));
				}
				columnStr.append(" )");
				valueStr.append(" )");
				sql.append(columnStr);
				sql.append(valueStr);
			} catch (Exception e) {
				System.out.println("拼装语句错误：" + sql);
				sql = new StringBuffer("");
				e.printStackTrace();
			}
		}
		try {
			start();
			statement.execute(sql.toString());
			System.out.println("execute SUCCESS : "+sql.toString());
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("execute Failur : "+sql.toString());
		}finally{
			try {
				close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	public boolean writeCsv(Map map,String numType,double longitudeNum,double latitudeNum){
		String []str = getArrayFromMap(map);
		String keys[] = getKeysFromMap(map);
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
			open(paths,keys);
			write(str);
			closeFile();
		} catch (Exception e) {
			return false;
		}
		return true;
	}
	public String[] getArrayFromMap(Map map){
		String [] strs = new String[map.size()];
		int i = 0;
		Collection collection = map.values();
		Object[] objects = collection.toArray(); 
		for (int j = 0; j < objects.length; j++) {
			String str = objects[j]==null?"":objects[j].toString();
			strs[i++]= str;
		}
		return strs;
	}
	public String[] getKeysFromMap(Map map){
		String [] keys = new String[map.size()];
		int i = 0;
		Set set = map.keySet();
		Object[] objects = set.toArray(); 
		for (int j = 0; j < objects.length; j++) {
			String str = objects[j].toString();
			keys[i++]= str;
		}
		return keys;
	}
	//添加表头
	public void open(String fileName,String[] keys) {
		boolean flag = false;
		csvFile = new File(fileName);
		try {
			if(!csvFile.exists()){
				csvFile.getParentFile().mkdirs();
				csvFile.createNewFile();
				flag = true;
			}
			writer = new BufferedWriter(new FileWriterWithEncoding(csvFile,"gbk", true));
			cwriter = new CsvWriter(writer,',');
			if(flag){
				write(keys);//写入表头
			}
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
	
	public void closeFile() {
		
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
	public List getLNGLATByLevel(String mapLevel){
		List list = new ArrayList();
		String sql = "SELECT map.loninterval lng,map.latinterval lat FROM map_level map where level = "+mapLevel;
		double lng = 0;
		double lat = 0;
		Connection conn1 = null;
		Statement statement1 = null;
		try {
			String driver = "com.mysql.jdbc.Driver";
			String url = ConfParser.url.substring(0,ConfParser.url.lastIndexOf("/")+1)+"static_param";
			System.out.println("******************"+url);
			String user = ConfParser.user;
			String password = ConfParser.password;
			Class.forName(driver);
			conn1 = DriverManager.getConnection(url, user, password);
			statement1 = conn1.createStatement();
			System.out.println(sql);
			ResultSet rs = statement1.executeQuery(sql);
			if(rs.next()){
				lng = rs.getDouble("lng");
				lat = rs.getDouble("lat");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			try {
				if(statement1!=null){
					statement1.close();
				}
				if (conn1 != null) {
					conn1.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		list.add(lng);
		list.add(lat);
		return list;
	}
	/**
	 * 复制上一个月表的数据到最新的月份
	 * @param table
	 * @param table_domain
	 */
	public void copyTable(String table,String table_domain){
		String copySql = "insert into "+table+" select * from "+table_domain;
		try {
			start();
			this.insert(copySql);
			System.out.println("复制表"+table_domain+"到"+table+"成功");
		} catch (Exception e) {
			System.out.println("复制表"+table_domain+"到"+table+"失败");
			e.printStackTrace();
		}finally{
			try {
				close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	public List getTestTimesByGPS(String table,String lng,String lat,String id){
		List list = new ArrayList();
		Integer counts = 0;
		String avg = "";
		String down_rate = "";
		String top_rate = "";
		String sql = "";
		if(table.startsWith("speed_test")){
			sql  = "select test_times counts,top_rate,down_rate from "+table+" where lng = "+lng+" and lat = "+lat+" and id = "+id+" for update";
		}else{
			sql  = "select test_times counts,avg_rate avg from "+table+" where lng = "+lng+" and lat = "+lat+" and id = "+id+" for update";	
		}
		try {
			ResultSet rs = (ResultSet)statement.executeQuery(sql);
			if(rs.next()){
				counts = rs.getInt("counts");
				list.add(counts);
				if(table.startsWith("speed_test")){
					down_rate = rs.getString("down_rate");
					top_rate = rs.getString("top_rate");
					if(top_rate!=null){
						list.add(top_rate);	
					}else{
						list.add("");
					}
					if(down_rate!=null){
						list.add(down_rate);	
					}else{
						list.add("");
					}
				}else{
					avg = rs.getString("avg");
					if(avg!=null){
						list.add(avg);	
					}else{
						list.add("");
					}
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return list;
	}
	public boolean queryTableExist(String tableName){
		System.out.println(ConfParser.url);
		start();
		boolean flag = false;
		DatabaseMetaData databaseMetaData;
		try {
			databaseMetaData = (DatabaseMetaData) conn.getMetaData();
			ResultSet resultSet = databaseMetaData.getTables(null, null, tableName, null);
			if(resultSet.next()){
				flag = true;
			}else{
				flag = false;
			}
			resultSet.close();
		} catch (SQLException e) {
			flag = false;
			e.printStackTrace();
		}finally{
			try {
				close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return flag;
	}
	public List<String> queryExist(String table,String lng,String lat){
		List<String> list = new ArrayList();
		String sql  = "select * from "+table+" where lng = "+lng+" and lat = "+lat;
		String numFlag = "0";
		String province = "";
		String city = "";
		String district = "";
		String id = "";
		start();
		try {
			ResultSet rs = (ResultSet)statement.executeQuery(sql);
			if(rs.next()){
				id = rs.getInt("id")+"";
				province = rs.getString("province");
				city = rs.getString("city");
				district = rs.getString("district");
				System.out.println(district);
				if(province!=null && !province.isEmpty()){
					numFlag = "2";
				}else{
					numFlag = "1";	
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}finally{
			try {
				close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		list.add(numFlag);
		list.add(province);
		list.add(city);
		list.add(district); 
		list.add(id);
		return list;
	}
	/**
	 * GPS坐标转换
	 * @param meta
	 * @return
	 */
	private static String[] transGpsPoint(String meta){

		String[] result = new String[2];
		String[] info = null;
		if(meta!=null && !"".equals(meta)){
			info = meta.split(" ");
		}else{
			return null;
		}
		
		String latitude = "";
		String longitude = "";
		
		String gpsPoint = "";
		try{
			for(int i=0; i<info.length&&i<2; i++){
				if(info[i].contains("°")){
					String degrees = info[i].substring(0,info[i].lastIndexOf("°"));
					String minutes = info[i].substring(info[i].lastIndexOf("°")+1,info[i].lastIndexOf("′"));
					String seconds = info[i].substring(info[i].lastIndexOf("′")+1,info[i].lastIndexOf("″"));
					
					//Long gpsLong = Long.parseLong(degrees)+Long.parseLong(minutes)/60+Long.parseLong(seconds)/3600;
					float gpsLong = Float.parseFloat(degrees)+Float.parseFloat(minutes)/60+Float.parseFloat(seconds)/3600;
					
					DecimalFormat decimalFormat=new DecimalFormat(".000000");
					gpsPoint = decimalFormat.format(gpsLong);
					if(info[i].contains("E")){
						longitude = gpsPoint;
					}else if(info[i].contains("N")){
						latitude = gpsPoint;
					}else if(gpsLong>80.0){
						longitude = gpsPoint;
					}else{
						latitude = gpsPoint;
					}
				}else{
					if(info[i].contains("E")){
						gpsPoint = info[i].substring(0,info[i].lastIndexOf("E"));
						float gpsLong = Float.valueOf(gpsPoint);
						DecimalFormat decimalFormat=new DecimalFormat(".000000");
						gpsPoint = decimalFormat.format(gpsLong);
						longitude = gpsPoint;
					}else if(info[i].contains("N")){
						gpsPoint = info[i].substring(0,info[i].lastIndexOf("N"));
						float gpsLong = Float.valueOf(gpsPoint);
						DecimalFormat decimalFormat=new DecimalFormat(".000000");
						gpsPoint = decimalFormat.format(gpsLong);
						latitude = gpsPoint;
					}else{
						gpsPoint = info[i];
						float gpsLong = Float.valueOf(gpsPoint);
						DecimalFormat decimalFormat=new DecimalFormat(".000000");
						gpsPoint = decimalFormat.format(gpsLong);
						if(gpsLong>80.0){
							longitude = gpsPoint;
						}else{
							latitude = gpsPoint;
						}
					}
				}
			}
			result[0] = longitude;
			result[1] = latitude;
		}catch(Exception e){
			e.printStackTrace();
		}
		return result;
	}
	public static String testPost(String x, String y) throws IOException {

		URL url = new URL("http://api.map.baidu.com/geocoder?ak="+ ak
				+ "&coordtype=wgs84ll&location=" + x + "," + y
				+ "&output=json");
		URLConnection connection = url.openConnection();
		/**
		 * 然后把连接设为输出模式。URLConnection通常作为输入来使用，比如下载一个Web页。
		 * 通过把URLConnection设为输出，你可以把数据向你个Web页传送。下面是如何做：
		 */
		System.out.println(url);
		connection.setDoOutput(true);
		OutputStreamWriter out = new OutputStreamWriter(
				connection.getOutputStream(), "utf-8");
		// remember to clean up
		out.flush();
		out.close();
		// 一旦发送成功，用以下方法就可以得到服务器的回应：
		String res;
		InputStream l_urlStream;
		l_urlStream = connection.getInputStream();
		BufferedReader in = new BufferedReader(new InputStreamReader(
				l_urlStream, "UTF-8"));
		StringBuilder sb = new StringBuilder("");
		while ((res = in.readLine()) != null) {
			sb.append(res.trim());
		}
		String str = sb.toString();
		System.out.println(str);
		return str;

	}
	public static String subLalo1(String str){
		JSONObject json = JSONObject.fromObject(str);
		String district = "";
		String province = "";
		String city = "";
		String street = "";
		String street_number = "";
		JSONObject result = json.getJSONObject("result");
		JSONObject address = result.getJSONObject("addressComponent");
		
		district = address.getString("district");
		city = address.getString("city");
		province = address.getString("province");
		street = address.getString("street");
		street_number = address.getString("street_number");
		
		String resultStr = province+"_"+city+"_"+district+"_"+street+"_"+street_number;
		return resultStr;
	}
	public static String subLalo(String str) {
		String ssq = "";
		if (StringUtils.isNotEmpty(str)) {
			int cStart = str.indexOf("city\":");
			int cEnd = str.indexOf(",\"district");

			int dStart = str.indexOf(",\"district");
			int dEnd = str.indexOf(",\"province");

			int pStart = str.indexOf(",\"province");
			int pEnd = str.indexOf(",\"street");

			int sStart = str.indexOf(",\"street");
			int sEnd = str.indexOf(",\"street_number");
			
			int snStart = str.indexOf("street_number");
			
			if (pStart > 0 && pEnd > 0) {
				String province = str.substring(pStart + 13, pEnd - 1);
				if (StringUtils.isNotBlank(province)) {

					ssq += province + "_";
				} else {
					System.out.println(str+"--->请求百度失败");
					ssq += "-_";
				}
			} else {
				ssq += "-_";
			}
			
			if (cStart > 0 && cEnd > 0) {
				String city = str.substring(cStart + 7, cEnd - 1);
				if (StringUtils.isNotBlank(city)) {

					ssq += city + "_";
				} else {
					ssq += "-_";
				}

			} else {
				ssq += "-_";
			}

			if (dStart > 0 && dEnd > 0) {
				String district = str.substring(dStart + 13, dEnd - 1);
				if (StringUtils.isNotBlank(district)) {

					ssq += district + "_";
				} else {
					ssq += "-_";
				}
			} else {
				ssq += "-" + "_";
			}


			if (sStart > 0 && sEnd > 0) {
				String street = str.substring(sStart + 11, sEnd - 1);
				if (StringUtils.isNotBlank(street)) {

					ssq += street + "_";
				} else {
					ssq += "-_";
				}
			} else {
				ssq += "-_";
			}
			
			if(snStart>0){
				String snStr = str.substring(snStart);
				if(snStr!=null && snStr.length()>0){
					int snEnd = snStart+16;
					String snStr1 = str.substring(snEnd);
					System.out.println(snStr1);
					int snIndex_end = snStr1.indexOf("\"");
					String street_number = snStr1.substring(0,snIndex_end);
					if (StringUtils.isNotBlank(street_number)) {
						ssq += street_number + "_";
					} else {
						ssq += "-_";
					}
				}
			}else
			{
				ssq += "-_";
			}
			if(ssq.endsWith("_")){
				ssq = ssq.substring(0, ssq.length()-1);
			}
			return ssq;
		}
		return null;
	}
	public boolean createGPS(String tablename,String numType) {
		boolean flag = true;
		String sql = "";
		if(numType.equals("03001") || numType.equals("03011") || tablename.startsWith("http_test") || tablename.startsWith("http")){
			sql = "CREATE TABLE "
				+ tablename
				+ " ( id int(10) NOT NULL AUTO_INCREMENT,lng decimal(10,6) DEFAULT NULL,lat decimal(10,6) DEFAULT NULL,test_times int(10) DEFAULT NULL,avg_rate decimal(10,3) DEFAULT NULL,province varchar(200) DEFAULT NULL,city varchar(200) DEFAULT NULL,district varchar(200) DEFAULT NULL,street varchar(200) DEFAULT NULL,street_number varchar(200) DEFAULT NULL,city_code varchar(200) DEFAULT NULL,file_type varchar(200) DEFAULT NULL,PRIMARY KEY (id))ENGINE=InnoDB AUTO_INCREMENT=21923385 DEFAULT CHARSET=utf8 ROW_FORMAT=COMPACT;";
		}else if(numType.equals("01001") || tablename.startsWith("speed_test")){
			sql = "CREATE TABLE "
				+ tablename
				+ " ( id int(10) NOT NULL AUTO_INCREMENT,lng decimal(10,6) DEFAULT NULL,lat decimal(10,6) DEFAULT NULL,test_times int(10) DEFAULT NULL,top_rate decimal(10,3) DEFAULT NULL,down_rate decimal(10,3) DEFAULT NULL,province varchar(200) DEFAULT NULL,city varchar(200) DEFAULT NULL,district varchar(200) DEFAULT NULL,street varchar(200) DEFAULT NULL,street_number varchar(200) DEFAULT NULL,city_code varchar(200) DEFAULT NULL,PRIMARY KEY (id))ENGINE=InnoDB AUTO_INCREMENT=21923385 DEFAULT CHARSET=utf8 ROW_FORMAT=COMPACT;";
		}else if(numType.equals("04002") || tablename.startsWith("ping")){
			sql = "CREATE TABLE "
				+ tablename
				+ " ( id int(10) NOT NULL AUTO_INCREMENT,lng decimal(10,6) DEFAULT NULL,lat decimal(10,6) DEFAULT NULL,test_times int(10) DEFAULT NULL,avg_rate decimal(10,3) DEFAULT NULL,province varchar(200) DEFAULT NULL,city varchar(200) DEFAULT NULL,district varchar(200) DEFAULT NULL,street varchar(200) DEFAULT NULL,street_number varchar(200) DEFAULT NULL,city_code varchar(200) DEFAULT NULL,PRIMARY KEY (id))ENGINE=InnoDB AUTO_INCREMENT=21923385 DEFAULT CHARSET=utf8 ROW_FORMAT=COMPACT;";
		}
		try {
			start();	
			statement.execute(sql);
		} catch (Exception e) {
			flag = false;
			e.printStackTrace();
		}finally{
			try {
				close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return flag;
	}
	public boolean createSpeedTestGPS(String tablename){
		boolean flag = true;
		String sql = "CREATE TABLE "
				+ tablename
				+ " ( id int(10) NOT NULL AUTO_INCREMENT,lng decimal(10,6) DEFAULT NULL,lat decimal(10,6) DEFAULT NULL,test_times int(10) DEFAULT NULL,avg_rate decimal(10,3) DEFAULT NULL,province varchar(200) DEFAULT NULL,city varchar(200) DEFAULT NULL,district varchar(200) DEFAULT NULL,street varchar(200) DEFAULT NULL,street_number varchar(200) DEFAULT NULL,city_code varchar(200) DEFAULT NULL,file_type varchar(200) DEFAULT NULL,PRIMARY KEY (id))ENGINE=InnoDB AUTO_INCREMENT=21923385 DEFAULT CHARSET=utf8 ROW_FORMAT=COMPACT;";
		
		try {
			start();	
			statement.execute(sql);
		} catch (Exception e) {
			flag = false;
			e.printStackTrace();
		}finally{
			try {
				close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return flag;
	
	}
	public boolean createHTTPGPS(String tablename) {
		boolean flag = true;
		String sql = "CREATE TABLE "
				+ tablename
				+ " ( id int(10) NOT NULL AUTO_INCREMENT,lng decimal(10,6) DEFAULT NULL,lat decimal(10,6) DEFAULT NULL,test_times int(10) DEFAULT NULL,avg_rate decimal(10,3) DEFAULT NULL,province varchar(200) DEFAULT NULL,city varchar(200) DEFAULT NULL,district varchar(200) DEFAULT NULL,street varchar(200) DEFAULT NULL,street_number varchar(200) DEFAULT NULL,city_code varchar(200) DEFAULT NULL,file_type varchar(200) DEFAULT NULL,PRIMARY KEY (id))ENGINE=InnoDB AUTO_INCREMENT=21923385 DEFAULT CHARSET=utf8 ROW_FORMAT=COMPACT;";
		
		try {
			start();	
			statement.execute(sql);
		} catch (Exception e) {
			flag = false;
			e.printStackTrace();
		}finally{
			try {
				close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return flag;
	}
	public boolean createArea(String tablename){
		boolean flag = true;
		String sql = "CREATE TABLE "+tablename+" (" +
				"  `id` int(11) NOT NULL AUTO_INCREMENT," +
				"  `dataNo` int(11) NOT NULL," +
				"  `data` varchar(50) DEFAULT NULL," +
				"  `area` varchar(200) DEFAULT NULL," +
				"  `test_counts` int(10) DEFAULT NULL," +
				"  `grid_counts` int(10) DEFAULT NULL," +
				"  `avg_rate` decimal(10,3) DEFAULT NULL," +
//				"  `fillred` decimal(10,3) DEFAULT NULL," +
//				"  `fillgreen` decimal(10,3) DEFAULT NULL," +
//				"  `fillOpacity` decimal(10,3) DEFAULT NULL," +
				"  PRIMARY KEY (`id`)" +
				")ENGINE=InnoDB AUTO_INCREMENT=21923385 DEFAULT CHARSET=utf8 ROW_FORMAT=COMPACT;";
		try {
			start();
			statement.execute(sql);
		} catch (Exception e) {
			flag = false;
			e.printStackTrace();
		}finally{
			try {
				close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return flag;
	}
	public boolean createAreaHTTP(String tablename){
		boolean flag = true;
		String sql = "CREATE TABLE "+tablename+" (" +
				"  `id` int(11) NOT NULL AUTO_INCREMENT," +
				"  `dataNo` int(11) NOT NULL," +
				"  `data` varchar(50) DEFAULT NULL," +
				"  `area` varchar(200) DEFAULT NULL," +
				"  `test_counts` int(10) DEFAULT NULL," +
				"  `grid_counts` int(10) DEFAULT NULL," +
				"  `top_rate` decimal(10,3) DEFAULT NULL," +
				"  `down_rate` decimal(10,3) DEFAULT NULL," +
//				"  `fillred` decimal(10,3) DEFAULT NULL," +
//				"  `fillgreen` decimal(10,3) DEFAULT NULL," +
//				"  `fillOpacity` decimal(10,3) DEFAULT NULL," +
				"  PRIMARY KEY (`id`)" +
				")ENGINE=InnoDB AUTO_INCREMENT=21923385 DEFAULT CHARSET=utf8 ROW_FORMAT=COMPACT;";
		try {
			start();
			statement.execute(sql);
		} catch (Exception e) {
			flag = false;
			e.printStackTrace();
		}finally{
			try {
				close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return flag;
	}
	public boolean copyAreaDataUp(String tableName,String dataOrg,String dataOrgUp){
		boolean flag = true;
		String sql = "DROP TABLE IF EXISTS `tmp`;";
		try {
			start();
			statement.execute(sql);
			sql = "create table tmp (select * from "+tableName+" where data = '"+dataOrgUp+"' group by area);";
			statement.execute(sql);
			sql = "update tmp set data = '"+dataOrg+"';";
			statement.execute(sql);
			if(tableName.endsWith("_speed_test")){
//				sql = "insert into "+tableName+" (dataNo,data,area,test_counts,grid_counts,top_rate,down_rate,fillred,fillgreen,fillOpacity) (select * from (SELECT dataNo,data,area,test_counts,grid_counts,top_rate,down_rate,fillred,fillgreen,fillOpacity FROM tmp ) tabletmp);";
				sql = "insert into "+tableName+" (dataNo,data,area,test_counts,grid_counts,top_rate,down_rate) (select * from (SELECT dataNo,data,area,test_counts,grid_counts,top_rate,down_rate FROM tmp ) tabletmp);";
			}else{
//				sql = "insert into "+tableName+" (dataNo,data,area,test_counts,grid_counts,avg_rate,fillred,fillgreen,fillOpacity) (select * from (SELECT dataNo,data,area,test_counts,grid_counts,avg_rate,fillred,fillgreen,fillOpacity FROM tmp ) tabletmp);";
				sql = "insert into "+tableName+" (dataNo,data,area,test_counts,grid_counts,avg_rate) (select * from (SELECT dataNo,data,area,test_counts,grid_counts,avg_rate FROM tmp ) tabletmp);";		
			}
			statement.execute(sql);
			sql = "drop table tmp; ";
			statement.execute(sql);
		} catch (Exception e) {
			flag = false;
			e.printStackTrace();
		} finally{
			try {
				close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return flag;
	}
	public void proDealAreaData(String data,String numType, String area, String table, String tablefrom, double m_maxspeed, double m_opacitymin, double m_opacitymax, double m_maxtestnum){
		String sql = "Select (@rownum :=(select max(dataNo) from "+table+" for update)) rownum;";
		try {
			start();
			int rownum = 0;
			ResultSet rs = statement.executeQuery(sql);
			if(rs.next()){
				rownum = rs.getObject("rownum")==null?0:rs.getInt("rownum");
			}else{
				rownum = 0;
			}
			rownum ++ ;
//			double avg = 0;
//			double top = 0;
//			double down = 0;
//			double fillcolorred = 0;
//			double fillcolorgreen = 0; 
//			if(numType.equals("01001")){
//				sql = "select MOD(sum(top_rate*test_times),SUM(test_times)) top,MOD(sum(down_rate*test_times),SUM(test_times)) down from "+tablefrom+"";
//				rs = statement.executeQuery(sql);
//				if(rs.next()){
//					top = rs.getObject("top")==null?0:rs.getDouble("top");
//					down = rs.getObject("down")==null?0:rs.getDouble("down");
//				}
//				fillcolorred = updateGPSDateDao.getFillcolorred(down);
//				fillcolorgreen = updateGPSDateDao.getFillcolorgreen(down);
//			}else{
//				sql = "select MOD(sum(avg_rate*test_times),SUM(test_times)) avg from "+tablefrom+"";
//				rs = statement.executeQuery(sql);
//				if(rs.next()){
//					avg = rs.getObject("avg")==null?0:rs.getDouble("avg");
//				}
//				fillcolorred = updateGPSDateDao.getFillcolorred(avg);
//				fillcolorgreen = updateGPSDateDao.getFillcolorgreen(avg);
//			}
			//当前为平均速率的计算 
			
			int count = 0;
			sql = "select count(lng+lat) count from "+tablefrom+"";
			rs = statement.executeQuery(sql);
			if(rs.next()){
				count = rs.getObject("count")==null?0:rs.getInt("count");
			}
			double fillOpacity = updateGPSDateDao.getfillOpacity(count);
			if(numType.equals("01001")){
//				sql = "insert into "+table+" (data,area,test_counts,grid_counts,top_rate,down_rate,fillred,fillgreen,fillOpacity,dataNo) (select *,"+rownum+" from (select "+data+","+area+",sum(test_times),count(lng+lat),MOD(sum(top_rate*test_times),SUM(test_times)) top,MOD(sum(down_rate*test_times),SUM(test_times)) down,"+fillcolorred+","+fillcolorgreen+","+fillOpacity+" from "+tablefrom+" group by '"+area+"') table_tmp)";
				//sql = "insert into "+table+" (data,area,test_counts,grid_counts,top_rate,down_rate,dataNo) (select *,"+rownum+" from (select "+data+","+area+",sum(test_times),count(lng+lat),MOD(sum(top_rate*test_times),SUM(test_times)) top,MOD(sum(down_rate*test_times),SUM(test_times)) down from "+tablefrom+" where "+area+" is not null group by "+area+") table_tmp)";
				sql = "insert into "+table+" (data,area,dataNo) (select *,"+rownum+" from (select "+data+","+area+" from "+tablefrom+" where "+area+" is not null group by "+area+") table_tmp)";
			}else{
//				sql = "insert into "+table+" (data,area,test_counts,grid_counts,avg_rate,fillred,fillgreen,fillOpacity,dataNo) (select *,"+rownum+" from (select "+data+","+area+",sum(test_times),count(lng+lat),MOD(sum(avg_rate*test_times),SUM(test_times)) avg,"+fillcolorred+","+fillcolorgreen+","+fillOpacity+" from "+tablefrom+" group by '"+area+"') table_tmp)";
				//sql = "insert into "+table+" (data,area,test_counts,grid_counts,avg_rate,dataNo) (select *,"+rownum+" from (select "+data+","+area+",sum(test_times),count(lng+lat),MOD(sum(avg_rate*test_times),SUM(test_times)) avg from "+tablefrom+" where "+area+" is not null group by "+area+") table_tmp)";
				sql = "insert into "+table+" (data,area,dataNo) (select *,"+rownum+" from (select "+data+","+area+" from "+tablefrom+" where "+area+" is not null group by "+area+") table_tmp)";
			}
			statement.execute(sql);
		} catch (Exception e) {
			e.printStackTrace();
		} finally{
			try {
				close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	/**
	 * 执行存储过程或者函数
	 * @param name
	 */
	public void execute(String name) {
		start(); 	
        CallableStatement stat = null;
        String sql = "{call "+name+"()}";
        try {
            stat = conn.prepareCall(sql);
            stat.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }finally{
        	if(stat!=null){
        		try {
					stat.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
        	}
            try {
				close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
        }
	}
	/**
	 * 一个字符串参数的存储过程调用
	 * @param name
	 * @param value
	 */
	public void execute(String name,String value) {
		start(); 	
        CallableStatement stat = null;
        String sql = "{call "+name+" (?)}";
        try {
            stat = conn.prepareCall(sql);
            stat.setString(1, value);
            stat.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }finally{
        	if(stat!=null){
        		try {
					stat.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
        	}
            try {
				close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
        }
		
	}
}
