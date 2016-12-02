package com.opencassandra.v01.dao.impl;

import java.io.BufferedReader;
import java.io.File;
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
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.sf.json.JSONObject;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;

import com.mysql.jdbc.exceptions.MySQLSyntaxErrorException;
import com.opencassandra.descfile.ConfParser;
import com.sun.org.apache.xml.internal.utils.ObjectPool;



public class CopyOfInsertMysqlDao{
	Statement statement;
	Connection conn;
	StringBuffer sql = new StringBuffer();
	private static String ak = ConfParser.ak;
	
	public static void main(String[] args) {
		String dbName = "test_wql_0625";
		if(!ConfParser.url.endsWith("/")){
			ConfParser.url = ConfParser.url.substring(0,ConfParser.url.lastIndexOf("/"));
			ConfParser.url = ConfParser.url+"/"+dbName;
		}
		InsertMysqlByDataToGPS insert = new InsertMysqlByDataToGPS();
		System.out.println("url:"+ConfParser.url);
		CopyOfInsertMysqlDao insertMysqlDao = new CopyOfInsertMysqlDao();
		try {
			insertMysqlDao.start();	
		}catch (MySQLSyntaxErrorException e) {
			insertMysqlDao.createDB(dbName);
			try {
				insertMysqlDao.start();
			} catch (Exception e1) {
				e1.printStackTrace();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		try {
			insertMysqlDao.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public void createDB(String database){
		try {
			String oldUrl = ConfParser.url;
			ConfParser.url = ConfParser.url.substring(0,ConfParser.url.lastIndexOf("/"));
			start();
			String sql = "create DATABASE "+database;
			statement.execute(sql);
			close();
			ConfParser.url = oldUrl;
			start();
			//创建map_level表
			sql = "CREATE TABLE "+database+".map_level (" +
					"  `level` int(20) NOT NULL," +
					"  `loninterval` varchar(20) NOT NULL," +
					"  `latinterval` varchar(20) NOT NULL," +
					"  PRIMARY KEY (`level`)" +
					") ENGINE=InnoDB DEFAULT CHARSET=utf8;";
			statement.execute(sql);
			System.out.println(ConfParser.url);
			sql = "INSERT INTO  "+database+".map_level values (1,0.0001,0.00008),(2,0.0002,0.00016),(5,0.0005,0.0004),(10,0.001,0.0008),(40,0.004,0.0032),(150,0.015,0.012),(600,0.06,0.048),(2500,0.25,0.2)";
			System.out.println(sql);
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
	public void start() throws Exception {
		String driver = "com.mysql.jdbc.Driver";
		// String url = "jdbc:mysql://192.168.85.233:3306/testdataanalyse";
		String url = ConfParser.url;
		System.out.println("******************"+url);
		String user = ConfParser.user;
		// String password = "cmrictpdata";
		String password = ConfParser.password;
		Class.forName(driver);
		conn = DriverManager.getConnection(url, user, password);
		statement = conn.createStatement();
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
	
	public String insertMysqlDB(Map dataMap, String keyspace, String numType,
			File file, String detailreport) {
		System.out.println("开始insertMysqlDB************："+new Date().toLocaleString() +"   毫秒："+new Date().getTime());
		StringBuffer sql = new StringBuffer("");
		String destFilePath = ConfParser.backReportPath;
		/*String filePath = file.getAbsolutePath();

		String path1 = filePath.substring(0, filePath.lastIndexOf("."));
		String filenameorg = path1.substring(path1.lastIndexOf("."), path1
				.length());
		String[] files = new String[] { filenameorg, ".detail", ".deviceInfo",
				".monitor" };
		String[] filepaths = new String[4];
		if (detailreport.equals("")) {
			for (int i = 0; i < files.length; i++) {
				String newfilePath = file.getAbsolutePath().replace(
						filenameorg, files[i]);
				filepaths[i] = newfilePath;
				File newFile = new File(newfilePath);
				if (newFile.exists()) {
					detailreport += "1";
				} else {
					detailreport += "0";
				}
			}
		}*/
		boolean flag = false;
		if (!numType.isEmpty()) {
			System.out.println("开始准备进行解析数据************："+new Date().toLocaleString() +"   毫秒："+new Date().getTime());
			if (numType.equals("01001")) {
				sql = insertSpeedTest(dataMap, keyspace, numType, detailreport);
			} else if (numType.equals("02001")) {
				sql = insertWebBrowser(dataMap, keyspace, numType, detailreport);
			} else if (numType.equals("02011")) {
				sql = insertVideoTest(dataMap, keyspace, numType, detailreport);
			} else if (numType.equals("03001") || numType.equals("03011")) {
				sql = insertHTTP(dataMap, keyspace, numType, detailreport);
			} else if (numType.equals("04002")) {
				String flag1 = dataMap.get("flag") == null ? "" : dataMap.get(
						"flag").toString();
				if (flag1.equals("1")) {
					sql = insertPINGManyToOne(dataMap, keyspace, numType,
							detailreport);
				} else {
					sql = insertPING(dataMap, keyspace, numType, detailreport);
				}
			} else if (numType.equals("05001")) {
				sql = insertCall(dataMap, keyspace, numType, detailreport);
			} else if (numType.equals("04006")) {
				sql = insertResideTest(dataMap, keyspace, numType, detailreport);
			} else if (numType.equals("05002")) {
				sql = insertSMS(dataMap, keyspace, numType, detailreport);
			}
			System.out.println("开始准备插入数据************："+new Date().toLocaleString() +"   毫秒："+new Date().getTime());
			if (sql.toString().isEmpty()) {
				flag = false;
			} else {
				try {
					start();
					insert(sql.toString());
					flag = true;
				} catch (Exception e) {
					flag = false;
					System.out.println("执行sql语句错误");
					e.printStackTrace();
				} finally {
					try {
						close();
					} catch (SQLException e) {
						e.printStackTrace();
					}
				}
				/*System.out.println("结束插入数据  开始备份文件************："+new Date().toLocaleString() +"   毫秒："+new Date().getTime());
				for (int i = 0; i < filepaths.length; i++) {
					String newfilePath = filepaths[i];
					if (newfilePath == null || newfilePath.isEmpty()) {
						continue;
					}
					File destFile = new File(newfilePath.replace(
							ConfParser.org_prefix, destFilePath));
					File newFile = new File(newfilePath);
					if (!newFile.exists()) {
						continue;
					}
					if (destFile.exists()) {
						destFile.delete();
					}
					if (!destFile.exists()) {
						if (!destFile.getParentFile().exists()) {
							destFile.getParentFile().mkdirs();
						}
						try {
							destFile.createNewFile();
							FileUtils.copyFile(newFile, destFile);
						} catch (IOException e) {
							e.printStackTrace();
						}finally{
							newFile.delete();
						}
					}
				}
				System.out.println("结束备份************："+new Date().toLocaleString() +"   毫秒："+new Date().getTime());*/
			}
		}
		System.out.println("更新数据：" + flag);
		return detailreport;
	}
	/**
	 * 插入speedTest数据
	 */
	public StringBuffer insertSpeedTest(Map dataMap,String keyspace,String numType,String detailreport){
		List nameList = new ArrayList();
		List valueList = new ArrayList();
		Map map = new HashMap();
		Set keySet = dataMap.keySet();
		Iterator iter = keySet.iterator();
		
		String imei = (String)(dataMap.get("imei")==null?"":dataMap.get("imei"));
		String file_path = (String)(dataMap.get("filePath")==null?"":dataMap.get("filePath"));
		String detailreportvalue = (String)(dataMap.get("detailreport")==null?"":dataMap.get("detailreport"));
		map.put("detailreport", detailreport);
		map.put("imei",imei);
		map.put("file_path",file_path.replace("\\", "|").replace("//", "|"));
		
		String lac_tac = "";
		String cid_pci = "";
		String gpsStr = "";
		String latitude = "";
		String longitude = "";
		String model = "";//终端类型
		String logversion = "";//软件版本
		String device_org = keyspace;//终端分组
		String time = dataMap.get("time")==null?"":dataMap.get("time").toString();//测试时间
		
		map.put("device_org", device_org);
		map.put("time", time);
		
		String location = "";//测试地点
		String province = "";//测试省
		String district = "";//测试district
		String city = "";//测试city
		String max_up = "";//最大上行速率
		String max_down = "";//最大下行速率
		String avg_up = "";//上行平均速率
		String avg_down = "";//下行平均速率
		String protocol = "";//测试类型
		String downlink = "";//下行线程数
		String uplink = "";//上行线程数
		String delay = "";//时延
		String server_ip = "";//服务器信息
		String net_type = "";//网络类型
		String signal_strength = "";//信号迁都
		String sinr = "";//SINR
		
		String net_type1 = "";
		String signal_strength1 = "";
		String sinr1 = "";
		String cell1 = "";
		String net_type2 = "";
		String signal_strength2 = "";
		String sinr2 = "";
		String cell2 = "";
		String wifi_ss = "";//WIFI SIGNAL STRENGTH
		
		boolean flag1 = false;
		boolean flag2 = false;
		
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
		map.put("gps", gpsStr);
		if(gpsStr.contains(" ")){
			String[] gps = transGpsPoint(gpsStr);
			if(gps!=null && gps[0]!=null && gps[1]!=null){
				longitude = gps[0];
				latitude = gps[1];
			}
		}
		double longitudeNum = 0.0;
		double latitudeNum = 0.0;
		if(longitude.isEmpty() && latitude.isEmpty()){
			
		}else{
			double[] em = Earth2Mars.transform(Double.parseDouble(longitude), Double.parseDouble(latitude));
			double[] bf = BdFix.bd_encrypt(em[0], em[1]);
			longitudeNum = bf[0];
			latitudeNum = bf[1];
		}
		map.put("longitude",longitudeNum);
		map.put("latitude",latitudeNum);
		if(longitudeNum == 0.0 || longitudeNum == 0){
			map.put("longitude","");
		}
		if(latitudeNum == 0.0 || latitudeNum == 0){
			map.put("latitude","");
		}
		//获取GPS位置信息----end
		if(dataMap.containsKey("网络(1)网络制式") || dataMap.containsKey("Network(1) Type") || dataMap.containsKey("Network (1) standard")){
			flag1 = true;
		}
		if(dataMap.containsKey("网络(2)网络制式") || dataMap.containsKey("Network(2) Type") || dataMap.containsKey("Network (1) standard")){
			flag2 = true;
		}
		while (iter.hasNext()) {
			String key = iter.next() + "";
			if (key == null || key.isEmpty()) {
				continue;
			}
			
			if("平均速率(Kbps)".equals(key) || "平均速率(Mbps)".equals(key) ||"平均速率（Mbps）".equals(key) || "average speed (mbps)".equals(key.toLowerCase()) || "average speed (kbps)".equals(key.toLowerCase()) || "avg speed(kbps)".equals(key.toLowerCase()) || "avg speed(mbps)".equals(key.toLowerCase())){
			}else if("logversion".equals(key.toLowerCase()) || "日志版本".equals(key)){
				logversion = (String) (dataMap.get(key)==null?"":dataMap.get(key));
				map.put("logversion", logversion);
			}else if("信号强度".equals(key) || "Signal Strength".equals(key)){
				signal_strength = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("终端类型".equals(key) || "终端型号".equals(key) || "device type".equals(key.toLowerCase()) || "terminal model".equals(key.toLowerCase()) || "terminal type".equals(key.toLowerCase()) || "terminal mode".equals(key.toLowerCase()) || "device model".equals(key.toLowerCase()) || "terminal models".equals(key.toLowerCase())){
				model = (String) (dataMap.get(key)==null?"":dataMap.get(key));
				map.put("model",model);
			/*}else if("LAC".equals(key)){
				String lacStr = (String) (dataMap.get(key)==null?"":dataMap.get(key));
				if(lacStr.indexOf("=")!=-1){
					lac = lacStr.substring(lacStr.indexOf("=")+1,lacStr.length());
				}else{
					lac = lacStr;
				}
			}else if("CID".equals(key)){
				String cidStr = (String) (dataMap.get(key)==null?"":dataMap.get(key));
				if(cidStr.indexOf("=")!=-1){
					cid = cidStr.substring(cidStr.indexOf("=")+1,cidStr.length());
				}else{
					cid = cidStr;
				}
			}else if("LAC/CID".equals(key)){
				String lacCid = (String) (dataMap.get(key)==null?"":dataMap.get(key));
				if(lacCid.equals("N/A/N/A")){
					lac = "N/A";
					cid = "N/A";
				}else{
					if(lacCid.indexOf("/")!=-1 || lacCid.indexOf("N/A")!=-1){
						String[] cellInfo = lacCid.split("/");
						lac = cellInfo[0];
						cid = cellInfo[1];
					}
				}
			}else if("TAC".equals(key)){
				String tacStr = (String) (dataMap.get(key)==null?"":dataMap.get(key));
				if(tacStr.indexOf("=")!=-1){
					tac = tacStr.substring(tacStr.indexOf("=")+1,tacStr.length()-1);
				}else{
					tac = tacStr;
				}
			}else if("PCI".equals(key)){
				String pciStr = (String) (dataMap.get(key)==null?"":dataMap.get(key));
				if(pciStr.indexOf("=")!=-1){
					pci = pciStr.substring(pciStr.indexOf("=")+1,pciStr.length()-1);
				}else{
					pci = pciStr;
				}
			}else if("TAC/PCI".equals(key)){
				String tacPci = (String) (dataMap.get(key)==null?"":dataMap.get(key));
				if(tacPci.equals("N/A/N/A")){
					tac = "N/A";
					pci = "N/A";
				}else{
					if(tacPci.indexOf("/")!=-1 || tacPci.indexOf("N/A")!=-1){
						String[] cellInfo = tacPci.split("/");
						tac = cellInfo[0];
						pci = cellInfo[1];
					}
				}*/
			}else if("上行速率".equals(key)||"上行速率(Mbps)".equals(key) || "Up Speed".equals(key) || "Up Speed(Mbps)".equals(key) || "Up Speed(Kbps)".equals(key) || "Upload Speed".equals(key) || "Upload Speed(Mbps)".equals(key)){
				avg_up = (String) (dataMap.get(key)==null?"":dataMap.get(key));
				map.put("avg_up",avg_up);
			}else if("下行速率".equals(key)||"下行速率(Mbps)".equals(key)  || "Down Speed".equals(key) || "Down Speed(Mbps)".equals(key) || "Down Speed(Kbps)".equals(key) || "Download Speed".equals(key) || "Download Speed(Mbps)".equals(key)){
				avg_down = (String) (dataMap.get(key)==null?"":dataMap.get(key));
				map.put("avg_down",avg_down);
			}else if("maximum up".equals(key.toLowerCase()) || "maximum down".equals(key.toLowerCase()) || "上行最大速率(Mbps)".equals(key)){
				max_up = (String) (dataMap.get(key)==null?"":dataMap.get(key));
				map.put("max_up",max_up);
			}else if("maximum down".equals(key.toLowerCase()) || "maximum down(mbps)".equals(key.toLowerCase()) || "下行最大速率(Mbps)".equals(key)){
				max_down = (String) (dataMap.get(key)==null?"":dataMap.get(key));
				map.put("max_down",max_down);
			}else if("时延".equals(key) || "时延(ms)".equals(key) || "平均时延(ms)".equals(key) || "平均时延(ms)".equals(key) || "平均时延".equals(key) || "average delay(ms)".equals(key.toLowerCase()) || "avg latency(ms)".equals(key.toLowerCase()) || "avg time(ms)".equals(key.toLowerCase()) || "avg latency".equals(key.toLowerCase()) || "delay".equals(key.toLowerCase()) || "delay(ms)".equals(key.toLowerCase()) || "latency".equals(key.toLowerCase()) || "latency(ms)".equals(key.toLowerCase())){
				delay = (String) (dataMap.get(key)==null?"":dataMap.get(key));
				map.put("delay",delay);
			}else if("服务器信息".equals(key) || "服务器地址".equals(key) || "Server Info".equals(key) || "Service Info".equals(key)){
				server_ip = (String) (dataMap.get(key)==null?"":dataMap.get(key));
				map.put("server_ip",server_ip);
			}else if("测速类型".equals(key) || "测试类型".equals(key) || "Protocol".equals(key)){
				protocol = (String) (dataMap.get(key)==null?"":dataMap.get(key));
				map.put("protocol",protocol);
			}else if("上行线程数".equals(key) || "Uplink Thread(s)".equals(key) || "Uplink Thread Count".equals(key) || "Uplink Thread Counts".equals(key)){
				uplink = (String) (dataMap.get(key)==null?"":dataMap.get(key));
				map.put("uplink",uplink);
			}else if("下行线程数".equals(key) || "Downlink Thread(s)".equals(key) || "Downlink Thread Count".equals(key) || "Downlink Thread Counts".equals(key) ){
				downlink = (String) (dataMap.get(key)==null?"":dataMap.get(key));
				map.put("downlink",downlink);
			}else if("网络类型".equals(key) || "网络制式".equals(key) || "network type".equals(key.toLowerCase())){
				net_type = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("SINR".equals(key)){
				sinr = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("网络(1)网络制式".equals(key) || "network(1) type".equals(key.toLowerCase()) || "network (1) standard".equals(key.toLowerCase())){
				net_type1 = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("网络(1)信号强度".equals(key) || "网络(1)信号强度(dBm)".equals(key) || "network(1) signal strength".equals(key.toLowerCase()) || "network(1) signal strength(dbm)".equals(key.toLowerCase())){
				signal_strength1 = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("网络(1)小区信息".equals(key) || "network(1) cell".equals(key.toLowerCase()) || "network(1)".equals(key.toLowerCase())){
				cell1 = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("网络(1)SINR".equals(key) || "网络(1)SINR(dB)".equals(key) || "network(1) sinr".equals(key.toLowerCase()) || "network(1) sinr(db)".equals(key.toLowerCase())){
				sinr1 = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("网络(2)网络制式".equals(key) || "network(2) type".equals(key.toLowerCase()) || "network (2) standard".equals(key.toLowerCase())){
				net_type2 = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("网络(2)信号强度".equals(key) || "网络(2)信号强度(dBm)".equals(key)	 || "network(2) signal strength".equals(key.toLowerCase())){
				signal_strength2 = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("网络(2)小区信息".equals(key) || "network(2) cell".equals(key.toLowerCase()) || "network(2)".equals(key.toLowerCase())){
				cell2 = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("网络(2)SINR".equals(key) || "网络(2)SINR(dB)".equals(key) || "network(2) sinr".equals(key.toLowerCase()) || "network(2) sinr(db)".equals(key.toLowerCase())){
				sinr2 = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("WIFI信号强度".equals(key) || "WIFI信号强度(dBm)".equals(key) || "wifi signal strength".equals(key.trim().toLowerCase()) || "wifi signal strength(dbm)".equals(key.trim().toLowerCase())){
				wifi_ss = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}
		}
		if(max_down.equals("") && !avg_down.isEmpty()){
			max_down = avg_down;
			map.put("max_down", max_down);
		}
		if(max_up.equals("") && !avg_up.isEmpty()){
			max_up = avg_up;
			map.put("max_up", max_up);
		}
		String lac1 = "";
		String cid1 = "";
		if(cell1.indexOf("/")!=-1 || cell1.indexOf("N/A")!=-1){
			String[] cellInfo = cell1.split("/");
			if(cellInfo[0]!=null && !cellInfo[0].equals("")){
				lac1 = cellInfo[0];
			}
			/*if(tac!=null && !tac.equals("")){
				lac1 = tac;
			}*/
			if(cellInfo[1]!=null && !cellInfo[1].equals("")){
				cid1 = cellInfo[1];
			}
			/*if(pci!=null && !pci.equals("")){
				cid1 = pci;
			}
			if(tac!=null && !tac.equals("") && cellInfo[0]!=null && !cellInfo[0].equals("")){
				lac1 = cellInfo[0]+"|"+tac;
			}
			if(pci!=null && !pci.equals("") && cellInfo[1]!=null && !cellInfo[1].equals("")){
				cid1 = cellInfo[1]+"|"+pci;
			}*/
		}
		String lac2 = "";
		String cid2 = "";
		if(cell2.indexOf("/")!=-1 || cell2.indexOf("N/A")!=-1){
			String[] cellInfo = cell2.split("/");
			if(cellInfo[0]!=null && !cellInfo[0].equals("")){
				lac2 = cellInfo[0];
			}
			/*if(tac!=null && !tac.equals("")){
				lac2 = tac;
			}*/
			if(cellInfo[1]!=null && !cellInfo[1].equals("")){
				cid2 = cellInfo[1];
			}
			/*if(pci!=null && !pci.equals("")){
				cid2 = pci;
			}
			if(tac!=null && !tac.equals("") && cellInfo[0]!=null && !cellInfo[0].equals("")){
				lac2 = cellInfo[0]+"|"+tac;
			}
			if(pci!=null && !pci.equals("") && cellInfo[1]!=null && !cellInfo[1].equals("")){
				cid2 = cellInfo[1]+"|"+pci;
			}*/
		}
		//网络(1)信息存在时 当前数据返回的是网络(1)的信息
		if(flag1 && net_type1.equals(net_type)){
			sinr = sinr1;
			net_type = net_type1;
			signal_strength = signal_strength1;
			lac_tac = lac1;
			cid_pci = cid1;
		}
		//网络(2)信息存在时 当前数据返回的是网络(2)的信息
		if(flag2 && net_type2.equals(net_type)){
			sinr = sinr2;
			net_type = net_type2;
			signal_strength = signal_strength2;
			lac_tac = lac2;
			cid_pci = cid2;
		}
		if(!(flag1 && net_type.equals(net_type1)) && !(flag2 && net_type.equals(net_type2))){
			/*if(tac!=null && !tac.equals("")){
				lac_tac = tac;
			}
			if(pci!=null && !pci.equals("")){
				cid_pci = pci;
			}
			if(tac!=null && !tac.equals("") && lac!=null && !lac.equals("")){
				lac_tac = lac+"|"+tac;
			}
			if(pci!=null && !pci.equals("") && cid!=null && !cid.equals("")){
				cid_pci = cid+"|"+pci;
			}*/
			if(lac1 != null){
				lac_tac = lac1;
			}
			if(cid1 != null){
				cid_pci = cid1;
			}
			if(lac2 != null){
				lac_tac = lac2;
			}
			if(cid2 != null){
				cid_pci = cid2;
			}
		}
		map.put("lac_tac",lac_tac);
		map.put("cid_pci",cid_pci);
		map.put("sinr",sinr);
		if(net_type.equals("WIFI")){
			map.put("signal_strength",wifi_ss);	
		}else{
			map.put("signal_strength",signal_strength);
		}
		map.put("net_type",net_type);
		
		map.put("sinr1", sinr1);
		map.put("net_type1", net_type1);
		map.put("signal_strength1", signal_strength1);
		map.put("lac_tac1", lac1);
		map.put("cid_pci1", cid1);
		
		map.put("sinr2", sinr2);
		map.put("net_type2", net_type2);
		map.put("signal_strength2", signal_strength2);
		map.put("lac_tac2", lac2);
		map.put("cid_pci2", cid2);
		
		String file_index = (String)dataMap.get("file_index");
		String system_version = "";
		if(model.toLowerCase().contains("iphone")){
			map.put("android_ios", "ios");	
		}else{
			map.put("android_ios", "android");
		}
		map.put("file_index", file_index);
		sql = appendSql(map,numType,file_index);
		return sql;
	}
	/**
	 * 插入WebBrowser数据
	 */
	public StringBuffer insertWebBrowser(Map dataMap,String keyspace,String numType,String detailreport){
		List nameList = new ArrayList();
		List valueList = new ArrayList();
		Map map = new HashMap();
		Set keySet = dataMap.keySet();
		Iterator iter = keySet.iterator();
		String imei = (String)(dataMap.get("imei")==null?"":dataMap.get("imei"));
		String file_path = (String)(dataMap.get("filePath")==null?"":dataMap.get("filePath"));
		String detailreportvalue = (String)(dataMap.get("detailreport")==null?"":dataMap.get("detailreport"));
		map.put("detailreport", detailreport);
		
		map.put("imei",imei);
		map.put("file_path",file_path.replace("\\", "|").replace("//", "|"));
		
		String device_org = keyspace;//终端分组
		
		String longitude = "";
		String latitude = "";
		String province = "";//测试省
		String district = "";//测试district
		String city = "";//测试city
		String time = dataMap.get("time")==null?"":dataMap.get("time").toString();//测试时间
		String model = "";//终端类型
		String logversion = "";//软件版本
		map.put("device_org", device_org);
		map.put("time", time);
		
		String web_sit = "";//访问地址
		String eighty_loading = "";//80%加载时长
		String eighty_rate = "";//80%加载速率
		String full_complete = "";//100%加载时长
		String reference = "";//加载速率
		String success_rate = "";//成功率
		String success_counts = "";//成功次数

		String location = "";//测试地点
		String gpsStr = "";
		String net_type = "";//网络类型
		String signal_strength = "";//信号强度
		String sinr = "";//SINR
		String lac = "";
		String cid = "";
		String tac = "";
		String pci = "";
		String lac_tac = "";
		String cid_pci = "";
		String net_type1 = "";
		String signal_strength1 = "";
		String sinr1 = "";
		String cell1 = "";
		String net_type2 = "";
		String signal_strength2 = "";
		String sinr2 = "";
		String cell2 = "";
		String wifi_ss = "";//WIFI SIGNAL STRENGTH
		
		boolean flag1 = false;
		boolean flag2 = false;
		
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
		map.put("gps", gpsStr);
		if(gpsStr.contains(" ")){
			String[] gps = transGpsPoint(gpsStr);
			if(gps!=null && gps[0]!=null && gps[1]!=null){
				longitude = gps[0];
				latitude = gps[1];
			}
		}
		double longitudeNum = 0.0;
		double latitudeNum = 0.0;
		if(longitude.isEmpty() && latitude.isEmpty()){
			
		}else{
			double[] em = Earth2Mars.transform(Double.parseDouble(longitude), Double.parseDouble(latitude));
			double[] bf = BdFix.bd_encrypt(em[0], em[1]);
			longitudeNum = bf[0];
			latitudeNum = bf[1];
		}
		map.put("longitude",longitudeNum);
		map.put("latitude",latitudeNum);
		if(longitudeNum == 0.0 || longitudeNum == 0){
			map.put("longitude","");
		}
		if(latitudeNum == 0.0 || latitudeNum == 0){
			map.put("latitude","");
		}
		
		
		//获取GPS位置信息----end
		if(dataMap.containsKey("网络(1)网络制式") || dataMap.containsKey("Network(1) Type") || dataMap.containsKey("Network (1) standard")){
			flag1 = true;
		}
		if(dataMap.containsKey("网络(2)网络制式") || dataMap.containsKey("Network(2) Type") || dataMap.containsKey("Network (1) standard")){
			flag2 = true;
		}
		while (iter.hasNext()) {
			String key = iter.next() + "";
			if (key == null || key.isEmpty()) {
				continue;
			}
			if("平均速率(Kbps)".equals(key) || "平均速率(Mbps)".equals(key) ||"平均速率（Mbps）".equals(key) || "average speed (mbps)".equals(key.toLowerCase()) || "average speed (kbps)".equals(key.toLowerCase()) || "avg speed(kbps)".equals(key.toLowerCase()) || "avg speed(mbps)".equals(key.toLowerCase())){
				reference = (String) (dataMap.get(key)==null?"":dataMap.get(key));
				map.put("reference", reference);
			}else if("logversion".equals(key.toLowerCase()) || "日志版本".equals(key)){
				logversion = (String) (dataMap.get(key)==null?"":dataMap.get(key));
				map.put("logversion", logversion);
			}else if("信号强度".equals(key) || "Signal Strength".equals(key)){
				signal_strength = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("终端类型".equals(key) || "终端型号".equals(key) || "device type".equals(key.toLowerCase()) || "terminal model".equals(key.toLowerCase()) || "terminal type".equals(key.toLowerCase()) || "terminal mode".equals(key.toLowerCase()) || "device model".equals(key.toLowerCase()) || "terminal models".equals(key.toLowerCase())){
				model = (String) (dataMap.get(key)==null?"":dataMap.get(key));
				map.put("model",model);
			}else if("地址".equals(key) || "网址".equals(key) || "Website".equals(key) || "address".equals(key)){
				web_sit = (String) (dataMap.get(key)==null?"":dataMap.get(key));
				map.put("web_sit",web_sit);
			}else if("加载80%的平均时长(ms)".equals(key) || "加载80%平均时长(ms)".equals(key) || "加载80%的平均时长".equals(key) || "加载80%平均时长".equals(key) || "80% Avg Time".equals(key) || "80% Avg Time(ms)".equals(key)){
				eighty_loading = (String) (dataMap.get(key)==null?"":dataMap.get(key));
				map.put("eighty_loading",eighty_loading);
			}else if("加载80%的平均速率(Kbps)".equals(key) || "加载80%平均速率(Kbps)".equals(key) || "加载80%的平均速率".equals(key) || "加载80%平均速率".equals(key) || "80% Avg Speed".equals(key) || "80% Avg Speed(Kbps)".equals(key)){
				eighty_rate = (String) (dataMap.get(key)==null?"":dataMap.get(key));
				map.put("eighty_rate",eighty_rate);
			}else if("平均时长(ms)".equals(key) || "平均时延".equals(key) || "平均时延(ms)".equals(key) || "Avg Latency(ms)".equals(key) || "Avg Time(ms)".equals(key) || "Latency".equals(key) || "Delay".equals(key) || "Delay(ms)".equals(key)){
				full_complete = (String) (dataMap.get(key)==null?"":dataMap.get(key));
				map.put("full_complete",full_complete);	
			}else if("成功率".equals(key) || "success rate".equals(key.toLowerCase())){
				success_rate = (String)(dataMap.get(key)==null?"":dataMap.get(key));
				map.put("success_rate", success_rate);
			}else if("成功次数".equals(key) || "success counts".equals(key.toLowerCase()) || "success count".equals(key.toLowerCase()) ){
				success_counts = (String)(dataMap.get(key)==null?"":dataMap.get(key));
				map.put("success_counts", success_counts);
			}else if("网络类型".equals(key) || "网络制式".equals(key) || "network type".equals(key.toLowerCase())){
				net_type = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("SINR".equals(key)){
				sinr = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("网络(1)网络制式".equals(key) || "network(1) type".equals(key.toLowerCase()) || "network (1) standard".equals(key.toLowerCase())){
				net_type1 = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("网络(1)信号强度".equals(key) || "网络(1)信号强度(dBm)".equals(key) || "network(1) signal strength".equals(key.toLowerCase()) || "network(1) signal strength(dbm)".equals(key.toLowerCase())){
				signal_strength1 = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("网络(1)小区信息".equals(key) || "network(1) cell".equals(key.toLowerCase()) || "network(1)".equals(key.toLowerCase())){
				cell1 = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("网络(1)SINR".equals(key) || "网络(1)SINR(dB)".equals(key) || "network(1) sinr".equals(key.toLowerCase()) || "network(1) sinr(db)".equals(key.toLowerCase())){
				sinr1 = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("网络(2)网络制式".equals(key) || "network(2) type".equals(key.toLowerCase()) || "network (2) standard".equals(key.toLowerCase())){
				net_type2 = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("网络(2)信号强度".equals(key) || "网络(2)信号强度(dBm)".equals(key)	 || "network(2) signal strength".equals(key.toLowerCase())){
				signal_strength2 = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("网络(2)小区信息".equals(key) || "network(2) cell".equals(key.toLowerCase()) || "network(2)".equals(key.toLowerCase())){
				cell2 = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("网络(2)SINR".equals(key) || "网络(2)SINR(dB)".equals(key) || "network(2) sinr".equals(key.toLowerCase()) || "network(2) sinr(db)".equals(key.toLowerCase())){
				sinr2 = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("WIFI信号强度".equals(key) || "WIFI信号强度(dBm)".equals(key) || "wifi signal strength".equals(key.trim().toLowerCase()) || "wifi signal strength(dbm)".equals(key.trim().toLowerCase())){
				wifi_ss = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}
		}
		String lac1 = "";
		String cid1 = "";
		if(cell1.indexOf("/")!=-1 || cell1.indexOf("N/A")!=-1){
			String[] cellInfo = cell1.split("/");
			if(cellInfo[0]!=null && !cellInfo[0].equals("")){
				lac1 = cellInfo[0];
			}
			if(cellInfo[1]!=null && !cellInfo[1].equals("")){
				cid1 = cellInfo[1];
			}
		}
		String lac2 = "";
		String cid2 = "";
		if(cell2.indexOf("/")!=-1 || cell2.indexOf("N/A")!=-1){
			String[] cellInfo = cell2.split("/");
			if(cellInfo[0]!=null && !cellInfo[0].equals("")){
				lac2 = cellInfo[0];
			}
			if(cellInfo[1]!=null && !cellInfo[1].equals("")){
				cid2 = cellInfo[1];
			}
		}
		//网络(1)信息存在时 当前数据返回的是网络(1)的信息
		if(flag1 && net_type1.equals(net_type)){
			sinr = sinr1;
			net_type = net_type1;
			signal_strength = signal_strength1;
			lac_tac = lac1;
			cid_pci = cid1;
		}
		//网络(2)信息存在时 当前数据返回的是网络(2)的信息
		if(flag2 && net_type2.equals(net_type)){
			sinr = sinr2;
			net_type = net_type2;
			signal_strength = signal_strength2;
			lac_tac = lac2;
			cid_pci = cid2;
		}
		if(!(flag1 && net_type.equals(net_type1)) && !(flag2 && net_type.equals(net_type2))){
			if(lac1 != null){
				lac_tac = lac1;
			}
			if(cid1 != null){
				cid_pci = cid1;
			}
			if(lac2 != null){
				lac_tac = lac2;
			}
			if(cid2 != null){
				cid_pci = cid2;
			}
		}
		map.put("lac_tac",lac_tac);
		map.put("cid_pci",cid_pci);
		map.put("sinr",sinr);
		if(net_type.equals("WIFI")){
			map.put("signal_strength",wifi_ss);	
		}else{
			map.put("signal_strength",signal_strength);
		}
		map.put("net_type",net_type);
		
		map.put("sinr1", sinr1);
		map.put("net_type1", net_type1);
		map.put("signal_strength1", signal_strength1);
		map.put("lac_tac1", lac1);
		map.put("cid_pci1", cid1);
		
		map.put("sinr2", sinr2);
		map.put("net_type2", net_type2);
		map.put("signal_strength2", signal_strength2);
		map.put("lac_tac2", lac2);
		map.put("cid_pci2", cid2);
		
		String file_index = (String)dataMap.get("file_index");
		String system_version = "";
		if(model.toLowerCase().contains("iphone")){
			map.put("android_ios", "ios");	
		}else{
			map.put("android_ios", "android");
		}
		map.put("file_index", file_index);
		sql = appendSql(map,numType,file_index);
		return sql;
	}
	/**
	 * 插入VideoTest数据
	 */
	public StringBuffer insertVideoTest(Map dataMap,String keyspace,String numType,String detailreport){
		List nameList = new ArrayList();
		List valueList = new ArrayList();
		Map map = new HashMap();
		Set keySet = dataMap.keySet();
		Iterator iter = keySet.iterator();
		String imei = (String)(dataMap.get("imei")==null?"":dataMap.get("imei"));
		String file_path = (String)(dataMap.get("filePath")==null?"":dataMap.get("filePath"));
		String detailreportvalue = (String)(dataMap.get("detailreport")==null?"":dataMap.get("detailreport"));
		map.put("detailreport", detailreport);
		
		map.put("imei",imei);
		map.put("file_path",file_path.replace("\\", "|").replace("//", "|"));
		
		String device_org = keyspace;//终端分组
		
		String longitude = "";
		String latitude = "";
		String province = "";//测试省
		String district = "";//测试district
		String city = "";//测试city
		String time = dataMap.get("time")==null?"":dataMap.get("time").toString();//测试时间
		String model = "";//终端类型
		String logversion = "";//软件版本
		map.put("device_org", device_org);
		map.put("time", time);
		
		String end_time = "";//结束时间
		String test_type = "";//测试类型
		String address = "";//地址
		String buffer_times = "";//缓冲次数
		String delay = "";//时延(ms)
		String test_times = "";//测试时长(ms)

		String location = "";//测试地点
		String gpsStr = "";
		String net_type = "";//网络类型
		String signal_strength = "";//信号强度
		String sinr = "";//SINR
		String lac = "";
		String cid = "";
		String tac = "";
		String pci = "";
		String lac_tac = "";
		String cid_pci = "";
		String net_type1 = "";
		String signal_strength1 = "";
		String sinr1 = "";
		String cell1 = "";
		String net_type2 = "";
		String signal_strength2 = "";
		String sinr2 = "";
		String cell2 = "";
		String wifi_ss = "";//WIFI SIGNAL STRENGTH
		
		boolean flag1 = false;
		boolean flag2 = false;
		
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
		map.put("gps", gpsStr);
		if(gpsStr.contains(" ")){
			String[] gps = transGpsPoint(gpsStr);
			if(gps!=null && gps[0]!=null && gps[1]!=null){
				longitude = gps[0];
				latitude = gps[1];
			}
		}
		double longitudeNum = 0.0;
		double latitudeNum = 0.0;
		if(longitude.isEmpty() && latitude.isEmpty()){
			
		}else{
			double[] em = Earth2Mars.transform(Double.parseDouble(longitude), Double.parseDouble(latitude));
			double[] bf = BdFix.bd_encrypt(em[0], em[1]);
			longitudeNum = bf[0];
			latitudeNum = bf[1];
		}
		map.put("longitude",longitudeNum);
		map.put("latitude",latitudeNum);
		if(longitudeNum == 0.0 || longitudeNum == 0){
			map.put("longitude","");
		}
		if(latitudeNum == 0.0 || latitudeNum == 0){
			map.put("latitude","");
		}
		
		
		//获取GPS位置信息----end
		if(dataMap.containsKey("网络(1)网络制式") || dataMap.containsKey("Network(1) Type") || dataMap.containsKey("Network (1) standard")){
			flag1 = true;
		}
		if(dataMap.containsKey("网络(2)网络制式") || dataMap.containsKey("Network(2) Type") || dataMap.containsKey("Network (1) standard")){
			flag2 = true;
		}
		while (iter.hasNext()) {
			String key = iter.next() + "";
			if (key == null || key.isEmpty()) {
				continue;
			}
			if("平均速率(Kbps)".equals(key) || "平均速率(Mbps)".equals(key) ||"平均速率（Mbps）".equals(key) || "average speed (mbps)".equals(key.toLowerCase()) || "average speed (kbps)".equals(key.toLowerCase()) || "avg speed(kbps)".equals(key.toLowerCase()) || "avg speed(mbps)".equals(key.toLowerCase())){
			}else if("logversion".equals(key.toLowerCase()) || "日志版本".equals(key)){
				logversion = (String) (dataMap.get(key)==null?"":dataMap.get(key));
				map.put("logversion", logversion);
			}else if("信号强度".equals(key) || "Signal Strength".equals(key)){
				signal_strength = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("终端类型".equals(key) || "终端型号".equals(key) || "device type".equals(key.toLowerCase()) || "terminal model".equals(key.toLowerCase()) || "terminal type".equals(key.toLowerCase()) || "terminal mode".equals(key.toLowerCase()) || "device model".equals(key.toLowerCase()) || "terminal models".equals(key.toLowerCase())){
				model = (String) (dataMap.get(key)==null?"":dataMap.get(key));
				map.put("model",model);
			}else if("地址".equals(key) || "address".equals(key.toLowerCase())){
				address = (String) (dataMap.get(key)==null?"":dataMap.get(key));
				map.put("address", address);
			}else if("结束时间".equals(key) || "end time".equals(key.toLowerCase())){
				end_time = (String) (dataMap.get(key)==null?"":dataMap.get(key));
				map.put("end_time", end_time);
			}else if("时延(ms)".equals(key) || "Avg Latency(ms)".equals(key) || "Latency(ms)".equals(key) || "Latency".equals(key) || "Delay".equals(key) || "Delay(ms)".equals(key)){
				delay = (String) (dataMap.get(key)==null?"":dataMap.get(key));
				if(delay.contains(",")){
					try {
						String [] delays = delay.split(",");
						double sum = 0.0;
						for (int i = 0; i < delays.length; i++) {
							double delayNum = Double.parseDouble(delays[i]);
							sum += delayNum;
						}
						double avg_delay = 0.0;
						if(delays.length>0 && sum>0){
							avg_delay = sum/delays.length;
						}else{
							avg_delay = 0;
						}
						Format fm = new DecimalFormat("#.###");
						map.put("avg_delay", fm.format(avg_delay));
					} catch (Exception e) {
						System.out.println("时延有误");
					}
				}
				map.put("delay",delay);	
			}else if("测试类型".equals(key) || "test type".equals(key.toLowerCase())){
				test_type = (String)(dataMap.get(key)==null?"":dataMap.get(key));
				map.put("test_type", test_type);
			}else if("缓冲次数".equals(key) || "buffer counts".equals(key.toLowerCase())){
				buffer_times = (String)(dataMap.get(key)==null?"":dataMap.get(key));
				map.put("buffer_times", buffer_times);
			}else if("测试时长(ms)".equals(key) || "duration(ms)".equals(key.toLowerCase())){
				test_times = (String)(dataMap.get(key)==null?"":dataMap.get(key));
				map.put("test_times", test_times);
			}else if("LAC".equals(key)){
				String lacStr = (String) (dataMap.get(key)==null?"":dataMap.get(key));
				if(lacStr.indexOf("=")!=-1){
					lac = lacStr.substring(lacStr.indexOf("=")+1,lacStr.length());
				}else{
					lac = lacStr;
				}
				
			}else if("网络类型".equals(key) || "网络制式".equals(key) || "network type".equals(key.toLowerCase())){
				net_type = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("SINR".equals(key)){
				sinr = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("网络(1)网络制式".equals(key) || "network(1) type".equals(key.toLowerCase()) || "network (1) standard".equals(key.toLowerCase())){
				net_type1 = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("网络(1)信号强度".equals(key) || "网络(1)信号强度(dBm)".equals(key) || "network(1) signal strength".equals(key.toLowerCase()) || "network(1) signal strength(dbm)".equals(key.toLowerCase())){
				signal_strength1 = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("网络(1)小区信息".equals(key) || "network(1) cell".equals(key.toLowerCase()) || "network(1)".equals(key.toLowerCase())){
				cell1 = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("网络(1)SINR".equals(key) || "网络(1)SINR(dB)".equals(key) || "network(1) sinr".equals(key.toLowerCase()) || "network(1) sinr(db)".equals(key.toLowerCase())){
				sinr1 = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("网络(2)网络制式".equals(key) || "network(2) type".equals(key.toLowerCase()) || "network (2) standard".equals(key.toLowerCase())){
				net_type2 = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("网络(2)信号强度".equals(key) || "网络(2)信号强度(dBm)".equals(key)	 || "network(2) signal strength".equals(key.toLowerCase())){
				signal_strength2 = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("网络(2)小区信息".equals(key) || "network(2) cell".equals(key.toLowerCase()) || "network(2)".equals(key.toLowerCase())){
				cell2 = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("网络(2)SINR".equals(key) || "网络(2)SINR(dB)".equals(key) || "network(2) sinr".equals(key.toLowerCase()) || "network(2) sinr(db)".equals(key.toLowerCase())){
				sinr2 = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("WIFI信号强度".equals(key) || "WIFI信号强度(dBm)".equals(key) || "wifi signal strength".equals(key.trim().toLowerCase()) || "wifi signal strength(dbm)".equals(key.trim().toLowerCase())){
				wifi_ss = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}
		}
		String lac1 = "";
		String cid1 = "";
		if(cell1.indexOf("/")!=-1 || cell1.indexOf("N/A")!=-1){
			String[] cellInfo = cell1.split("/");
			if(cellInfo[0]!=null && !cellInfo[0].equals("")){
				lac1 = cellInfo[0];
			}
			if(cellInfo[1]!=null && !cellInfo[1].equals("")){
				cid1 = cellInfo[1];
			}
		}
		String lac2 = "";
		String cid2 = "";
		if(cell2.indexOf("/")!=-1 || cell2.indexOf("N/A")!=-1){
			String[] cellInfo = cell2.split("/");
			if(cellInfo[0]!=null && !cellInfo[0].equals("")){
				lac2 = cellInfo[0];
			}
			if(cellInfo[1]!=null && !cellInfo[1].equals("")){
				cid2 = cellInfo[1];
			}
		}
		//网络(1)信息存在时 当前数据返回的是网络(1)的信息
		if(flag1 && net_type1.equals(net_type)){
			sinr = sinr1;
			net_type = net_type1;
			signal_strength = signal_strength1;
			lac_tac = lac1;
			cid_pci = cid1;
		}
		//网络(2)信息存在时 当前数据返回的是网络(2)的信息
		if(flag2 && net_type2.equals(net_type)){
			sinr = sinr2;
			net_type = net_type2;
			signal_strength = signal_strength2;
			lac_tac = lac2;
			cid_pci = cid2;
		}
		if(!(flag1 && net_type.equals(net_type1)) && !(flag2 && net_type.equals(net_type2))){
			if(lac1 != null){
				lac_tac = lac1;
			}
			if(cid1 != null){
				cid_pci = cid1;
			}
			if(lac2 != null){
				lac_tac = lac2;
			}
			if(cid2 != null){
				cid_pci = cid2;
			}
		}
		map.put("lac_tac",lac_tac);
		map.put("cid_pci",cid_pci);
		map.put("sinr",sinr);
		if(net_type.equals("WIFI")){
			map.put("signal_strength",wifi_ss);	
		}else{
			map.put("signal_strength",signal_strength);
		}
		map.put("net_type",net_type);
		
		map.put("sinr1", sinr1);
		map.put("net_type1", net_type1);
		map.put("signal_strength1", signal_strength1);
		map.put("lac_tac1", lac1);
		map.put("cid_pci1", cid1);
		
		map.put("sinr2", sinr2);
		map.put("net_type2", net_type2);
		map.put("signal_strength2", signal_strength2);
		map.put("lac_tac2", lac2);
		map.put("cid_pci2", cid2);
		
		String file_index = (String)dataMap.get("file_index");
		String system_version = "";
		if(model.toLowerCase().contains("iphone")){
			map.put("android_ios", "ios");	
		}else{
			map.put("android_ios", "android");
		}
		map.put("file_index", file_index);
		sql = appendSql(map,numType,file_index);
		return sql;
	}
	/**
	 * 插入HTTP数据
	 */
	public StringBuffer insertHTTP(Map dataMap,String keyspace,String numType,String detailreport){
		List nameList = new ArrayList();
		List valueList = new ArrayList();
		Map map = new HashMap();
		Set keySet = dataMap.keySet();
		Iterator iter = keySet.iterator();
		String imei = (String)(dataMap.get("imei")==null?"":dataMap.get("imei"));
		String file_path = (String)(dataMap.get("filePath")==null?"":dataMap.get("filePath"));
		String detailreportvalue = (String)(dataMap.get("detailreport")==null?"":dataMap.get("detailreport"));
		map.put("detailreport", detailreport);
		
		map.put("imei",imei);
		map.put("file_path",file_path.replace("\\", "|").replace("//", "|"));
		
		String device_org = keyspace;//终端分组
		String longitude = "";
		String latitude = "";
		String province = "";//测试省
		String district = "";//测试district
		String city = "";//测试city
		String time = dataMap.get("time")==null?"":dataMap.get("time").toString();//测试时间
		String model = "";//终端类型
		String logversion = "";//软件版本
		map.put("device_org", device_org);
		map.put("time", time);
		
		String url = "";//文件地址
		String testfileurl = "";//测试文件地址
		String resource_size = "";//文件大小
		String duration = "";//下载/上传时长
		String avg_rate = "";//平均速率
		String max_rate = "";//最大速率
		String avg_latency = "";//平均时长
		String file_type = "";//测试类型
		String success_rate = "";//成功率

		String location = "";//测试地点
		String gpsStr = "";
		String net_type = "";//网络类型
		String signal_strength = "";//信号强度
		String sinr = "";//SINR
		String lac = "";
		String cid = "";
		String tac = "";
		String pci = "";
		String lac_tac = "";
		String cid_pci = "";
		String net_type1 = "";
		String signal_strength1 = "";
		String sinr1 = "";
		String cell1 = "";
		String net_type2 = "";
		String signal_strength2 = "";
		String sinr2 = "";
		String cell2 = "";
		String wifi_ss = "";//WIFI SIGNAL STRENGTH
		
		boolean flag1 = false;
		boolean flag2 = false;
		
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
		map.put("gps", gpsStr);
		if(gpsStr.contains(" ")){
			String[] gps = transGpsPoint(gpsStr);
			if(gps!=null && gps[0]!=null && gps[1]!=null){
				longitude = gps[0];
				latitude = gps[1];
			}
		}
		double longitudeNum = 0.0;
		double latitudeNum = 0.0;
		if(longitude.isEmpty() && latitude.isEmpty()){
			
		}else{
			double[] em = Earth2Mars.transform(Double.parseDouble(longitude), Double.parseDouble(latitude));
			double[] bf = BdFix.bd_encrypt(em[0], em[1]);
			longitudeNum = bf[0];
			latitudeNum = bf[1];
		}
		map.put("longitude",longitudeNum);
		map.put("latitude",latitudeNum);
		if(longitudeNum == 0.0 || longitudeNum == 0){
			map.put("longitude","");
		}
		if(latitudeNum == 0.0 || latitudeNum == 0){
			map.put("latitude","");
		}
		
		
		//获取GPS位置信息----end
		if(dataMap.containsKey("网络(1)网络制式") || dataMap.containsKey("Network(1) Type") || dataMap.containsKey("Network (1) standard")){
			flag1 = true;
		}
		if(dataMap.containsKey("网络(2)网络制式") || dataMap.containsKey("Network(2) Type") || dataMap.containsKey("Network (1) standard")){
			flag2 = true;
		}
		while (iter.hasNext()) {
			String key = iter.next() + "";
			if (key == null || key.isEmpty()) {
				continue;
			}
			if("平均速率(Kbps)".equals(key) || "平均速率(Mbps)".equals(key) ||"平均速率（Mbps）".equals(key) || "average speed (mbps)".equals(key.toLowerCase()) || "average speed (kbps)".equals(key.toLowerCase()) || "avg speed(kbps)".equals(key.toLowerCase()) || "avg speed(mbps)".equals(key.toLowerCase())){
				avg_rate = (String) (dataMap.get(key)==null?"":dataMap.get(key));
				map.put("avg_rate",avg_rate);
			}else if("Max Speed(Kbps)".equals(key) || "Max Speed(Mbps)".equals(key) || "最大速率(Mbps)".equals(key) || "最大速率(Kbps)".equals(key) || "最大速率（Mbps）".equals(key) || "最大速率（Kbps）".equals(key)){
				max_rate = (String) (dataMap.get(key)==null?"":dataMap.get(key));
				map.put("max_rate", max_rate);
			}else if("平均时长(ms)".equals(key) || "平均时延".equals(key) || "平均时延(ms)".equals(key) || "Avg Latency(ms)".equals(key) || "Avg Time(ms)".equals(key) || "Latency".equals(key) || "Delay".equals(key) || "Delay(ms)".equals(key)){
				avg_latency = (String)(dataMap.get(key)==null?"":dataMap.get(key));
				map.put("avg_latency",avg_latency);
			}else if("logversion".equals(key.toLowerCase()) || "日志版本".equals(key)){
				logversion = (String) (dataMap.get(key)==null?"":dataMap.get(key));
				map.put("logversion", logversion);
			}else if("信号强度".equals(key) || "Signal Strength".equals(key)){
				signal_strength = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("终端类型".equals(key) || "终端型号".equals(key) || "device type".equals(key.toLowerCase()) || "terminal model".equals(key.toLowerCase()) || "terminal type".equals(key.toLowerCase()) || "terminal mode".equals(key.toLowerCase()) || "device model".equals(key.toLowerCase()) || "terminal models".equals(key.toLowerCase())){
				model = (String) (dataMap.get(key)==null?"":dataMap.get(key));
				map.put("model",model);
			}else if("类型".equals(key) || "Type".equals(key) || "Test Type".equals(key) || "测试类型".equals(key)){
				file_type = (String) (dataMap.get(key)==null?"":dataMap.get(key));
				map.put("file_type", file_type);
			}else if("文件".equals(key) || "File".equals(key)){
				url = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("测试文件".equals(key) || "Test File".equals(key)){
				testfileurl = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("文件大小".equals(key) || "File Size".equals(key) || "File Size(MB)".equals(key) || "文件大小(MB)".equals(key)){
				resource_size = (String) (dataMap.get(key)==null?"":dataMap.get(key));
//				if(resource_size.equals("") || resource_size.equals("--") || !resource_size.contains(".")){
					map.put("resource_size",resource_size);
//				}else
//				{
//					map.put("resource_size",resource_size.substring(resource_size.lastIndexOf("/")+1, resource_size.lastIndexOf(".")));
//				}
			}else if("成功率".equals(key) || "success rate".equals(key.toLowerCase())){
				success_rate = (String)(dataMap.get(key)==null?"":dataMap.get(key));
				map.put("success_rate", success_rate);
			}else if("时长(s)".equals(key) || "时长（s）".equals(key) || "时长(ms)".equals(key) || "duration".equals(key.toLowerCase()) || "duration(ms)".equals(key.toLowerCase())){
				duration = (String) (dataMap.get(key)==null?"":dataMap.get(key));
				map.put("duration",duration);
			}else if("网络类型".equals(key) || "网络制式".equals(key) || "network type".equals(key.toLowerCase())){
				net_type = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("SINR".equals(key)){
				sinr = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("网络(1)网络制式".equals(key) || "network(1) type".equals(key.toLowerCase()) || "network (1) standard".equals(key.toLowerCase())){
				net_type1 = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("网络(1)信号强度".equals(key) || "网络(1)信号强度(dBm)".equals(key) || "network(1) signal strength".equals(key.toLowerCase()) || "network(1) signal strength(dbm)".equals(key.toLowerCase())){
				signal_strength1 = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("网络(1)小区信息".equals(key) || "network(1) cell".equals(key.toLowerCase()) || "network(1)".equals(key.toLowerCase())){
				cell1 = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("网络(1)SINR".equals(key) || "网络(1)SINR(dB)".equals(key) || "network(1) sinr".equals(key.toLowerCase()) || "network(1) sinr(db)".equals(key.toLowerCase())){
				sinr1 = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("网络(2)网络制式".equals(key) || "network(2) type".equals(key.toLowerCase()) || "network (2) standard".equals(key.toLowerCase())){
				net_type2 = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("网络(2)信号强度".equals(key) || "网络(2)信号强度(dBm)".equals(key)	 || "network(2) signal strength".equals(key.toLowerCase())){
				signal_strength2 = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("网络(2)小区信息".equals(key) || "network(2) cell".equals(key.toLowerCase()) || "network(2)".equals(key.toLowerCase())){
				cell2 = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("网络(2)SINR".equals(key) || "网络(2)SINR(dB)".equals(key) || "network(2) sinr".equals(key.toLowerCase()) || "network(2) sinr(db)".equals(key.toLowerCase())){
				sinr2 = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("WIFI信号强度".equals(key) || "WIFI信号强度(dBm)".equals(key) || "wifi signal strength".equals(key.trim().toLowerCase()) || "wifi signal strength(dbm)".equals(key.trim().toLowerCase())){
				wifi_ss = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}
		}
		
		//处理上传类型时url取文件中的值
		if(file_type.equals("上传") || file_type.toLowerCase().equals("upload")){
			map.put("url", url);
		}else{
			if(!testfileurl.isEmpty()){
				map.put("url", url);
			}
			if(!url.isEmpty()){
				map.put("url", url);
			}
		}
		String lac1 = "";
		String cid1 = "";
		if(cell1.indexOf("/")!=-1 || cell1.indexOf("N/A")!=-1){
			String[] cellInfo = cell1.split("/");
			if(cellInfo[0]!=null && !cellInfo[0].equals("")){
				lac1 = cellInfo[0];
			}
			if(cellInfo[1]!=null && !cellInfo[1].equals("")){
				cid1 = cellInfo[1];
			}
		}
		String lac2 = "";
		String cid2 = "";
		if(cell2.indexOf("/")!=-1 || cell2.indexOf("N/A")!=-1){
			String[] cellInfo = cell2.split("/");
			if(cellInfo[0]!=null && !cellInfo[0].equals("")){
				lac2 = cellInfo[0];
			}
			if(cellInfo[1]!=null && !cellInfo[1].equals("")){
				cid2 = cellInfo[1];
			}
		}
		//网络(1)信息存在时 当前数据返回的是网络(1)的信息
		if(flag1 && net_type1.equals(net_type)){
			sinr = sinr1;
			net_type = net_type1;
			signal_strength = signal_strength1;
			lac_tac = lac1;
			cid_pci = cid1;
		}
		//网络(2)信息存在时 当前数据返回的是网络(2)的信息
		if(flag2 && net_type2.equals(net_type)){
			sinr = sinr2;
			net_type = net_type2;
			signal_strength = signal_strength2;
			lac_tac = lac2;
			cid_pci = cid2;
		}
		if(!(flag1 && net_type.equals(net_type1)) && !(flag2 && net_type.equals(net_type2))){
			if(lac1 != null){
				lac_tac = lac1;
			}
			if(cid1 != null){
				cid_pci = cid1;
			}
			if(lac2 != null){
				lac_tac = lac2;
			}
			if(cid2 != null){
				cid_pci = cid2;
			}
		}
		map.put("lac_tac",lac_tac);
		map.put("cid_pci",cid_pci);
		map.put("sinr",sinr);
		if(net_type.equals("WIFI")){
			map.put("signal_strength",wifi_ss);	
		}else{
			map.put("signal_strength",signal_strength);
		}
		map.put("net_type",net_type);
		
		map.put("sinr1", sinr1);
		map.put("net_type1", net_type1);
		map.put("signal_strength1", signal_strength1);
		map.put("lac_tac1", lac1);
		map.put("cid_pci1", cid1);
		
		map.put("sinr2", sinr2);
		map.put("net_type2", net_type2);
		map.put("signal_strength2", signal_strength2);
		map.put("lac_tac2", lac2);
		map.put("cid_pci2", cid2);
		
		String file_index = (String)dataMap.get("file_index");
		String system_version = "";
		if(model.toLowerCase().contains("iphone")){
			map.put("android_ios", "ios");	
		}else{
			map.put("android_ios", "android");
		}
		map.put("file_index", file_index);
		sql = appendSql(map,numType,file_index);
		return sql;
	}
	/**
	 * 插入PING数据
	 */
	public StringBuffer insertPINGManyToOne(Map allMap,String keyspace,String numType,String detailreport){
		System.out.println("开始准备进行解析"+(allMap.size()-1)+"份"+"PING数据************："+new Date().toLocaleString() +"   毫秒："+new Date().getTime());
		Map map = new HashMap();
		String file_index = "";
		for (int i = 0; i < allMap.size()-1; i++) {
			Map dataMap = (Map) allMap.get(i);
			file_index = (String)dataMap.get("file_index");
			Set keySet = dataMap.keySet();
			Iterator iter = keySet.iterator();
			String imei = (String)(dataMap.get("imei")==null?"":dataMap.get("imei"));
			String file_path = (String)(dataMap.get("filePath")==null?"":dataMap.get("filePath"));
			String detailreportvalue = (String)(dataMap.get("detailreport")==null?"":dataMap.get("detailreport"));
			String time = dataMap.get("time")==null?"":dataMap.get("time").toString();//测试时间
			String device_org = keyspace;//终端分组
			String longitude = "";
			String latitude = "";
			String province = "";//测试省
			String district = "";//测试district
			String city = "";//测试city
			String model = "";//终端类型
			String logversion = "";//软件版本
			String packet_loss_rate= "";
			
			map.put("detailreport", detailreport);
			map.put("imei",imei);
			map.put("file_path",file_path.replace("\\", "|").replace("//", "|"));
			map.put("device_org", device_org);
			map.put("time", time);
			
			
			String domain_address = "";
			String max_rtt = "";
			String min_rtt = "";
			String avg_rtt = "";
			String times = "";
			String success_rate = "";//成功率
			
			String location = "";//测试地点
			String gpsStr = "";
			String net_type = "";//网络类型
			String signal_strength = "";//信号强度
			String sinr = "";//SINR
			String lac = "";
			String cid = "";
			String tac = "";
			String pci = "";
			String lac_tac = "";
			String cid_pci = "";
			String net_type1 = "";
			String signal_strength1 = "";
			String sinr1 = "";
			String cell1 = "";
			String net_type2 = "";
			String signal_strength2 = "";
			String sinr2 = "";
			String cell2 = "";
			String wifi_ss = "";//WIFI SIGNAL STRENGTH
			
			boolean flag1 = false;
			boolean flag2 = false;
			
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
			map.put("gps", gpsStr);
			if(gpsStr.contains(" ")){
				String[] gps = transGpsPoint(gpsStr);
				if(gps!=null && gps[0]!=null && gps[1]!=null){
					longitude = gps[0];
					latitude = gps[1];
				}
			}
			double longitudeNum = 0.0;
			double latitudeNum = 0.0;
			if(longitude.isEmpty() && latitude.isEmpty()){
				
			}else{
				double[] em = Earth2Mars.transform(Double.parseDouble(longitude), Double.parseDouble(latitude));
				double[] bf = BdFix.bd_encrypt(em[0], em[1]);
				longitudeNum = bf[0];
				latitudeNum = bf[1];
			}
			map.put("longitude",longitudeNum);
			map.put("latitude",latitudeNum);
			if(longitudeNum == 0.0 || longitudeNum == 0){
				map.put("longitude","");
			}
			if(latitudeNum == 0.0 || latitudeNum == 0){
				map.put("latitude","");
			}
			
			String success_rate_filter = "";
			if(dataMap.containsKey("成功率")){
				success_rate_filter = dataMap.get("成功率")==null?"":dataMap.get("成功率").toString();
				if(success_rate_filter.equals("0") || success_rate_filter.equals("0%")){
					continue;
				}
			}
			if(dataMap.containsKey("success rate")){
				success_rate_filter = dataMap.get("success rate")==null?"":dataMap.get("success rate").toString();
				if(success_rate_filter.equals("0") || success_rate_filter.equals("0%")){
					continue;
				}
			}
			if(dataMap.containsKey("Success Rate")){
				success_rate_filter = dataMap.get("Success Rate")==null?"":dataMap.get("Success Rate").toString();
				if(success_rate_filter.equals("0") || success_rate_filter.equals("0%")){
					continue;
				}
			}
			//获取GPS位置信息----end
			if(dataMap.containsKey("网络(1)网络制式") || dataMap.containsKey("Network(1) Type") || dataMap.containsKey("Network (1) standard")){
				flag1 = true;
			}
			if(dataMap.containsKey("网络(2)网络制式") || dataMap.containsKey("Network(2) Type") || dataMap.containsKey("Network (1) standard")){
				flag2 = true;
			}
			while (iter.hasNext()) {
				String key = iter.next() + "";
				if (key == null || key.isEmpty()) {
					continue;
				}
				
				if("平均速率(Kbps)".equals(key) || "平均速率(Mbps)".equals(key) ||"平均速率（Mbps）".equals(key) || "average speed (mbps)".equals(key.toLowerCase()) || "average speed (kbps)".equals(key.toLowerCase()) || "avg speed(kbps)".equals(key.toLowerCase()) || "avg speed(mbps)".equals(key.toLowerCase())){
				}else if("logversion".equals(key.toLowerCase()) || "日志版本".equals(key)){
					logversion = (String) (dataMap.get(key)==null?"":dataMap.get(key));
					map.put("logversion", logversion);
				}else if("信号强度".equals(key) || "Signal Strength".equals(key)){
					signal_strength = (String) (dataMap.get(key)==null?"":dataMap.get(key));
				}else if("终端类型".equals(key) || "终端型号".equals(key) || "device type".equals(key.toLowerCase()) || "terminal model".equals(key.toLowerCase()) || "terminal type".equals(key.toLowerCase()) || "terminal mode".equals(key.toLowerCase()) || "device model".equals(key.toLowerCase()) || "terminal models".equals(key.toLowerCase())){
					model = (String)(dataMap.get(key)==null?"":dataMap.get(key));
					map.put("model",model);
				}else if("地址".equals(key) || "Address".equals(key) || "address".equals(key)){
					domain_address = (String)(dataMap.get(key)==null?"":dataMap.get(key));
					if(map.containsKey("domain_address") && !map.get("domain_address").toString().isEmpty()){
						map.put("domain_address",map.get("domain_address").toString()+","+domain_address);						
					}else {
						map.put("domain_address","Overall,"+domain_address);
					}
				}else if("平均时延(ms)".equals(key) || "平均时延".equals(key) || "average delay(ms)".equals(key.toLowerCase()) || "avg latency(ms)".equals(key.toLowerCase()) || "avg time(ms)".equals(key.toLowerCase()) || "avg latency".equals(key.toLowerCase()) || "delay".equals(key.toLowerCase()) || "delay(ms)".equals(key.toLowerCase())){
					avg_rtt = (String)(dataMap.get(key)==null?"":dataMap.get(key));
					if(map.containsKey("avg_rtt") && !map.get("avg_rtt").toString().isEmpty()){
						String avgStr = map.get("avg_rtt").toString();
						String []str = avgStr.split(",");
						double num = Double.parseDouble(avg_rtt); 
						double avg = Double.parseDouble(str[0]);
						str[0] = ""+((avg*(str.length-1)+num)/str.length);
						String avg_rtt_str = "";
						for (int j = 0; j < str.length; j++) {
							avg_rtt_str += str[j]+",";
						}
						avg_rtt_str += avg_rtt;
						map.put("avg_rtt",avg_rtt_str);
					}else{
						if(avg_rtt.isEmpty()){
							map.put("avg_rtt",0+","+avg_rtt);							
						}else{
							map.put("avg_rtt",avg_rtt+","+avg_rtt);
						}
					}
				}else if("the minimum delay(ms)".equals(key.trim().toLowerCase()) ||"最大时延(ms)".equals(key)||"最大时延".equals(key) || "max latency(ms)".equals(key.toLowerCase()) || "max latency".equals(key.toLowerCase())){
					max_rtt = (String)(dataMap.get(key)==null?"":dataMap.get(key));
					if(map.containsKey("max_rtt") && !map.get("max_rtt").toString().isEmpty()){
						String avgStr = map.get("max_rtt").toString();
						String []str = avgStr.split(",");
						double num = Double.parseDouble(max_rtt); 
						double avg = Double.parseDouble(str[0]);
						str[0] = ""+((avg*(str.length-1)+num)/str.length);
						String max_rtt_str = "";
						for (int j = 0; j < str.length; j++) {
							max_rtt_str += str[j]+",";
						}
						max_rtt_str += max_rtt;
						map.put("max_rtt",max_rtt_str);
					}else{
						if(max_rtt.isEmpty()){
							map.put("max_rtt",0+","+max_rtt);							
						}else{
							map.put("max_rtt",max_rtt+","+max_rtt);
						}
					}
				}else if("the maximum delay(ms)".equals(key.trim().toLowerCase()) || "最小时延(ms)".equals(key)||"最小时延".equals(key) || "min latency(ms)".equals(key.toLowerCase()) || "min latency".equals(key.toCharArray())){
					min_rtt = (String)(dataMap.get(key)==null?"":dataMap.get(key));
					if(map.containsKey("min_rtt") && !map.get("min_rtt").toString().isEmpty()){
						String avgStr = map.get("min_rtt").toString();
						String []str = avgStr.split(",");
						double num = Double.parseDouble(min_rtt); 
						double avg = Double.parseDouble(str[0]);
						str[0] = ""+((avg*(str.length-1)+num)/str.length);
						String min_rtt_str = "";
						for (int j = 0; j < str.length; j++) {
							min_rtt_str += str[j]+",";
						}
						min_rtt_str += min_rtt;
						map.put("min_rtt",min_rtt_str);
					}else{
						if(min_rtt.isEmpty()){
							map.put("min_rtt",0+","+min_rtt);							
						}else{
							map.put("min_rtt",min_rtt+","+min_rtt);
						}
					}
				}else if("测试次数".equals(key)||"重复次数".equals(key) || "repetition".equals(key.toLowerCase()) || "number of repetitions".equals(key.toLowerCase()) || "Test Counts".equals(key)){
					times = (String)(dataMap.get(key)==null?"":dataMap.get(key));
					map.put("times",times);
				}else if("packet_loss_rate".equals(key.toLowerCase()) || "丢包率".equals(key)){
					packet_loss_rate = (String)(dataMap.get(key)==null?"":dataMap.get(key));
					map.put("packet_loss_rate",packet_loss_rate);
				}else if("成功率".equals(key) || "success rate".equals(key.toLowerCase())){
					success_rate = (String)(dataMap.get(key)==null?"":dataMap.get(key));
					if(success_rate.contains("%")){
						success_rate = success_rate.replaceAll("%", "");
					}
					if(map.containsKey("success_rate") && !map.get("success_rate").toString().isEmpty()){
						String avgStr = map.get("success_rate").toString();
						String []str = avgStr.split(",");
						double num = Double.parseDouble(success_rate);
						double avg = Double.parseDouble(str[0]);
						str[0] = ""+((avg*(str.length-1)+num)/str.length);
						String success_rate_str = "";
						for (int j = 0; j < str.length; j++) {
							success_rate_str += str[j]+",";
						}
						success_rate_str += success_rate;
						map.put("success_rate",success_rate_str);
					}else{
						if(success_rate.isEmpty()){
							map.put("success_rate",0+","+success_rate);							
						}else{
							map.put("success_rate",success_rate+","+success_rate);
						}
					}
				}else if("网络类型".equals(key) || "网络制式".equals(key) || "network type".equals(key.toLowerCase())){
					net_type = (String) (dataMap.get(key)==null?"":dataMap.get(key));
				}else if("SINR".equals(key)){
					sinr = (String) (dataMap.get(key)==null?"":dataMap.get(key));
				}else if("网络(1)网络制式".equals(key) || "network(1) type".equals(key.toLowerCase()) || "network (1) standard".equals(key.toLowerCase())){
					net_type1 = (String) (dataMap.get(key)==null?"":dataMap.get(key));
				}else if("网络(1)信号强度".equals(key) || "网络(1)信号强度(dBm)".equals(key) || "network(1) signal strength".equals(key.toLowerCase()) || "network(1) signal strength(dbm)".equals(key.toLowerCase())){
					signal_strength1 = (String) (dataMap.get(key)==null?"":dataMap.get(key));
				}else if("网络(1)小区信息".equals(key) || "network(1) cell".equals(key.toLowerCase()) || "network(1)".equals(key.toLowerCase())){
					cell1 = (String) (dataMap.get(key)==null?"":dataMap.get(key));
				}else if("网络(1)SINR".equals(key) || "网络(1)SINR(dB)".equals(key) || "network(1) sinr".equals(key.toLowerCase()) || "network(1) sinr(db)".equals(key.toLowerCase())){
					sinr1 = (String) (dataMap.get(key)==null?"":dataMap.get(key));
				}else if("网络(2)网络制式".equals(key) || "network(2) type".equals(key.toLowerCase()) || "network (2) standard".equals(key.toLowerCase())){
					net_type2 = (String) (dataMap.get(key)==null?"":dataMap.get(key));
				}else if("网络(2)信号强度".equals(key) || "网络(2)信号强度(dBm)".equals(key)	 || "network(2) signal strength".equals(key.toLowerCase())){
					signal_strength2 = (String) (dataMap.get(key)==null?"":dataMap.get(key));
				}else if("网络(2)小区信息".equals(key) || "network(2) cell".equals(key.toLowerCase()) || "network(2)".equals(key.toLowerCase())){
					cell2 = (String) (dataMap.get(key)==null?"":dataMap.get(key));
				}else if("网络(2)SINR".equals(key) || "网络(2)SINR(dB)".equals(key) || "network(2) sinr".equals(key.toLowerCase()) || "network(2) sinr(db)".equals(key.toLowerCase())){
					sinr2 = (String) (dataMap.get(key)==null?"":dataMap.get(key));
				}else if("WIFI信号强度".equals(key) || "WIFI信号强度(dBm)".equals(key) || "wifi signal strength".equals(key.trim().toLowerCase()) || "wifi signal strength(dbm)".equals(key.trim().toLowerCase())){
					wifi_ss = (String) (dataMap.get(key)==null?"":dataMap.get(key));
				}
			}
			String lac1 = "";
			String cid1 = "";
			if(cell1.indexOf("/")!=-1 || cell1.indexOf("N/A")!=-1){
				String[] cellInfo = cell1.split("/");
				if(cellInfo[0]!=null && !cellInfo[0].equals("")){
					lac1 = cellInfo[0];
				}
				if(cellInfo[1]!=null && !cellInfo[1].equals("")){
					cid1 = cellInfo[1];
				}
			}
			String lac2 = "";
			String cid2 = "";
			if(cell2.indexOf("/")!=-1 || cell2.indexOf("N/A")!=-1){
				String[] cellInfo = cell2.split("/");
				if(cellInfo[0]!=null && !cellInfo[0].equals("")){
					lac2 = cellInfo[0];
				}
				if(cellInfo[1]!=null && !cellInfo[1].equals("")){
					cid2 = cellInfo[1];
				}
			}
			//网络(1)信息存在时 当前数据返回的是网络(1)的信息
			if(flag1 && net_type1.equals(net_type)){
				sinr = sinr1;
				net_type = net_type1;
				signal_strength = signal_strength1;
				lac_tac = lac1;
				cid_pci = cid1;
			}
			//网络(2)信息存在时 当前数据返回的是网络(2)的信息
			if(flag2 && net_type2.equals(net_type)){
				sinr = sinr2;
				net_type = net_type2;
				signal_strength = signal_strength2;
				lac_tac = lac2;
				cid_pci = cid2;
			}
			if(!(flag1 && net_type.equals(net_type1)) && !(flag2 && net_type.equals(net_type2))){
				if(lac1 != null){
					lac_tac = lac1;
				}
				if(cid1 != null){
					cid_pci = cid1;
				}
				if(lac2 != null){
					lac_tac = lac2;
				}
				if(cid2 != null){
					cid_pci = cid2;
				}
			}
			map.put("lac_tac",lac_tac);
			map.put("cid_pci",cid_pci);
			map.put("sinr",sinr);
			if(net_type.equals("WIFI")){
				map.put("signal_strength",wifi_ss);	
			}else{
				map.put("signal_strength",signal_strength);
			}
			
			map.put("net_type",net_type);
			
			map.put("sinr1", sinr1);
			map.put("net_type1", net_type1);
			map.put("signal_strength1", signal_strength1);
			map.put("lac_tac1", lac1);
			map.put("cid_pci1", cid1);
			
			map.put("sinr2", sinr2);
			map.put("net_type2", net_type2);
			map.put("signal_strength2", signal_strength2);
			map.put("lac_tac2", lac2);
			map.put("cid_pci2", cid2);
			
			String system_version = "";
			if(model.toLowerCase().contains("iphone")){
				map.put("android_ios", "ios");	
			}else{
				map.put("android_ios", "android");
			}
			map.put("file_index", file_index);
		}
		System.out.println("解析完毕PING数据 开始组装sql************："+new Date().toLocaleString() +"   毫秒："+new Date().getTime());
		sql = appendSql(map,numType,file_index);
		return sql;
	}
	/**
	 * 插入PING数据
	 */
	public StringBuffer insertPING(Map dataMap,String keyspace,String numType,String detailreport){
		List nameList = new ArrayList();
		List valueList = new ArrayList();
		Map map = new HashMap();
		Set keySet = dataMap.keySet();
		Iterator iter = keySet.iterator();
		String imei = (String)(dataMap.get("imei")==null?"":dataMap.get("imei"));
		String file_path = (String)(dataMap.get("filePath")==null?"":dataMap.get("filePath"));
		String detailreportvalue = (String)(dataMap.get("detailreport")==null?"":dataMap.get("detailreport"));
		map.put("detailreport", detailreport);
		map.put("imei",imei);
		map.put("file_path",file_path.replace("\\", "|").replace("//", "|"));

		String device_org = keyspace;//终端分组
		String longitude = "";
		String latitude = "";
		String province = "";//测试省
		String district = "";//测试district
		String city = "";//测试city
		String time = dataMap.get("time")==null?"":dataMap.get("time").toString();//测试时间
		String model = "";//终端类型
		String logversion = "";//软件版本
		map.put("device_org", device_org);
		map.put("time", time);
		
		String domain_address = "";
		String max_rtt = "";
		String min_rtt = "";
		String avg_rtt = "";
		String times = "";
		String success_rate = "";//成功率
		
		String location = "";//测试地点
		String gpsStr = "";
		String net_type = "";//网络类型
		String signal_strength = "";//信号强度
		String sinr = "";//SINR
		String lac = "";
		String cid = "";
		String tac = "";
		String pci = "";
		String lac_tac = "";
		String cid_pci = "";
		String net_type1 = "";
		String signal_strength1 = "";
		String sinr1 = "";
		String cell1 = "";
		String net_type2 = "";
		String signal_strength2 = "";
		String sinr2 = "";
		String cell2 = "";
		String wifi_ss = "";//WIFI SIGNAL STRENGTH
		
		boolean flag1 = false;
		boolean flag2 = false;
		
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
		map.put("gps", gpsStr);
		if(gpsStr.contains(" ")){
			String[] gps = transGpsPoint(gpsStr);
			if(gps!=null && gps[0]!=null && gps[1]!=null){
				longitude = gps[0];
				latitude = gps[1];
			}
		}
		double longitudeNum = 0.0;
		double latitudeNum = 0.0;
		if(longitude.isEmpty() && latitude.isEmpty()){
			
		}else{
			double[] em = Earth2Mars.transform(Double.parseDouble(longitude), Double.parseDouble(latitude));
			double[] bf = BdFix.bd_encrypt(em[0], em[1]);
			longitudeNum = bf[0];
			latitudeNum = bf[1];
		}
		map.put("longitude",longitudeNum);
		map.put("latitude",latitudeNum);
		if(longitudeNum == 0.0 || longitudeNum == 0){
			map.put("longitude","");
		}
		if(latitudeNum == 0.0 || latitudeNum == 0){
			map.put("latitude","");
		}
		
		
		//获取GPS位置信息----end
		if(dataMap.containsKey("网络(1)网络制式") || dataMap.containsKey("Network(1) Type") || dataMap.containsKey("Network (1) standard")){
			flag1 = true;
		}
		if(dataMap.containsKey("网络(2)网络制式") || dataMap.containsKey("Network(2) Type") || dataMap.containsKey("Network (1) standard")){
			flag2 = true;
		}
		while (iter.hasNext()) {
			String key = iter.next() + "";
			if (key == null || key.isEmpty()) {
				continue;
			}
			
			if("平均速率(Kbps)".equals(key) || "平均速率(Mbps)".equals(key) ||"平均速率（Mbps）".equals(key) || "average speed (mbps)".equals(key.toLowerCase()) || "average speed (kbps)".equals(key.toLowerCase()) || "avg speed(kbps)".equals(key.toLowerCase()) || "avg speed(mbps)".equals(key.toLowerCase())){
			}else if("logversion".equals(key.toLowerCase()) || "日志版本".equals(key)){
				logversion = (String) (dataMap.get(key)==null?"":dataMap.get(key));
				map.put("logversion", logversion);
			}else if("信号强度".equals(key) || "Signal Strength".equals(key)){
				signal_strength = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("终端类型".equals(key) || "终端型号".equals(key) || "device type".equals(key.toLowerCase()) || "terminal model".equals(key.toLowerCase()) || "terminal type".equals(key.toLowerCase()) || "terminal mode".equals(key.toLowerCase()) || "device model".equals(key.toLowerCase()) || "terminal models".equals(key.toLowerCase())){
				model = (String)(dataMap.get(key)==null?"":dataMap.get(key));
				map.put("model",model);
			}else if("地址".equals(key) || "Address".equals(key) || "address".equals(key)){
				domain_address = (String)(dataMap.get(key)==null?"":dataMap.get(key));
				map.put("domain_address",domain_address);
			}else if("平均时延(ms)".equals(key) || "平均时延".equals(key) || "average delay(ms)".equals(key.toLowerCase()) || "avg latency(ms)".equals(key.toLowerCase()) || "avg time(ms)".equals(key.toLowerCase()) || "avg latency".equals(key.toLowerCase()) || "delay".equals(key.toLowerCase()) || "delay(ms)".equals(key.toLowerCase())){
				avg_rtt = (String)(dataMap.get(key)==null?"":dataMap.get(key));
				map.put("avg_rtt",avg_rtt);
			}else if("the minimum delay(ms)".equals(key.trim().toLowerCase()) ||"最大时延(ms)".equals(key)||"最大时延".equals(key) || "max latency(ms)".equals(key.toLowerCase()) || "max latency".equals(key.toLowerCase())){
				max_rtt = (String)(dataMap.get(key)==null?"":dataMap.get(key));
				map.put("max_rtt",max_rtt);
			}else if("the maximum delay(ms)".equals(key.trim().toLowerCase()) || "最小时延(ms)".equals(key)||"最小时延".equals(key) || "min latency(ms)".equals(key.toLowerCase()) || "min latency".equals(key.toCharArray())){
				min_rtt = (String)(dataMap.get(key)==null?"":dataMap.get(key));
				map.put("min_rtt",min_rtt);
			}else if("测试次数".equals(key)||"重复次数".equals(key) || "repetition".equals(key.toLowerCase()) || "number of repetitions".equals(key.toLowerCase()) || "Test Counts".equals(key)){
				times = (String)(dataMap.get(key)==null?"":dataMap.get(key));
				map.put("times",times);
			}else if("成功率".equals(key) || "success rate".equals(key.toLowerCase())){
				success_rate = (String)(dataMap.get(key)==null?"":dataMap.get(key));
				map.put("success_rate", success_rate);
			}else if("网络类型".equals(key) || "网络制式".equals(key) || "network type".equals(key.toLowerCase())){
				net_type = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("SINR".equals(key)){
				sinr = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("网络(1)网络制式".equals(key) || "network(1) type".equals(key.toLowerCase()) || "network (1) standard".equals(key.toLowerCase())){
				net_type1 = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("网络(1)信号强度".equals(key) || "网络(1)信号强度(dBm)".equals(key) || "network(1) signal strength".equals(key.toLowerCase()) || "network(1) signal strength(dbm)".equals(key.toLowerCase())){
				signal_strength1 = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("网络(1)小区信息".equals(key) || "network(1) cell".equals(key.toLowerCase()) || "network(1)".equals(key.toLowerCase())){
				cell1 = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("网络(1)SINR".equals(key) || "网络(1)SINR(dB)".equals(key) || "network(1) sinr".equals(key.toLowerCase()) || "network(1) sinr(db)".equals(key.toLowerCase())){
				sinr1 = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("网络(2)网络制式".equals(key) || "network(2) type".equals(key.toLowerCase()) || "network (2) standard".equals(key.toLowerCase())){
				net_type2 = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("网络(2)信号强度".equals(key) || "网络(2)信号强度(dBm)".equals(key)	 || "network(2) signal strength".equals(key.toLowerCase())){
				signal_strength2 = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("网络(2)小区信息".equals(key) || "network(2) cell".equals(key.toLowerCase()) || "network(2)".equals(key.toLowerCase())){
				cell2 = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("网络(2)SINR".equals(key) || "网络(2)SINR(dB)".equals(key) || "network(2) sinr".equals(key.toLowerCase()) || "network(2) sinr(db)".equals(key.toLowerCase())){
				sinr2 = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("WIFI信号强度".equals(key) || "WIFI信号强度(dBm)".equals(key) || "wifi signal strength".equals(key.trim().toLowerCase()) || "wifi signal strength(dbm)".equals(key.trim().toLowerCase())){
				wifi_ss = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}
		}
		String lac1 = "";
		String cid1 = "";
		if(cell1.indexOf("/")!=-1 || cell1.indexOf("N/A")!=-1){
			String[] cellInfo = cell1.split("/");
			if(cellInfo[0]!=null && !cellInfo[0].equals("")){
				lac1 = cellInfo[0];
			}
			if(cellInfo[1]!=null && !cellInfo[1].equals("")){
				cid1 = cellInfo[1];
			}
		}
		String lac2 = "";
		String cid2 = "";
		if(cell2.indexOf("/")!=-1 || cell2.indexOf("N/A")!=-1){
			String[] cellInfo = cell2.split("/");
			if(cellInfo[0]!=null && !cellInfo[0].equals("")){
				lac2 = cellInfo[0];
			}
			if(cellInfo[1]!=null && !cellInfo[1].equals("")){
				cid2 = cellInfo[1];
			}
		}
		//网络(1)信息存在时 当前数据返回的是网络(1)的信息
		if(flag1 && net_type1.equals(net_type)){
			sinr = sinr1;
			net_type = net_type1;
			signal_strength = signal_strength1;
			lac_tac = lac1;
			cid_pci = cid1;
		}
		//网络(2)信息存在时 当前数据返回的是网络(2)的信息
		if(flag2 && net_type2.equals(net_type)){
			sinr = sinr2;
			net_type = net_type2;
			signal_strength = signal_strength2;
			lac_tac = lac2;
			cid_pci = cid2;
		}
		if(!(flag1 && net_type.equals(net_type1)) && !(flag2 && net_type.equals(net_type2))){
			if(lac1 != null){
				lac_tac = lac1;
			}
			if(cid1 != null){
				cid_pci = cid1;
			}
			if(lac2 != null){
				lac_tac = lac2;
			}
			if(cid2 != null){
				cid_pci = cid2;
			}
		}
		map.put("lac_tac",lac_tac);
		map.put("cid_pci",cid_pci);
		map.put("sinr",sinr);
		if(net_type.equals("WIFI")){
			map.put("signal_strength",wifi_ss);	
		}else{
			map.put("signal_strength",signal_strength);
		}
		
		map.put("net_type",net_type);
		
		map.put("sinr1", sinr1);
		map.put("net_type1", net_type1);
		map.put("signal_strength1", signal_strength1);
		map.put("lac_tac1", lac1);
		map.put("cid_pci1", cid1);
		
		map.put("sinr2", sinr2);
		map.put("net_type2", net_type2);
		map.put("signal_strength2", signal_strength2);
		map.put("lac_tac2", lac2);
		map.put("cid_pci2", cid2);
		
		String file_index = (String)dataMap.get("file_index");
		String system_version = "";
		if(model.toLowerCase().contains("iphone")){
			map.put("android_ios", "ios");	
		}else{
			map.put("android_ios", "android");
		}
		map.put("file_index", file_index);
		sql = appendSql(map,numType,file_index);
		return sql;
	}
	/**
	 * 插入call数据
	 */
	public StringBuffer insertCall(Map dataMap,String keyspace,String numType,String detailreport){
		List nameList = new ArrayList();
		List valueList = new ArrayList();
		Map map = new HashMap();
		Set keySet = dataMap.keySet();
		Iterator iter = keySet.iterator();
		String imei = (String)(dataMap.get("imei")==null?"":dataMap.get("imei"));
		String file_path = (String)(dataMap.get("filePath")==null?"":dataMap.get("filePath"));
		String detailreportvalue = (String)(dataMap.get("detailreport")==null?"":dataMap.get("detailreport"));
		map.put("detailreport", detailreport);

		map.put("imei",imei);
		map.put("file_path",file_path.replace("\\", "|").replace("//", "|"));
		
		String device_org = keyspace;//终端分组
		String longitude = "";
		String latitude = "";
		String province = "";//测试省
		String district = "";//测试district
		String city = "";//测试city
		String time = dataMap.get("time")==null?"":dataMap.get("time").toString();//测试时间
		String model = "";//终端类型
		String logversion = "";//软件版本
		map.put("device_org", device_org);
		map.put("time", time);
		
		String continuing_time = "";//接续时长
		String connecting_time = "";//接通时长
		String connected_time = "";//通话时长
		String recording = "";
		String phone_number = "";
		String target_number = "";
		String network = "";
		String hand_over_situation = "";
		String success_failure = "";
		String success_rate = "";
		String fallbacktocs = "";
		String backtolte = "";
		
		String location = "";//测试地点
		String gpsStr = "";
		String net_type = "";//网络类型
		String signal_strength = "";//信号强度
		String sinr = "";//SINR
		String lac = "";
		String cid = "";
		String tac = "";
		String pci = "";
		String lac_tac = "";
		String cid_pci = "";
		String net_type1 = "";
		String signal_strength1 = "";
		String sinr1 = "";
		String cell1 = "";
		String net_type2 = "";
		String signal_strength2 = "";
		String sinr2 = "";
		String cell2 = "";
		String wifi_ss = "";//WIFI SIGNAL STRENGTH
		
		boolean flag1 = false;
		boolean flag2 = false;
		
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
		map.put("gps", gpsStr);
		if(gpsStr.contains(" ")){
			String[] gps = transGpsPoint(gpsStr);
			if(gps!=null && gps[0]!=null && gps[1]!=null){
				longitude = gps[0];
				latitude = gps[1];
			}
		}
		double longitudeNum = 0.0;
		double latitudeNum = 0.0;
		if(longitude.isEmpty() && latitude.isEmpty()){
			
		}else{
			double[] em = Earth2Mars.transform(Double.parseDouble(longitude), Double.parseDouble(latitude));
			double[] bf = BdFix.bd_encrypt(em[0], em[1]);
			longitudeNum = bf[0];
			latitudeNum = bf[1];
		}
		map.put("longitude",longitudeNum);
		map.put("latitude",latitudeNum);
		if(longitudeNum == 0.0 || longitudeNum == 0){
			map.put("longitude","");
		}
		if(latitudeNum == 0.0 || latitudeNum == 0){
			map.put("latitude","");
		}
		
		//获取GPS位置信息----end
		if(dataMap.containsKey("网络(1)网络制式") || dataMap.containsKey("Network(1) Type") || dataMap.containsKey("Network (1) standard")){
			flag1 = true;
		}
		if(dataMap.containsKey("网络(2)网络制式") || dataMap.containsKey("Network(2) Type") || dataMap.containsKey("Network (1) standard")){
			flag2 = true;
		}
		while (iter.hasNext()) {
			String key = iter.next() + "";
			if (key == null || key.isEmpty()) {
				continue;
			}
			
			if("平均速率(Kbps)".equals(key) || "平均速率(Mbps)".equals(key) ||"平均速率（Mbps）".equals(key) || "average speed (mbps)".equals(key.toLowerCase()) || "average speed (kbps)".equals(key.toLowerCase()) || "avg speed(kbps)".equals(key.toLowerCase()) || "avg speed(mbps)".equals(key.toLowerCase())){
			}else if("logversion".equals(key.toLowerCase()) || "日志版本".equals(key)){
				logversion = (String) (dataMap.get(key)==null?"":dataMap.get(key));
				map.put("logversion", logversion);
			}else if("信号强度".equals(key) || "Signal Strength".equals(key)){
				signal_strength = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("终端类型".equals(key) || "终端型号".equals(key) || "device type".equals(key.toLowerCase()) || "terminal model".equals(key.toLowerCase()) || "terminal type".equals(key.toLowerCase()) || "terminal mode".equals(key.toLowerCase()) || "device model".equals(key.toLowerCase()) || "terminal models".equals(key.toLowerCase())){
				model = (String)(dataMap.get(key)==null?"":dataMap.get(key));
				map.put("model",model);
			}else if("是否成功".equals(key) || "success or not".equals(key.toLowerCase())){
				success_failure = (String)(dataMap.get(key)==null?"":dataMap.get(key));
				map.put("success_failure",success_failure);
			}else if("接续时长(ms)".equals(key) || "Continuing Time(ms)".equals(key)){
				continuing_time = (String)(dataMap.get(key)==null?"":dataMap.get(key));
				map.put("continuing_time",continuing_time);
			}else if("接通时长(ms)".equals(key) || "Connecting time(ms)".equals(key)){
				connecting_time = (String)(dataMap.get(key)==null?"":dataMap.get(key));
				map.put("connecting_time",connecting_time);
			}else if("通话时长(s)".equals(key) || "Connected time(s)".equals(key)){
				connected_time = (String)(dataMap.get(key)==null?"":dataMap.get(key));
				map.put("connected_time",connected_time);
			}else if("手机号码".equals(key) || "Phone Number".equals(key)){
				target_number = (String)(dataMap.get(key)==null?"":dataMap.get(key));
				map.put("target_number",target_number);
			}else if("网络类型".equals(key) || "网络制式".equals(key) || "network type".equals(key.toLowerCase())){
				network = (String)(dataMap.get(key)==null?"":dataMap.get(key));
				map.put("network",network);
			}else if("fallbackToCs(ms)".equals(key) || "FallBack To CS(s)".equals(key)){
				hand_over_situation = (String)(dataMap.get(key)==null?"":dataMap.get(key));
				map.put("hand_over_situation", hand_over_situation);
				fallbacktocs = (String)(dataMap.get(key)==null?"":dataMap.get(key));
			}else if("backToLte(ms)".equals(key) || "Back To Lte(ms)".equals(key)){
				backtolte = (String)(dataMap.get(key)==null?"":dataMap.get(key));
			}else if("是否录音".equals(key) || "Call Recording".equals(key)){
				recording = (String)(dataMap.get(key)==null?"":dataMap.get(key));
				map.put("recording",recording);
			}else if("成功率".equals(key) || "success rate".equals(key.toLowerCase())){
				success_rate = (String)(dataMap.get(key)==null?"":dataMap.get(key));
				map.put("success_rate", success_rate);
			}else if("网络".equals(key) || "Network".equals(key)){
				net_type = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("SINR".equals(key)){
				sinr = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("网络(1)网络制式".equals(key) || "network(1) type".equals(key.toLowerCase()) || "network (1) standard".equals(key.toLowerCase())){
				net_type1 = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("网络(1)信号强度".equals(key) || "网络(1)信号强度(dBm)".equals(key) || "network(1) signal strength".equals(key.toLowerCase()) || "network(1) signal strength(dbm)".equals(key.toLowerCase())){
				signal_strength1 = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("网络(1)小区信息".equals(key) || "network(1) cell".equals(key.toLowerCase()) || "network(1)".equals(key.toLowerCase())){
				cell1 = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("网络(1)SINR".equals(key) || "网络(1)SINR(dB)".equals(key) || "network(1) sinr".equals(key.toLowerCase()) || "network(1) sinr(db)".equals(key.toLowerCase())){
				sinr1 = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("网络(2)网络制式".equals(key) || "network(2) type".equals(key.toLowerCase()) || "network (2) standard".equals(key.toLowerCase())){
				net_type2 = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("网络(2)信号强度".equals(key) || "网络(2)信号强度(dBm)".equals(key)	 || "network(2) signal strength".equals(key.toLowerCase())){
				signal_strength2 = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("网络(2)小区信息".equals(key) || "network(2) cell".equals(key.toLowerCase()) || "network(2)".equals(key.toLowerCase())){
				cell2 = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("网络(2)SINR".equals(key) || "网络(2)SINR(dB)".equals(key) || "network(2) sinr".equals(key.toLowerCase()) || "network(2) sinr(db)".equals(key.toLowerCase())){
				sinr2 = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("WIFI信号强度".equals(key) || "WIFI信号强度(dBm)".equals(key) || "wifi signal strength".equals(key.trim().toLowerCase()) || "wifi signal strength(dbm)".equals(key.trim().toLowerCase())){
				wifi_ss = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}
		}
		if(file_path.contains("05001.C")){
			map.put("csfb", 1);
			map.put("backtolte", backtolte);
			map.put("fallbacktocs", fallbacktocs);
		}else{
			map.put("csfb", 0);
		}
		String lac1 = "";
		String cid1 = "";
		if(cell1.indexOf("/")!=-1 || cell1.indexOf("N/A")!=-1){
			String[] cellInfo = cell1.split("/");
			if(cellInfo[0]!=null && !cellInfo[0].equals("")){
				lac1 = cellInfo[0];
			}
			if(cellInfo[1]!=null && !cellInfo[1].equals("")){
				cid1 = cellInfo[1];
			}
		}
		String lac2 = "";
		String cid2 = "";
		if(cell2.indexOf("/")!=-1 || cell2.indexOf("N/A")!=-1){
			String[] cellInfo = cell2.split("/");
			if(cellInfo[0]!=null && !cellInfo[0].equals("")){
				lac2 = cellInfo[0];
			}
			if(cellInfo[1]!=null && !cellInfo[1].equals("")){
				cid2 = cellInfo[1];
			}
		}
		//网络(1)信息存在时 当前数据返回的是网络(1)的信息
		if(flag1 && net_type1.equals(net_type)){
			sinr = sinr1;
			net_type = net_type1;
			signal_strength = signal_strength1;
			lac_tac = lac1;
			cid_pci = cid1;
		}
		//网络(2)信息存在时 当前数据返回的是网络(2)的信息
		if(flag2 && net_type2.equals(net_type)){
			sinr = sinr2;
			net_type = net_type2;
			signal_strength = signal_strength2;
			lac_tac = lac2;
			cid_pci = cid2;
		}
		if(!(flag1 && net_type.equals(net_type1)) && !(flag2 && net_type.equals(net_type2))){
			if(lac1 != null){
				lac_tac = lac1;
			}
			if(cid1 != null){
				cid_pci = cid1;
			}
			if(lac2 != null){
				lac_tac = lac2;
			}
			if(cid2 != null){
				cid_pci = cid2;
			}
		}
		map.put("lac_tac",lac_tac);
		map.put("cid_pci",cid_pci);
		map.put("sinr",sinr);
		if(net_type.equals("WIFI")){
			map.put("signal_strength",wifi_ss);	
		}else{
			map.put("signal_strength",signal_strength);
		}
		map.put("net_type",net_type);
		
		map.put("sinr1", sinr1);
		map.put("net_type1", net_type1);
		map.put("signal_strength1", signal_strength1);
		map.put("lac_tac1", lac1);
		map.put("cid_pci1", cid1);
		
		map.put("sinr2", sinr2);
		map.put("net_type2", net_type2);
		map.put("signal_strength2", signal_strength2);
		map.put("lac_tac2", lac2);
		map.put("cid_pci2", cid2);
		
		String file_index = (String)dataMap.get("file_index");
		String system_version = "";
		if(model.toLowerCase().contains("iphone")){
			map.put("android_ios", "ios");	
		}else{
			map.put("android_ios", "android");
		}
		map.put("file_index", file_index);
		sql = appendSql(map,numType,file_index);
		return sql;
	}
	/**
	 * 插入ResideTest数据
	 */
	public StringBuffer insertResideTest(Map dataMap,String keyspace,String numType,String detailreport){
		List nameList = new ArrayList();
		List valueList = new ArrayList();
		Map map = new HashMap();
		Set keySet = dataMap.keySet();
		Iterator iter = keySet.iterator();
		String imei = (String)(dataMap.get("imei")==null?"":dataMap.get("imei"));
		String file_path = (String)(dataMap.get("filePath")==null?"":dataMap.get("filePath"));
		String detailreportvalue = (String)(dataMap.get("detailreport")==null?"":dataMap.get("detailreport"));
		map.put("detailreport", detailreport);
		
		map.put("imei",imei);
		map.put("file_path",file_path.replace("\\", "|").replace("//", "|"));
		
		String device_org = keyspace;//终端分组
		map.put("device_org", device_org);
		String longitude = "";
		String latitude = "";
		String province = "";//测试省
		String district = "";//测试district
		String city = "";//测试city
		String time = dataMap.get("time")==null?"":dataMap.get("time").toString();//测试时间
		String model = "";//终端类型
		String logversion = "";//软件版本
		map.put("device_org", device_org);
		map.put("time", time);
		
		String duration = "";
		String max_latency = "";
		String min_latency = "";
		String avg_latency = "";
		
		String location = "";//测试地点
		String gpsStr = "";
		String net_type = "";//网络类型
		String signal_strength = "";//信号强度
		String sinr = "";//SINR
		String lac = "";
		String cid = "";
		String tac = "";
		String pci = "";
		String lac_tac = "";
		String cid_pci = "";
		String net_type1 = "";
		String signal_strength1 = "";
		String sinr1 = "";
		String cell1 = "";
		String net_type2 = "";
		String signal_strength2 = "";
		String sinr2 = "";
		String cell2 = "";
		String wifi_ss = "";//WIFI SIGNAL STRENGTH
		
		boolean flag1 = false;
		boolean flag2 = false;
		
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
		map.put("gps", gpsStr);
		if(gpsStr.contains(" ")){
			String[] gps = transGpsPoint(gpsStr);
			if(gps!=null && gps[0]!=null && gps[1]!=null){
				longitude = gps[0];
				latitude = gps[1];
			}
		}
		double longitudeNum = 0.0;
		double latitudeNum = 0.0;
		if(longitude.isEmpty() && latitude.isEmpty()){
			
		}else{
			double[] em = Earth2Mars.transform(Double.parseDouble(longitude), Double.parseDouble(latitude));
			double[] bf = BdFix.bd_encrypt(em[0], em[1]);
			longitudeNum = bf[0];
			latitudeNum = bf[1];
		}
		map.put("longitude",longitudeNum);
		map.put("latitude",latitudeNum);
		if(longitudeNum == 0.0 || longitudeNum == 0){
			map.put("longitude","");
		}
		if(latitudeNum == 0.0 || latitudeNum == 0){
			map.put("latitude","");
		}
		
		//获取GPS位置信息----end
		if(dataMap.containsKey("网络(1)网络制式") || dataMap.containsKey("Network(1) Type") || dataMap.containsKey("Network (1) standard")){
			flag1 = true;
		}
		if(dataMap.containsKey("网络(2)网络制式") || dataMap.containsKey("Network(2) Type") || dataMap.containsKey("Network (1) standard")){
			flag2 = true;
		}
		while (iter.hasNext()) {
			String key = iter.next() + "";
			if (key == null || key.isEmpty()) {
				continue;
			}
			
			if("平均速率(Kbps)".equals(key) || "平均速率(Mbps)".equals(key) ||"平均速率（Mbps）".equals(key) || "average speed (mbps)".equals(key.toLowerCase()) || "average speed (kbps)".equals(key.toLowerCase()) || "avg speed(kbps)".equals(key.toLowerCase()) || "avg speed(mbps)".equals(key.toLowerCase())){
			}else if("logversion".equals(key.toLowerCase()) || "日志版本".equals(key)){
				logversion = (String) (dataMap.get(key)==null?"":dataMap.get(key));
				map.put("logversion", logversion);
			}else if("信号强度".equals(key) || "Signal Strength".equals(key)){
				signal_strength = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("终端类型".equals(key) || "终端型号".equals(key) || "device type".equals(key.toLowerCase()) || "terminal model".equals(key.toLowerCase()) || "terminal type".equals(key.toLowerCase()) || "terminal mode".equals(key.toLowerCase()) || "device model".equals(key.toLowerCase()) || "terminal models".equals(key.toLowerCase())){
				model = (String)(dataMap.get(key)==null?"":dataMap.get(key));
				map.put("model",model);
			}else if("切换重选时延(ms)".equals(key) || "平均时长(ms)".equals(key) || "平均时延".equals(key) || "平均时延(ms)".equals(key) || "Avg Latency(ms)".equals(key) || "Avg Time(ms)".equals(key) || "Latency".equals(key) || "Latency(ms)".equals(key) || "Delay".equals(key) || "Delay(ms)".equals(key) || "Handover Latency".equals(key) ){
				avg_latency = (String)(dataMap.get(key)==null?"":dataMap.get(key));
				map.put("avg_latency",avg_latency);
			}else if("网络类型".equals(key) || "网络制式".equals(key) || "network type".equals(key.toLowerCase())){
				net_type = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("SINR".equals(key)){
				sinr = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("网络(1)网络制式".equals(key) || "network(1) type".equals(key.toLowerCase()) || "network (1) standard".equals(key.toLowerCase())){
				net_type1 = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("网络(1)信号强度".equals(key) || "网络(1)信号强度(dBm)".equals(key) || "network(1) signal strength".equals(key.toLowerCase()) || "network(1) signal strength(dbm)".equals(key.toLowerCase())){
				signal_strength1 = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("网络(1)小区信息".equals(key) || "network(1) cell".equals(key.toLowerCase()) || "network(1)".equals(key.toLowerCase())){
				cell1 = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("网络(1)SINR".equals(key) || "网络(1)SINR(dB)".equals(key) || "network(1) sinr".equals(key.toLowerCase()) || "network(1) sinr(db)".equals(key.toLowerCase())){
				sinr1 = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("网络(2)网络制式".equals(key) || "network(2) type".equals(key.toLowerCase()) || "network (2) standard".equals(key.toLowerCase())){
				net_type2 = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("网络(2)信号强度".equals(key) || "网络(2)信号强度(dBm)".equals(key)	 || "network(2) signal strength".equals(key.toLowerCase())){
				signal_strength2 = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("网络(2)小区信息".equals(key) || "network(2) cell".equals(key.toLowerCase()) || "network(2)".equals(key.toLowerCase())){
				cell2 = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("网络(2)SINR".equals(key) || "网络(2)SINR(dB)".equals(key) || "network(2) sinr".equals(key.toLowerCase()) || "network(2) sinr(db)".equals(key.toLowerCase())){
				sinr2 = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("WIFI信号强度".equals(key) || "WIFI信号强度(dBm)".equals(key) || "wifi signal strength".equals(key.trim().toLowerCase()) || "wifi signal strength(dbm)".equals(key.trim().toLowerCase())){
				wifi_ss = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}
		}
		String lac1 = "";
		String cid1 = "";
		if(cell1.indexOf("/")!=-1 || cell1.indexOf("N/A")!=-1){
			String[] cellInfo = cell1.split("/");
			if(cellInfo[0]!=null && !cellInfo[0].equals("")){
				lac1 = cellInfo[0];
			}
			if(cellInfo[1]!=null && !cellInfo[1].equals("")){
				cid1 = cellInfo[1];
			}
		}
		String lac2 = "";
		String cid2 = "";
		if(cell2.indexOf("/")!=-1 || cell2.indexOf("N/A")!=-1){
			String[] cellInfo = cell2.split("/");
			if(cellInfo[0]!=null && !cellInfo[0].equals("")){
				lac2 = cellInfo[0];
			}
			if(cellInfo[1]!=null && !cellInfo[1].equals("")){
				cid2 = cellInfo[1];
			}
		}
		//网络(1)信息存在时 当前数据返回的是网络(1)的信息
		if(flag1 && net_type1.equals(net_type)){
			sinr = sinr1;
			net_type = net_type1;
			signal_strength = signal_strength1;
			lac_tac = lac1;
			cid_pci = cid1;
		}
		//网络(2)信息存在时 当前数据返回的是网络(2)的信息
		if(flag2 && net_type2.equals(net_type)){
			sinr = sinr2;
			net_type = net_type2;
			signal_strength = signal_strength2;
			lac_tac = lac2;
			cid_pci = cid2;
		}
		if(!(flag1 && net_type.equals(net_type1)) && !(flag2 && net_type.equals(net_type2))){
			if(lac1 != null){
				lac_tac = lac1;
			}
			if(cid1 != null){
				cid_pci = cid1;
			}
			if(lac2 != null){
				lac_tac = lac2;
			}
			if(cid2 != null){
				cid_pci = cid2;
			}
		}
		map.put("lac_tac",lac_tac);
		map.put("cid_pci",cid_pci);
		map.put("sinr",sinr);
		if(net_type.equals("WIFI")){
			map.put("signal_strength",wifi_ss);	
		}else{
			map.put("signal_strength",signal_strength);
		}
		map.put("net_type",net_type);
		
		map.put("sinr1", sinr1);
		map.put("net_type1", net_type1);
		map.put("signal_strength1", signal_strength1);
		map.put("lac_tac1", lac1);
		map.put("cid_pci1", cid1);
		
		map.put("sinr2", sinr2);
		map.put("net_type2", net_type2);
		map.put("signal_strength2", signal_strength2);
		map.put("lac_tac2", lac2);
		map.put("cid_pci2", cid2);
		
		String file_index = (String)dataMap.get("file_index");
		String system_version = "";
		if(model.toLowerCase().contains("iphone")){
			map.put("android_ios", "ios");	
		}else{
			map.put("android_ios", "android");
		}
		map.put("file_index", file_index);
		sql = appendSql(map,numType,file_index);
		return sql;
	}
	
	/**
	 * 插入SMS数据
	 */
	public StringBuffer insertSMS(Map dataMap,String keyspace,String numType,String detailreport){
		List nameList = new ArrayList();
		List valueList = new ArrayList();
		Map map = new HashMap();
		Set keySet = dataMap.keySet();
		Iterator iter = keySet.iterator();
		String imei = (String)(dataMap.get("imei")==null?"":dataMap.get("imei"));
		String file_path = (String)(dataMap.get("filePath")==null?"":dataMap.get("filePath"));
		String detailreportvalue = (String)(dataMap.get("detailreport")==null?"":dataMap.get("detailreport"));
		map.put("detailreport", detailreport);
		
		map.put("imei",imei);
		map.put("file_path",file_path.replace("\\", "|").replace("//", "|"));
		
		String device_org = keyspace;//终端分组
		map.put("device_org", device_org);
		String longitude = "";
		String latitude = "";
		String province = "";//测试省
		String district = "";//测试district
		String city = "";//测试city
		String time = dataMap.get("time")==null?"":dataMap.get("time").toString();//测试时间
		String model = "";//终端类型
		String logversion = "";//软件版本
		map.put("device_org", device_org);
		map.put("time", time);
		
		String target_number = "";//目标号码
		String sending_delay = "";//发送时延
		String success_times = "";//成功次数
		String test_times = "";//测试次数
		String success_rate = "";//成功率
		String success_failure = "";
		String content = "";//发送内容
		String testtype= "";//长短信或者短短信
		
		String location = "";//测试地点
		String gpsStr = "";
		String net_type = "";//网络类型
		String signal_strength = "";//信号强度
		String sinr = "";//SINR
		String lac = "";
		String cid = "";
		String tac = "";
		String pci = "";
		String lac_tac = "";
		String cid_pci = "";
		String net_type1 = "";
		String signal_strength1 = "";
		String sinr1 = "";
		String cell1 = "";
		String net_type2 = "";
		String signal_strength2 = "";
		String sinr2 = "";
		String cell2 = "";
		String wifi_ss = "";//WIFI SIGNAL STRENGTH
		
		boolean flag1 = false;
		boolean flag2 = false;
		
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
		map.put("gps", gpsStr);
		if(gpsStr.contains(" ")){
			String[] gps = transGpsPoint(gpsStr);
			if(gps!=null && gps[0]!=null && gps[1]!=null){
				longitude = gps[0];
				latitude = gps[1];
			}
		}
		double longitudeNum = 0.0;
		double latitudeNum = 0.0;
		if(longitude.isEmpty() && latitude.isEmpty()){
			
		}else{
			double[] em = Earth2Mars.transform(Double.parseDouble(longitude), Double.parseDouble(latitude));
			double[] bf = BdFix.bd_encrypt(em[0], em[1]);
			longitudeNum = bf[0];
			latitudeNum = bf[1];
		}
		map.put("longitude",longitudeNum);
		map.put("latitude",latitudeNum);
		if(longitudeNum == 0.0 || longitudeNum == 0){
			map.put("longitude","");
		}
		if(latitudeNum == 0.0 || latitudeNum == 0){
			map.put("latitude","");
		}
		
		//获取GPS位置信息----end
		if(dataMap.containsKey("网络(1)网络制式") || dataMap.containsKey("Network(1) Type") || dataMap.containsKey("Network (1) standard")){
			flag1 = true;
		}
		if(dataMap.containsKey("网络(2)网络制式") || dataMap.containsKey("Network(2) Type") || dataMap.containsKey("Network (1) standard")){
			flag2 = true;
		}
		while (iter.hasNext()) {
			String key = iter.next() + "";
			if (key == null || key.isEmpty()) {
				continue;
			}
			if("平均速率(Kbps)".equals(key) || "平均速率(Mbps)".equals(key) ||"平均速率（Mbps）".equals(key) || "average speed (mbps)".equals(key.toLowerCase()) || "average speed (kbps)".equals(key.toLowerCase()) || "avg speed(kbps)".equals(key.toLowerCase()) || "avg speed(mbps)".equals(key.toLowerCase())){
			}else if("logversion".equals(key.toLowerCase()) || "日志版本".equals(key)){
				logversion = (String) (dataMap.get(key)==null?"":dataMap.get(key));
				map.put("logversion", logversion);
			}else if("信号强度".equals(key) || "signal strength".equals(key.toLowerCase())){
				signal_strength = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("发送内容".equals(key) || "content".equals(key.toLowerCase())){
				content = (String) (dataMap.get(key)==null?"":dataMap.get(key));
				if(content.getBytes().length>70){
					testtype = "long";
					map.put("testtype", testtype);
				}else{
					testtype = "short";
					map.put("testtype", testtype);
				}
			}else if("终端类型".equals(key) || "终端型号".equals(key) || "device type".equals(key.toLowerCase()) || "terminal model".equals(key.toLowerCase()) || "terminal type".equals(key.toLowerCase()) || "terminal mode".equals(key.toLowerCase()) || "device model".equals(key.toLowerCase()) || "terminal models".equals(key.toLowerCase())){
				model = (String)(dataMap.get(key)==null?"":dataMap.get(key));
				map.put("model",model);
			}else if("时延(ms)".equals(key) || "delay(ms)".equals(key.toLowerCase())){
				sending_delay = (String)(dataMap.get(key)==null?"":dataMap.get(key));
				map.put("sending_delay", sending_delay);
			}else if("手机号码".equals(key) || "phone number".equals(key.toLowerCase())){
				target_number = (String)(dataMap.get(key)==null?"":dataMap.get(key));
				map.put("target_number", target_number);
			}else if("测试次数".equals(key) || "test times".equals(key.trim().toLowerCase()) || "send sms counts".equals(key.toLowerCase())){
				test_times = (String)(dataMap.get(key)==null?"":dataMap.get(key));
				map.put("test_times", test_times);
			}else if("成功率".equals(key) || "success rate".equals(key.toLowerCase())){
				success_rate = (String)(dataMap.get(key)==null?"":dataMap.get(key));
				map.put("success_rate", success_rate);
			}else if("成功次数".equals(key) || "succ counts".equals(key.toLowerCase())){
				success_times = (String)(dataMap.get(key)==null?"":dataMap.get(key));
				map.put("success_times", success_times);
			}else if("是否成功".equals(key) || "success or not".equals(key.toLowerCase())){
				success_failure = (String)(dataMap.get(key)==null?"":dataMap.get(key));
				map.put("success_failure",success_failure);
			}else if("网络类型".equals(key) || "网络制式".equals(key) || "network type".equals(key.toLowerCase())){
				net_type = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("SINR".equals(key)){
				sinr = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("网络(1)网络制式".equals(key) || "network(1) type".equals(key.toLowerCase()) || "network (1) standard".equals(key.toLowerCase())){
				net_type1 = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("网络(1)信号强度".equals(key) || "网络(1)信号强度(dBm)".equals(key) || "network(1) signal strength".equals(key.toLowerCase()) || "network(1) signal strength(dbm)".equals(key.toLowerCase())){
				signal_strength1 = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("网络(1)小区信息".equals(key) || "network(1) cell".equals(key.toLowerCase()) || "network(1)".equals(key.toLowerCase())){
				cell1 = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("网络(1)SINR".equals(key) || "网络(1)SINR(dB)".equals(key) || "network(1) sinr".equals(key.toLowerCase()) || "network(1) sinr(db)".equals(key.toLowerCase())){
				sinr1 = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("网络(2)网络制式".equals(key) || "network(2) type".equals(key.toLowerCase()) || "network (2) standard".equals(key.toLowerCase())){
				net_type2 = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("网络(2)信号强度".equals(key) || "网络(2)信号强度(dBm)".equals(key)	 || "network(2) signal strength".equals(key.toLowerCase())){
				signal_strength2 = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("网络(2)小区信息".equals(key) || "network(2) cell".equals(key.toLowerCase()) || "network(2)".equals(key.toLowerCase())){
				cell2 = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("网络(2)SINR".equals(key) || "网络(2)SINR(dB)".equals(key) || "network(2) sinr".equals(key.toLowerCase()) || "network(2) sinr(db)".equals(key.toLowerCase())){
				sinr2 = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("WIFI信号强度".equals(key) || "WIFI信号强度(dBm)".equals(key) || "wifi signal strength".equals(key.trim().toLowerCase()) || "wifi signal strength(dbm)".equals(key.trim().toLowerCase())){
				wifi_ss = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}
		}
		String lac1 = "";
		String cid1 = "";
		if(cell1.indexOf("/")!=-1 || cell1.indexOf("N/A")!=-1){
			String[] cellInfo = cell1.split("/");
			if(cellInfo[0]!=null && !cellInfo[0].equals("")){
				lac1 = cellInfo[0];
			}
			if(cellInfo[1]!=null && !cellInfo[1].equals("")){
				cid1 = cellInfo[1];
			}
		}
		String lac2 = "";
		String cid2 = "";
		if(cell2.indexOf("/")!=-1 || cell2.indexOf("N/A")!=-1){
			String[] cellInfo = cell2.split("/");
			if(cellInfo[0]!=null && !cellInfo[0].equals("")){
				lac2 = cellInfo[0];
			}
			if(cellInfo[1]!=null && !cellInfo[1].equals("")){
				cid2 = cellInfo[1];
			}
		}
		//网络(1)信息存在时 当前数据返回的是网络(1)的信息
		if(flag1 && net_type1.equals(net_type)){
			sinr = sinr1;
			net_type = net_type1;
			signal_strength = signal_strength1;
			lac_tac = lac1;
			cid_pci = cid1;
		}
		//网络(2)信息存在时 当前数据返回的是网络(2)的信息
		if(flag2 && net_type2.equals(net_type)){
			sinr = sinr2;
			net_type = net_type2;
			signal_strength = signal_strength2;
			lac_tac = lac2;
			cid_pci = cid2;
		}
		if(!(flag1 && net_type.equals(net_type1)) && !(flag2 && net_type.equals(net_type2))){
			if(lac1 != null){
				lac_tac = lac1;
			}
			if(cid1 != null){
				cid_pci = cid1;
			}
			if(lac2 != null){
				lac_tac = lac2;
			}
			if(cid2 != null){
				cid_pci = cid2;
			}
		}
		map.put("lac_tac",lac_tac);
		map.put("cid_pci",cid_pci);
		map.put("sinr",sinr);
		if(net_type.equals("WIFI")){
			map.put("signal_strength",wifi_ss);	
		}else{
			map.put("signal_strength",signal_strength);
		}
		map.put("net_type",net_type);
		
		map.put("sinr1", sinr1);
		map.put("net_type1", net_type1);
		map.put("signal_strength1", signal_strength1);
		map.put("lac_tac1", lac1);
		map.put("cid_pci1", cid1);
		
		map.put("sinr2", sinr2);
		map.put("net_type2", net_type2);
		map.put("signal_strength2", signal_strength2);
		map.put("lac_tac2", lac2);
		map.put("cid_pci2", cid2);
		
		String file_index = (String)dataMap.get("file_index");
		String system_version = "";
		if(model.toLowerCase().contains("iphone")){
			map.put("android_ios", "ios");	
		}else{
			map.put("android_ios", "android");
		}
		map.put("file_index", file_index);
		sql = appendSql(map,numType,file_index);
		return sql;
	}
	
	private StringBuffer appendSql (Map map,String numType,String fileIndex){
		StringBuffer sql = new StringBuffer("");
		StringBuffer columnStr = new StringBuffer("(");
		StringBuffer valueStr = new StringBuffer(" values (");
		StringBuffer updateStr = new StringBuffer("");
		String table = "";
		if(numType.equals("01001")){
			table = "speed_test";
		}else if(numType.equals("02001")){
			table = "web_browsing";
		}else if(numType.equals("02011")){
			table = "video_test";
		}else if(numType.equals("03001") || numType.equals("03011")){
			table = "http_test";
		}else if(numType.equals("04002")){
			table = "ping";
		}else if(numType.equals("04006")){
			table = "hand_over";
		}else if(numType.equals("05001")){
			table = "call_test";
		}else if(numType.equals("05002")){
			table = "sms";
		}else {
			return new StringBuffer("");
		}
		System.out.println("开始组装sql   查询是否存在数据************："+new Date().toLocaleString() +"   毫秒："+new Date().getTime());
		boolean flag = this.queryExist(table,fileIndex);//有数据则为true 执行更新
		System.out.println("开始组装sql   查询完毕开始组装sql************："+new Date().toLocaleString() +"   毫秒："+new Date().getTime());
		if(flag){
			sql.append("update "+table+" set ");
			try {
				map.put("haschange", "0");
				Set set = map.keySet();
				Iterator iter = set.iterator();
				while(iter.hasNext()){
					String name = (String)iter.next();
					if(name.isEmpty()){
						continue;
					}
					String value = map.get(name)+"";
					if(value.isEmpty()){
						continue;
					}
					updateStr.append(name +" = '"+value+"', ");
				}
			
				while(updateStr.toString().trim().endsWith(",")){
					updateStr = new StringBuffer(updateStr.toString().substring(0,updateStr.toString().lastIndexOf(",")));
				}
				sql.append(updateStr);
				sql.append("where file_index = '"+fileIndex+"'");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}else{
			sql.append("insert into "+table+" ");
			try {
				map.put("haschange", "0");
				Set set = map.keySet();
				Iterator iter = set.iterator();
				while(iter.hasNext()){
					String name = (String)iter.next();
					if(name.isEmpty()){
						continue;
					}
					String value = map.get(name)+"";
					if(value.isEmpty()){
						continue;
					}
					columnStr.append(name+",");
					valueStr.append("'"+value+"',");
				}
				while(columnStr.toString().trim().endsWith(",")){
					columnStr = new StringBuffer(columnStr.toString().substring(0,columnStr.toString().lastIndexOf(",")));
				}
				while(valueStr.toString().trim().endsWith(",")){
					valueStr = new StringBuffer(valueStr.toString().substring(0,valueStr.toString().lastIndexOf(",")));
				}
				columnStr.append(" )");
				valueStr.append(" )");
				sql.append(columnStr);
				sql.append(valueStr);
			} catch (Exception e) {
				System.out.println("拼装语句错误："+sql);
				sql = new StringBuffer("");
				e.printStackTrace();
			}finally{
				try {
					close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		System.out.println("组装完毕sql************："+new Date().toLocaleString() +"   毫秒："+new Date().getTime());
		return sql;
	}
//	private StringBuffer appendSql (List nameList,List valueList,String numType,String imei,String file_path){
//		StringBuffer sql = new StringBuffer("");
//		String database = "testdataanalyse";
//		StringBuffer columnStr = new StringBuffer("(");
//		StringBuffer valueStr = new StringBuffer(" values (");
//		StringBuffer updateStr = new StringBuffer("");
//		String table = "";
//		if(numType.equals("01001")){
//			table = "speed_test";
//		}else if(numType.equals("02001")){
//			table = "web_browsing";
//		}else if(numType.equals("03001")){
//			table = "internet_downloading";
//		}else if(numType.equals("04002")){
//			table = "ping";
//		}else if(numType.equals("04006")){
//			table = "hand_over";
//		}else if(numType.equals("05001")){
//			table = "call_test";
//		}
//		boolean flag = this.queryExist(database,table,imei,file_path);//有数据则为true 执行更新
//		if(flag){
//			sql.append("update "+database+"."+table+" set ");
//			try {
//				for (int i = 0; i < nameList.size(); i++) {
//					String name = nameList.get(i)==null?"":nameList.get(i).toString();
//					if(name.isEmpty()){
//						continue;
//					}
//					String value = valueList.get(i)==null?"":valueList.get(i).toString();
//					if(value.isEmpty()){
//						continue;
//					}
//					if(i==nameList.size()-1){
//						updateStr.append(name +" = '"+value+"' ");
//					}else{
//						updateStr.append(name +" = '"+value+"', ");
//					}
//				}
//				while(updateStr.toString().trim().endsWith(",")){
//					updateStr = new StringBuffer(updateStr.toString().substring(0,updateStr.toString().lastIndexOf(",")));
//				}
//				sql.append(updateStr);
//				sql.append("where imei = '"+imei+"' and file_path = '"+file_path+"'");
//			} catch (Exception e) {
//				e.printStackTrace();
//			}
//		}else{
//			sql.append("insert into "+database+"."+table+" ");
//			try {
//				for (int i = 0; i < nameList.size(); i++) {
//					String name = nameList.get(i)==null?"":nameList.get(i).toString();
//					if(name.equals("") || name.isEmpty()){
//						continue;
//					}
//					String value = valueList.get(i)==null?"":valueList.get(i).toString();
//					if(i==nameList.size()-1){
//						columnStr.append(name+")");
//						valueStr.append("'"+value+"')");
//					}else{
//						columnStr.append(name+",");
//						valueStr.append("'"+value+"',");
//					}
//				}
//				while(columnStr.toString().trim().endsWith(",")){
//					columnStr = new StringBuffer(columnStr.toString().substring(0,columnStr.toString().lastIndexOf(",")));
//				}
//				while(valueStr.toString().trim().endsWith(",")){
//					valueStr = new StringBuffer(valueStr.toString().substring(0,valueStr.toString().lastIndexOf(",")));
//				}
//				sql.append(columnStr);
//				sql.append(valueStr);
//			} catch (Exception e) {
//				System.out.println("拼装语句错误："+sql);
//				sql = new StringBuffer("");
//				e.printStackTrace();
//			}finally{
//				try {
//					close();
//				} catch (SQLException e) {
//					e.printStackTrace();
//				}
//			}
//		}
//		return sql;
//	}
	public String queryOrgKeyById(String id){
		if(id==null || id.isEmpty() || id.equals("EMPTY")){
			return "";	
		}else{
			String sql = "select org_key from auth.t_org where id = '"+id+"'";
			try {
				start();
				ResultSet rs = (ResultSet)statement.executeQuery(sql);
				if(rs.next()){
					return rs.getString("org_key");
				}
			} catch (Exception e) {
				e.printStackTrace();
			}finally{
				try {
					close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			return "";
		}
	}
	public boolean queryExist(String table,String fileIndex){
		
		String sql  = "select count(*) count from "+table+" where file_index = '"+fileIndex+"'";
		System.out.println("查询的sql："+sql);
		
		try {
			start();
			ResultSet rs = (ResultSet)statement.executeQuery(sql);
			if(rs.next()){
				int count = rs.getInt("count");
				if(count == 1){
					return true;	
				}else{
					return false;
				}
				
			}
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			try {
				close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return false;
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
					
					DecimalFormat decimalFormat=new DecimalFormat(".0000000");
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
						DecimalFormat decimalFormat=new DecimalFormat(".0000000");
						gpsPoint = decimalFormat.format(gpsLong);
						longitude = gpsPoint;
					}else if(info[i].contains("N")){
						gpsPoint = info[i].substring(0,info[i].lastIndexOf("N"));
						float gpsLong = Float.valueOf(gpsPoint);
						DecimalFormat decimalFormat=new DecimalFormat(".0000000");
						gpsPoint = decimalFormat.format(gpsLong);
						latitude = gpsPoint;
					}else{
						gpsPoint = info[i];
						float gpsLong = Float.valueOf(gpsPoint);
						DecimalFormat decimalFormat=new DecimalFormat(".0000000");
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
	public void execute(String name) {
		Connection conn1 = null;
        CallableStatement stat = null;
        String sql = "{call "+name+"()}";
        try {
        	start();
        	conn1 = DriverManager.getConnection(ConfParser.url,ConfParser.user,ConfParser.password);
            stat = conn1.prepareCall(sql);
            stat.executeUpdate();
        } catch (Exception e) {
            e.printStackTrace();
        }finally{
        	if(stat!=null){
        		try {
					stat.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
        	}
            if(conn1!=null){
            	try {
					conn1.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
            }
        }
	}
	
	public List getappIds(String sql){
		List list = new ArrayList();
		try {
			start();
			System.out.println(sql);
			ResultSet rs = statement.executeQuery(sql);
			while(rs.next()){
				String appid =rs.getString("app_id");
				if(appid==null || appid.isEmpty()){
					
				}else{
					list.add(appid);	
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			try {
				close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return list;
	}
}
