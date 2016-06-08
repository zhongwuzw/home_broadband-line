package com.opencassandra.resultsettest.separateapp;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URL;
import java.net.URLConnection;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.text.Format;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import net.sf.json.JSONObject;

import org.apache.commons.lang.StringUtils;

import com.mysql.jdbc.DatabaseMetaData;
import com.opencassandra.descfile.ConfParser;
import com.opencassandra.v01.dao.impl.BdFix;
import com.opencassandra.v01.dao.impl.Earth2Mars;



public class ResultsetTestDao{
	public static Statement statement;
	public static Connection conn;
	StringBuffer sql = new StringBuffer();
	private static String ak = ConfParser.ak;
	private String[] dateStr = new String[] { "yyyy", "MM", "dd", "HH", "mm",
			"ss", "SSS" };
	
	// 数据入库的地址
	static String dsturl = ConfParser.dsturl;
	 static String dstuser = ConfParser.dstuser;
	 static String dstpassword = ConfParser.dstpassword;
	
	/**
	 * 建立数据库连接
	 * 
	 * @param url
	 * @param user
	 * @param password
	 * @return void
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 */
	public void start(String url, String user, String password) throws Exception {
		System.out.println(url + " :数据库连接: " + user + " :: " + password);
		this.close();
		String driver = "com.mysql.jdbc.Driver";
		Class.forName(driver);
		conn = DriverManager.getConnection(url, user, password);
		statement = conn.createStatement();
	}

	/**
	 * 关闭数据库连接
	 * 
	 * @throws SQLException
	 * @return void
	 */
	public void close() throws SQLException {
		// TODO Auto-generated method stub
		if (statement != null) {
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
	
	public Map<StringBuffer, Boolean> insertMysqlDB(Map dataMap, String keyspace, String numType,
			File file, String detailreport,String testtime) {
		System.out.println("开始insertMysqlDB************："+new Date().toLocaleString() +"   毫秒："+new Date().getTime());
		
		System.out.println(keyspace+"   insertMysqlDB device_org 的值");

		StringBuffer sql = new StringBuffer("");
		Map<StringBuffer, Boolean> resultMap = new HashMap<StringBuffer, Boolean>();
		if (!numType.isEmpty()) {
			System.out.println("开始准备进行解析数据************："+new Date().toLocaleString() +"   毫秒："+new Date().getTime());
			if (numType.equals("01001")) {
				resultMap = insertSpeedTest(dataMap, keyspace, numType, detailreport,testtime);
			} else if (numType.equals("02001")) {
				resultMap = insertWebBrowser(dataMap, keyspace, numType, detailreport,testtime);
			} else if (numType.equals("02011")) {
				resultMap = insertVideoTest(dataMap, keyspace, numType, detailreport,testtime);
			} else if (numType.equals("03001") || numType.equals("03011")) {
				resultMap = insertHTTP(dataMap, keyspace, numType, detailreport,testtime);
			} else if (numType.equals("04002")) {
				resultMap = insertPING(dataMap, keyspace, numType, detailreport,testtime);
			} else if (numType.equals("05001")) {
				resultMap = insertCall(dataMap, keyspace, numType, detailreport,testtime);
			} else if (numType.equals("04006")) {
				resultMap = insertResideTest(dataMap, keyspace, numType, detailreport,testtime);
			} else if (numType.equals("05002")) {
				resultMap = insertSMS(dataMap, keyspace, numType, detailreport,testtime);
			} else if (numType.equals("05005")){
				resultMap = insertSMSQuery(dataMap, keyspace, numType, detailreport,testtime);
			}
		}
		return resultMap;
	}
	/**
	 * 插入speedTest数据
	 */
	public  Map<StringBuffer, Boolean> insertSpeedTest(Map dataMap,String keyspace,String numType,String detailreport,String testtime){
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
		String time =dataMap.get("time")==null?"":dataMap.get("time").toString();//测试时间
		String appid = dataMap.get("appid")==null?"":dataMap.get("appid").toString();//appid
		map.put("appid", appid);
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
		
		String test_description = "";
		String net_type1 = "";
		String signal_strength1 = "";
		String sinr1 = "";
		String cell1 = "";
		String net_type2 = "";
		String signal_strength2 = "";
		String sinr2 = "";
		String cell2 = "";
		String wifi_ss = "";//WIFI SIGNAL STRENGTH
		
		String connect_operator = "";
		String test_location = "";
		
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
		if(longitude.isEmpty() || latitude.isEmpty()){
			
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
			}else if("信号强度".equals(key) || "Signal Strength".equals(key)){
				signal_strength = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("终端类型".equals(key) || "终端型号".equals(key) || "device type".equals(key.toLowerCase()) || "terminal model".equals(key.toLowerCase()) || "terminal type".equals(key.toLowerCase()) || "terminal mode".equals(key.toLowerCase()) || "device model".equals(key.toLowerCase()) || "terminal models".equals(key.toLowerCase())){
				model = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("上行速率".equals(key)||"上行速率(Mbps)".equals(key) || "Up Speed".equals(key) || "Up Speed(Mbps)".equals(key) || "Up Speed(Kbps)".equals(key) || "Upload Speed".equals(key) || "Upload Speed(Mbps)".equals(key)){
				avg_up = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("下行速率".equals(key)||"下行速率(Mbps)".equals(key)  || "Down Speed".equals(key) || "Down Speed(Mbps)".equals(key) || "Down Speed(Kbps)".equals(key) || "Download Speed".equals(key) || "Download Speed(Mbps)".equals(key)){
				avg_down = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("maximum up".equals(key.toLowerCase()) || "maximum up(mbps)".equals(key.toLowerCase()) || "上行最大速率(Mbps)".equals(key) ||  "上行最大速率".equals(key)){
				max_up = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("maximum down".equals(key.toLowerCase()) || "maximum down(mbps)".equals(key.toLowerCase()) || "下行最大速率(Mbps)".equals(key) || "下行最大速率".equals(key)){
				max_down = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("时延".equals(key) || "时延(ms)".equals(key) || "平均时延(ms)".equals(key) || "平均时延(ms)".equals(key) || "平均时延".equals(key) || "average delay(ms)".equals(key.toLowerCase()) || "avg latency(ms)".equals(key.toLowerCase()) || "avg time(ms)".equals(key.toLowerCase()) || "avg latency".equals(key.toLowerCase()) || "delay".equals(key.toLowerCase()) || "delay(ms)".equals(key.toLowerCase()) || "latency".equals(key.toLowerCase()) || "latency(ms)".equals(key.toLowerCase())){
				delay = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("服务器信息".equals(key) || "服务器地址".equals(key) || "Server Info".equals(key) || "Service Info".equals(key)){
				server_ip = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("测速类型".equals(key) || "测试类型".equals(key) || "Protocol".equals(key)){
				protocol = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("上行线程数".equals(key) || "Uplink Thread(s)".equals(key) || "Uplink Thread Count".equals(key) || "Uplink Thread Counts".equals(key)){
				uplink = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("下行线程数".equals(key) || "Downlink Thread(s)".equals(key) || "Downlink Thread Count".equals(key) || "Downlink Thread Counts".equals(key) ){
				downlink = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("网络类型".equals(key) || "网络制式".equals(key) || "network type".equals(key.toLowerCase())){
				net_type = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("SINR".equals(key)){
				sinr = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("测试描述".equals(key) || "TestDescription".equals(key.toLowerCase())){
				test_description = (String) (dataMap.get(key)==null?"":dataMap.get(key));
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
			}else if("接入运营商".equals(key)){
				connect_operator = (String)(dataMap.get(key)==null?"":dataMap.get(key));
			}else if("测试地点".equals(key)){
				test_location = (String)(dataMap.get(key)==null?"":dataMap.get(key));
			}
		}
		
		map.put("connect_operator", connect_operator);
		map.put("test_location",test_location );
		map.put("logversion", logversion);
		map.put("model",model);
		map.put("avg_up",avg_up);
		map.put("avg_down",avg_down);
		map.put("max_up",max_up);
		map.put("max_down",max_down);
		map.put("delay",delay);
		map.put("server_ip",server_ip);
		map.put("protocol",protocol);
		map.put("uplink",uplink.isEmpty()?0:uplink);
		map.put("downlink",downlink.isEmpty()?0:downlink);
		
		map.put("max_down", max_down);
		map.put("max_up", max_up);
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
			sinr = sinr1;
			signal_strength = signal_strength1;
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
		map.put("test_description", test_description);
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
		return appendSql(map,numType,file_index,testtime);
	}
	/**
	 * 插入WebBrowser数据
	 */
	public  Map<StringBuffer, Boolean> insertWebBrowser(Map dataMap,String keyspace,String numType,String detailreport,String testtime){
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
		String appid = dataMap.get("appid")==null?"":dataMap.get("appid").toString();//appid
		
		map.put("appid", appid);
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
		String test_description = "";
		String net_type1 = "";
		String signal_strength1 = "";
		String sinr1 = "";
		String cell1 = "";
		String net_type2 = "";
		String signal_strength2 = "";
		String sinr2 = "";
		String cell2 = "";
		String wifi_ss = "";//WIFI SIGNAL STRENGTH
		String test_location = "";
		String connect_operator = "";
		
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
		if(longitude.isEmpty() || latitude.isEmpty()){
			
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
			}else if("logversion".equals(key.toLowerCase()) || "日志版本".equals(key)){
				logversion = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("信号强度".equals(key) || "Signal Strength".equals(key)){
				signal_strength = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("终端类型".equals(key) || "终端型号".equals(key) || "device type".equals(key.toLowerCase()) || "terminal model".equals(key.toLowerCase()) || "terminal type".equals(key.toLowerCase()) || "terminal mode".equals(key.toLowerCase()) || "device model".equals(key.toLowerCase()) || "terminal models".equals(key.toLowerCase())){
				model = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("地址".equals(key) || "网址".equals(key) || "Website".equals(key) || "address".equals(key)){
				web_sit = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("加载80%的平均时长(ms)".equals(key) || "加载80%平均时长(ms)".equals(key) || "加载80%的平均时长".equals(key) || "加载80%平均时长".equals(key) || "80% Avg Time".equals(key) || "80% Avg Time(ms)".equals(key)){
				eighty_loading = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("加载80%的平均速率(Kbps)".equals(key) || "加载80%平均速率(Kbps)".equals(key) || "加载80%的平均速率".equals(key) || "加载80%平均速率".equals(key) || "80% Avg Speed".equals(key) || "80% Avg Speed(Kbps)".equals(key)){
				eighty_rate = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("平均时长(ms)".equals(key) || "平均时延".equals(key) || "平均时延(ms)".equals(key) || "Avg Latency(ms)".equals(key) || "Avg Time(ms)".equals(key) || "Latency".equals(key) || "Delay".equals(key) || "Delay(ms)".equals(key)){
				full_complete = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("成功率".equals(key) || "success rate".equals(key.toLowerCase())){
				success_rate = (String)(dataMap.get(key)==null?"":dataMap.get(key));
			}else if("成功次数".equals(key) || "success counts".equals(key.toLowerCase()) || "success count".equals(key.toLowerCase()) ){
				success_counts = (String)(dataMap.get(key)==null?"":dataMap.get(key));
			}else if("网络类型".equals(key) || "网络制式".equals(key) || "network type".equals(key.toLowerCase())){
				net_type = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("SINR".equals(key)){
				sinr = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("测试描述".equals(key) || "TestDescription".equals(key.toLowerCase())){
				test_description = (String) (dataMap.get(key)==null?"":dataMap.get(key));
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
			}else if("接入运营商".equals(key)){
				connect_operator = (String)(dataMap.get(key)==null?"":dataMap.get(key));
			}else if("测试地点".equals(key)){
				test_location = (String)(dataMap.get(key)==null?"":dataMap.get(key));
			}
		}
		
		map.put("connect_operator", connect_operator);
		map.put("test_location",test_location );
		map.put("reference", reference);
		map.put("logversion", logversion);
		map.put("model",model);
		map.put("web_sit",web_sit);
		map.put("eighty_loading",eighty_loading);
		map.put("eighty_rate",eighty_rate);
		map.put("full_complete",full_complete);	
		map.put("success_rate", success_rate);
		map.put("success_counts", success_counts.isEmpty()?0:success_counts);
		
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
			sinr = sinr1;
			signal_strength = signal_strength1;
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
		map.put("test_description", test_description);
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
		return appendSql(map,numType,file_index,testtime);
	}
	/**
	 * 插入VideoTest数据
	 */
	public  Map<StringBuffer, Boolean> insertVideoTest(Map dataMap,String keyspace,String numType,String detailreport,String testtime){
		List nameList = new ArrayList();
		List valueList = new ArrayList();
		Map map = new HashMap();
		Set keySet = dataMap.keySet();
		Iterator iter = keySet.iterator();
		Format fm = new DecimalFormat("#.###");
		
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
		String appid = dataMap.get("appid")==null?"":dataMap.get("appid").toString();//appid
		
		map.put("appid", appid);
		map.put("device_org", device_org);
		map.put("time", time);
		
		double avg_delay = 0.0;
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
		String test_description = "";
		String net_type1 = "";
		String signal_strength1 = "";
		String sinr1 = "";
		String cell1 = "";
		String net_type2 = "";
		String signal_strength2 = "";
		String sinr2 = "";
		String cell2 = "";
		String wifi_ss = "";//WIFI SIGNAL STRENGTH
		
		String test_location = "";
		String connect_operator = "";
		
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
		if(longitude.isEmpty() || latitude.isEmpty()){
			
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
			}else if("信号强度".equals(key) || "Signal Strength".equals(key)){
				signal_strength = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("终端类型".equals(key) || "终端型号".equals(key) || "device type".equals(key.toLowerCase()) || "terminal model".equals(key.toLowerCase()) || "terminal type".equals(key.toLowerCase()) || "terminal mode".equals(key.toLowerCase()) || "device model".equals(key.toLowerCase()) || "terminal models".equals(key.toLowerCase())){
				model = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("地址".equals(key) || "address".equals(key.toLowerCase())){
				address = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("结束时间".equals(key) || "end time".equals(key.toLowerCase())){
				end_time = (String) (dataMap.get(key)==null?"":dataMap.get(key));
				String dataLong = "";
				String datatimeStr = end_time;
				Date date1 = null;
				Pattern pattern2 = Pattern.compile("\\d+");
				try {
					if(datatimeStr==null || datatimeStr.isEmpty()){
						dataLong = "";	
					}else{
						String timeStrs[] = pattern2.split(datatimeStr);
						StringBuffer rex = new StringBuffer("");
						for (int j = 0; j < timeStrs.length; j++) {
							rex.append(timeStrs[j]);
							rex.append(dateStr[j]);
						}
						SimpleDateFormat formatter = new SimpleDateFormat(
								rex.toString());
						date1 = formatter.parse(datatimeStr);	
						dataLong = Long.toString(date1.getTime());	
						end_time = dataLong;
					}
				} catch (ParseException e) {
					e.printStackTrace();
					System.out.println("转换时间有误");
				}
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
						if(delays.length>0 && sum>0){
							avg_delay = sum/delays.length;
						}else{
							avg_delay = 0.0;
						}
					} catch (Exception e) {
						System.out.println("时延有误");
					}
				}else{
					avg_delay = Double.parseDouble(delay);
				}
			}else if("测试类型".equals(key) || "test type".equals(key.toLowerCase())){
				test_type = (String)(dataMap.get(key)==null?"":dataMap.get(key));
			}else if("缓冲次数".equals(key) || "buffer counts".equals(key.toLowerCase())){
				buffer_times = (String)(dataMap.get(key)==null?"":dataMap.get(key));
			}else if("测试时长(ms)".equals(key) || "duration(ms)".equals(key.toLowerCase())){
				test_times = (String)(dataMap.get(key)==null?"":dataMap.get(key));
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
			}else if("测试描述".equals(key) || "TestDescription".equals(key.toLowerCase())){
				test_description = (String) (dataMap.get(key)==null?"":dataMap.get(key));
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
			}else if("接入运营商".equals(key)){
				connect_operator = (String)(dataMap.get(key)==null?"":dataMap.get(key));
			}else if("测试地点".equals(key)){
				test_location = (String)(dataMap.get(key)==null?"":dataMap.get(key));
			}
		}
		
		map.put("connect_operator", connect_operator);
		map.put("test_location",test_location );

		map.put("logversion", logversion);
		map.put("model",model);
		map.put("address", address);
		map.put("end_time", end_time);
		map.put("avg_delay", fm.format(avg_delay));
		map.put("delay",delay);	
		map.put("test_type", test_type);
		map.put("buffer_times", buffer_times);
		map.put("test_times", test_times);
		
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
			sinr = sinr1;
			signal_strength = signal_strength1;
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
		map.put("test_description", test_description);
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
		return appendSql(map,numType,file_index,testtime);
	}
	/**
	 * 插入HTTP数据
	 */
	public  Map<StringBuffer, Boolean> insertHTTP(Map dataMap,String keyspace,String numType,String detailreport,String testtime){
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
		String appid = dataMap.get("appid")==null?"":dataMap.get("appid").toString();//appid
		
		map.put("appid", appid);
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
		String test_description = "";
		String net_type1 = "";
		String signal_strength1 = "";
		String sinr1 = "";
		String cell1 = "";
		String net_type2 = "";
		String signal_strength2 = "";
		String sinr2 = "";
		String cell2 = "";
		String wifi_ss = "";//WIFI SIGNAL STRENGTH
		
		String test_location = "";
		String connect_operator = "";
		
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
		if(longitude.isEmpty() || latitude.isEmpty()){
			return new HashMap<StringBuffer, Boolean>();
		}else{
			System.out.println(longitude+" ::latitude "+latitude);
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
			}else if("Max Speed(Kbps)".equals(key) || "Max Speed(Mbps)".equals(key) || "最大速率(Mbps)".equals(key) || "最大速率(Kbps)".equals(key) || "最大速率（Mbps）".equals(key) || "最大速率（Kbps）".equals(key)){
				max_rate = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("平均时长(ms)".equals(key) || "平均时延".equals(key) || "平均时延(ms)".equals(key) || "Avg Latency(ms)".equals(key) || "Avg Time(ms)".equals(key) || "Latency".equals(key) || "Delay".equals(key) || "Delay(ms)".equals(key)){
				avg_latency = (String)(dataMap.get(key)==null?"":dataMap.get(key));
			}else if("logversion".equals(key.toLowerCase()) || "日志版本".equals(key)){
				logversion = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("信号强度".equals(key) || "Signal Strength".equals(key)){
				signal_strength = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("终端类型".equals(key) || "终端型号".equals(key) || "device type".equals(key.toLowerCase()) || "terminal model".equals(key.toLowerCase()) || "terminal type".equals(key.toLowerCase()) || "terminal mode".equals(key.toLowerCase()) || "device model".equals(key.toLowerCase()) || "terminal models".equals(key.toLowerCase())){
				model = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("类型".equals(key) || "Type".equals(key) || "Test Type".equals(key) || "测试类型".equals(key)){
				file_type = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("文件".equals(key) || "File".equals(key)){
				url = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("测试文件".equals(key) || "Test File".equals(key)){
				testfileurl = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("文件大小".equals(key) || "File Size".equals(key) || "File Size(MB)".equals(key) || "文件大小(MB)".equals(key)){
				resource_size = (String) (dataMap.get(key)==null?"":dataMap.get(key));
//				if(resource_size.equals("") || resource_size.equals("--") || !resource_size.contains(".")){
//				}else
//				{
//					map.put("resource_size",resource_size.substring(resource_size.lastIndexOf("/")+1, resource_size.lastIndexOf(".")));
//				}
			}else if("成功率".equals(key) || "success rate".equals(key.toLowerCase())){
				success_rate = (String)(dataMap.get(key)==null?"":dataMap.get(key));
			}else if("时长(s)".equals(key) || "时长（s）".equals(key) || "时长(ms)".equals(key) || "duration".equals(key.toLowerCase()) || "duration(ms)".equals(key.toLowerCase())){
				duration = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("网络类型".equals(key) || "网络制式".equals(key) || "network type".equals(key.toLowerCase())){
				net_type = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("SINR".equals(key)){
				sinr = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("测试描述".equals(key) || "TestDescription".equals(key.toLowerCase())){
				test_description = (String) (dataMap.get(key)==null?"":dataMap.get(key));
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
			}else if("接入运营商".equals(key)){
				connect_operator = (String)(dataMap.get(key)==null?"":dataMap.get(key));
			}else if("测试地点".equals(key)){
				test_location = (String)(dataMap.get(key)==null?"":dataMap.get(key));
			}
		}
		
		map.put("connect_operator", connect_operator);
		map.put("test_location",test_location );
		
		map.put("avg_rate",avg_rate);
		map.put("max_rate", max_rate);
		map.put("avg_latency",avg_latency);
		map.put("logversion", logversion);
		map.put("model",model);
		map.put("file_type", file_type);
		map.put("resource_size",resource_size);
		map.put("success_rate", success_rate);
		map.put("duration",duration);
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
			sinr = sinr1;
			signal_strength = signal_strength1;
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
		map.put("test_description", test_description);
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
		return appendSql(map,numType,file_index,testtime);
	}
	/**
	 * 插入PING数据
	 */
	public  Map<StringBuffer, Boolean> insertPINGManyToOne(Map allMap,String keyspace,String numType,String detailreport,String testtime){
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
			String appid = dataMap.get("appid")==null?"":dataMap.get("appid").toString();//appid
			
			map.put("appid", appid);
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
			String test_description = "";
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
			if(longitude.isEmpty() || latitude.isEmpty()){
				
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
					map.clear();
					continue;
				}
			}
			if(dataMap.containsKey("success rate")){
				success_rate_filter = dataMap.get("success rate")==null?"":dataMap.get("success rate").toString();
				if(success_rate_filter.equals("0") || success_rate_filter.equals("0%")){
					map.clear();
					continue;
				}
			}
			if(dataMap.containsKey("Success Rate")){
				success_rate_filter = dataMap.get("Success Rate")==null?"":dataMap.get("Success Rate").toString();
				if(success_rate_filter.equals("0") || success_rate_filter.equals("0%")){
					map.clear();
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
				}else if("信号强度".equals(key) || "Signal Strength".equals(key)){
					signal_strength = (String) (dataMap.get(key)==null?"":dataMap.get(key));
				}else if("终端类型".equals(key) || "终端型号".equals(key) || "device type".equals(key.toLowerCase()) || "terminal model".equals(key.toLowerCase()) || "terminal type".equals(key.toLowerCase()) || "terminal mode".equals(key.toLowerCase()) || "device model".equals(key.toLowerCase()) || "terminal models".equals(key.toLowerCase())){
					model = (String)(dataMap.get(key)==null?"":dataMap.get(key));
				}else if("地址".equals(key) || "Address".equals(key) || "address".equals(key)){
					domain_address = (String)(dataMap.get(key)==null?"":dataMap.get(key));
				}else if("平均时延(ms)".equals(key) || "平均时延".equals(key) || "average delay(ms)".equals(key.toLowerCase()) || "avg latency(ms)".equals(key.toLowerCase()) || "avg time(ms)".equals(key.toLowerCase()) || "avg latency".equals(key.toLowerCase()) || "delay".equals(key.toLowerCase()) || "delay(ms)".equals(key.toLowerCase())){
					avg_rtt = (String)(dataMap.get(key)==null?"":dataMap.get(key));
				}else if("the minimum delay(ms)".equals(key.trim().toLowerCase()) ||"最大时延(ms)".equals(key)||"最大时延".equals(key) || "max latency(ms)".equals(key.toLowerCase()) || "max latency".equals(key.toLowerCase())){
					max_rtt = (String)(dataMap.get(key)==null?"":dataMap.get(key));
				}else if("the maximum delay(ms)".equals(key.trim().toLowerCase()) || "最小时延(ms)".equals(key)||"最小时延".equals(key) || "min latency(ms)".equals(key.toLowerCase()) || "min latency".equals(key.toCharArray())){
					min_rtt = (String)(dataMap.get(key)==null?"":dataMap.get(key));
				}else if("测试次数".equals(key)||"重复次数".equals(key) || "repetition".equals(key.toLowerCase()) || "number of repetitions".equals(key.toLowerCase()) || "Test Counts".equals(key)){
					times = (String)(dataMap.get(key)==null?"":dataMap.get(key));
				}else if("packet_loss_rate".equals(key.toLowerCase()) || "丢包率".equals(key)){
					packet_loss_rate = (String)(dataMap.get(key)==null?"":dataMap.get(key));
				}else if("成功率".equals(key) || "success rate".equals(key.toLowerCase())){
					success_rate = (String)(dataMap.get(key)==null?"":dataMap.get(key));
					if(success_rate.contains("%")){
						success_rate = success_rate.replaceAll("%", "");
					}
				}else if("网络类型".equals(key) || "网络制式".equals(key) || "network type".equals(key.toLowerCase())){
					net_type = (String) (dataMap.get(key)==null?"":dataMap.get(key));
				}else if("SINR".equals(key)){
					sinr = (String) (dataMap.get(key)==null?"":dataMap.get(key));
				}else if("测试描述".equals(key) || "TestDescription".equals(key.toLowerCase())){
					test_description = (String) (dataMap.get(key)==null?"":dataMap.get(key));
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

			map.put("logversion", logversion);
			map.put("model",model);
			if(map.containsKey("domain_address") && !map.get("domain_address").toString().isEmpty()){
				map.put("domain_address",map.get("domain_address").toString()+","+domain_address);						
			}else {
				map.put("domain_address","Overall,"+domain_address);
			}
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
			map.put("times",times.isEmpty()?0:times);
			map.put("packet_loss_rate",packet_loss_rate);
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
				/*if(lac1 != null){
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
				}*/
				
				if(lac1 != null){
					lac_tac = lac1;
				}
				if(cid1 != null){
					cid_pci = cid1;
				}
				sinr = sinr1;
				signal_strength = signal_strength1;
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
			map.put("test_description", test_description);
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
		return appendSql(map,numType,file_index,testtime);
	}
	/**
	 * 插入PING数据
	 */
	public  Map<StringBuffer, Boolean> insertPING(Map dataMap,String keyspace,String numType,String detailreport,String testtime){
		List nameList = new ArrayList();
		List valueList = new ArrayList();
		Map map = new HashMap();
		Set keySet = dataMap.keySet();
		Iterator iter = keySet.iterator();
		String imei = (String)(dataMap.get("imei")==null?"":dataMap.get("imei"));
		String file_path = (String)(dataMap.get("filePath")==null?"":dataMap.get("filePath"));
		String detailreportvalue = (String)(dataMap.get("detailreport")==null?"":dataMap.get("detailreport"));
		String appid = dataMap.get("appid")==null?"":dataMap.get("appid").toString();//appid
		
		map.put("appid", appid);
		map.put("detailreport", detailreport);
		map.put("imei",imei);
		map.put("file_path",file_path.replace("\\", "|").replace("//", "|"));

		System.out.println(keyspace+"       insertPINGdevice_org");

		
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
		String packet_loss_rate = "";//丢包率
		
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
		String test_description = "";
		String net_type1 = "";
		String signal_strength1 = "";
		String sinr1 = "";
		String cell1 = "";
		String net_type2 = "";
		String signal_strength2 = "";
		String sinr2 = "";
		String cell2 = "";
		String wifi_ss = "";//WIFI SIGNAL STRENGTH
		
		String test_location = "";
		String connect_operator = "";
		
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
		
		
		if(longitude.isEmpty() || latitude.isEmpty()){
			
		}else{
			try {
				double[] em = Earth2Mars.transform(Double.parseDouble(longitude), Double.parseDouble(latitude));
				double[] bf = BdFix.bd_encrypt(em[0], em[1]);
				longitudeNum = bf[0];
				latitudeNum = bf[1];
			} catch (java.lang.NumberFormatException e) {
				System.err.println("经纬度错误");
			}
			
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
			}else if("信号强度".equals(key) || "Signal Strength".equals(key)){
				signal_strength = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("终端类型".equals(key) || "终端型号".equals(key) || "device type".equals(key.toLowerCase()) || "terminal model".equals(key.toLowerCase()) || "terminal type".equals(key.toLowerCase()) || "terminal mode".equals(key.toLowerCase()) || "device model".equals(key.toLowerCase()) || "terminal models".equals(key.toLowerCase())){
				model = (String)(dataMap.get(key)==null?"":dataMap.get(key));
			}else if("地址".equals(key) || "Address".equals(key) || "address".equals(key)){
				domain_address = (String)(dataMap.get(key)==null?"":dataMap.get(key));
			}else if("平均时延(ms)".equals(key) || "平均时延".equals(key) || "average delay(ms)".equals(key.toLowerCase()) || "avg latency(ms)".equals(key.toLowerCase()) || "avg time(ms)".equals(key.toLowerCase()) || "avg latency".equals(key.toLowerCase()) || "delay".equals(key.toLowerCase()) || "delay(ms)".equals(key.toLowerCase())){
				avg_rtt = (String)(dataMap.get(key)==null?"":dataMap.get(key));
			}else if("the minimum delay(ms)".equals(key.trim().toLowerCase()) ||"最大时延(ms)".equals(key)||"最大时延".equals(key) || "max latency(ms)".equals(key.toLowerCase()) || "max latency".equals(key.toLowerCase())){
				max_rtt = (String)(dataMap.get(key)==null?"":dataMap.get(key));
			}else if("the maximum delay(ms)".equals(key.trim().toLowerCase()) || "最小时延(ms)".equals(key)||"最小时延".equals(key) || "min latency(ms)".equals(key.toLowerCase()) || "min latency".equals(key.toCharArray())){
				min_rtt = (String)(dataMap.get(key)==null?"":dataMap.get(key));
			}else if("测试次数".equals(key)||"重复次数".equals(key) || "repetition".equals(key.toLowerCase()) || "number of repetitions".equals(key.toLowerCase()) || "Test Counts".equals(key)){
				times = (String)(dataMap.get(key)==null?"":dataMap.get(key));
			}else if("packet_loss_rate".equals(key.toLowerCase()) || "丢包率".equals(key)){
				packet_loss_rate = (String)(dataMap.get(key)==null?"":dataMap.get(key));
			}else if("成功率".equals(key) || "success rate".equals(key.toLowerCase())){
				success_rate = (String)(dataMap.get(key)==null?"":dataMap.get(key));
				if(success_rate.equals("0") || success_rate.equals("0%")){
					return null;
				}
			}else if("网络类型".equals(key) || "网络制式".equals(key) || "network type".equals(key.toLowerCase())){
				net_type = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("SINR".equals(key)){
				sinr = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("测试描述".equals(key) || "TestDescription".equals(key.toLowerCase())){
				test_description = (String) (dataMap.get(key)==null?"":dataMap.get(key));
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
			}else if("接入运营商".equals(key)){
				connect_operator = (String)(dataMap.get(key)==null?"":dataMap.get(key));
			}else if("测试地点".equals(key)){
				test_location = (String)(dataMap.get(key)==null?"":dataMap.get(key));
			}
		}
		
		map.put("connect_operator", connect_operator);
		map.put("test_location",test_location );
		
		map.put("logversion", logversion);
		map.put("model",model);
		map.put("domain_address",domain_address);
		map.put("avg_rtt",avg_rtt);
		map.put("max_rtt",max_rtt);
		map.put("min_rtt",min_rtt);
		map.put("times",times.isEmpty()?0:times);
		map.put("packet_loss_rate",packet_loss_rate);
		map.put("success_rate", success_rate);
		
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
			sinr = sinr1;
			signal_strength = signal_strength1;
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
		map.put("test_description", test_description);
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
		return appendSql(map,numType,file_index,testtime);
	}
	/**
	 * 插入call数据
	 */
	public  Map<StringBuffer, Boolean> insertCall(Map dataMap,String keyspace,String numType,String detailreport,String testtime){
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
		String appid = dataMap.get("appid")==null?"":dataMap.get("appid").toString();//appid
		
		map.put("appid", appid);
		map.put("device_org", device_org);
		map.put("time", time);
		
		String continuing_time = "";//接续时长
		String connecting_time = "";//接通时长
		String connected_time = "";//通话时长
		String recording = "";
		String phone_number = "";
		String target_number = "";
//		String network = "";
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
		String test_description = "";
		String net_type1 = "";
		String signal_strength1 = "";
		String sinr1 = "";
		String cell1 = "";
		String net_type2 = "";
		String signal_strength2 = "";
		String sinr2 = "";
		String cell2 = "";
		String wifi_ss = "";//WIFI SIGNAL STRENGTH
		
		String test_location = "";
		String connect_operator = "";
		
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
		if(longitude.isEmpty() || latitude.isEmpty()){
			
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
			}else if("信号强度".equals(key) || "Signal Strength".equals(key)){
				signal_strength = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("终端类型".equals(key) || "终端型号".equals(key) || "device type".equals(key.toLowerCase()) || "terminal model".equals(key.toLowerCase()) || "terminal type".equals(key.toLowerCase()) || "terminal mode".equals(key.toLowerCase()) || "device model".equals(key.toLowerCase()) || "terminal models".equals(key.toLowerCase())){
				model = (String)(dataMap.get(key)==null?"":dataMap.get(key));
			}else if("是否成功".equals(key) || "success or not".equals(key.toLowerCase())){
				success_failure = (String)(dataMap.get(key)==null?"":dataMap.get(key));
			}else if("接续时长(ms)".equals(key) || "Continuing Time(ms)".equals(key)){
				continuing_time = (String)(dataMap.get(key)==null?"":dataMap.get(key));
			}else if("接通时长(ms)".equals(key) || "Connecting time(ms)".equals(key)){
				connecting_time = (String)(dataMap.get(key)==null?"":dataMap.get(key));
			}else if("通话时长(s)".equals(key) || "Connected time(s)".equals(key)){
				connected_time = (String)(dataMap.get(key)==null?"":dataMap.get(key));
			}else if("手机号码".equals(key) || "Phone Number".equals(key)){
				target_number = (String)(dataMap.get(key)==null?"":dataMap.get(key));
			}else if("网络类型".equals(key) || "网络制式".equals(key) || "network type".equals(key.toLowerCase())){
				net_type = (String)(dataMap.get(key)==null?"":dataMap.get(key));
			}else if("fallbackToCs(ms)".equals(key) || "FallBack To CS(s)".equals(key)){
				hand_over_situation = (String)(dataMap.get(key)==null?"":dataMap.get(key));
				fallbacktocs = (String)(dataMap.get(key)==null?"":dataMap.get(key));
			}else if("backToLte(ms)".equals(key) || "Back To Lte(ms)".equals(key)){
				backtolte = (String)(dataMap.get(key)==null?"":dataMap.get(key));
			}else if("是否录音".equals(key) || "Call Recording".equals(key)){
				recording = (String)(dataMap.get(key)==null?"":dataMap.get(key));
			}else if("成功率".equals(key) || "success rate".equals(key.toLowerCase())){
				success_rate = (String)(dataMap.get(key)==null?"":dataMap.get(key));
//			}else if("网络".equals(key) || "Network".equals(key)){
//				net_type = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("SINR".equals(key)){
				sinr = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("测试描述".equals(key) || "TestDescription".equals(key.toLowerCase())){
				test_description = (String) (dataMap.get(key)==null?"":dataMap.get(key));
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
			}else if("接入运营商".equals(key)){
				connect_operator = (String)(dataMap.get(key)==null?"":dataMap.get(key));
			}else if("测试地点".equals(key)){
				test_location = (String)(dataMap.get(key)==null?"":dataMap.get(key));
			}
		}
		
		map.put("connect_operator", connect_operator);
		map.put("test_location",test_location );
		
		map.put("logversion", logversion);
		map.put("model",model);
		map.put("success_failure",success_failure);
		map.put("continuing_time",continuing_time);
		map.put("connecting_time",connecting_time);
		map.put("connected_time",connected_time);
		map.put("target_number",target_number);
//		map.put("network",network);
		map.put("hand_over_situation", hand_over_situation);
		map.put("recording",recording);
		map.put("success_rate", success_rate);
		
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
			sinr = sinr1;
			signal_strength = signal_strength1;
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
		map.put("test_description", test_description);
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
		return appendSql(map,numType,file_index,testtime);
	}
	/**
	 * 插入ResideTest数据
	 */
	public  Map<StringBuffer, Boolean> insertResideTest(Map dataMap,String keyspace,String numType,String detailreport,String testtime){
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
		String appid = dataMap.get("appid")==null?"":dataMap.get("appid").toString();//appid
		
		map.put("appid", appid);
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
		String test_description = "";
		String net_type1 = "";
		String signal_strength1 = "";
		String sinr1 = "";
		String cell1 = "";
		String net_type2 = "";
		String signal_strength2 = "";
		String sinr2 = "";
		String cell2 = "";
		String wifi_ss = "";//WIFI SIGNAL STRENGTH
		
		String test_location = "";
		String connect_operator = "";
		
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
		if(longitude.isEmpty() || latitude.isEmpty()){
			
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
			}else if("信号强度".equals(key) || "Signal Strength".equals(key)){
				signal_strength = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("终端类型".equals(key) || "终端型号".equals(key) || "device type".equals(key.toLowerCase()) || "terminal model".equals(key.toLowerCase()) || "terminal type".equals(key.toLowerCase()) || "terminal mode".equals(key.toLowerCase()) || "device model".equals(key.toLowerCase()) || "terminal models".equals(key.toLowerCase())){
				model = (String)(dataMap.get(key)==null?"":dataMap.get(key));
			}else if("切换重选时延(ms)".equals(key) || "平均时长(ms)".equals(key) || "平均时延".equals(key) || "平均时延(ms)".equals(key) || "Avg Latency(ms)".equals(key) || "Avg Time(ms)".equals(key) || "Latency".equals(key) || "Latency(ms)".equals(key) || "Delay".equals(key) || "Delay(ms)".equals(key) || "Handover Latency".equals(key) ){
				avg_latency = (String)(dataMap.get(key)==null?"":dataMap.get(key));
			}else if("网络类型".equals(key) || "网络制式".equals(key) || "network type".equals(key.toLowerCase())){
				net_type = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("SINR".equals(key)){
				sinr = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("测试描述".equals(key) || "TestDescription".equals(key.toLowerCase())){
				test_description = (String) (dataMap.get(key)==null?"":dataMap.get(key));
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
			}else if("接入运营商".equals(key)){
				connect_operator = (String)(dataMap.get(key)==null?"":dataMap.get(key));
			}else if("测试地点".equals(key)){
				test_location = (String)(dataMap.get(key)==null?"":dataMap.get(key));
			}
		}
		
		map.put("connect_operator", connect_operator);
		map.put("test_location",test_location );
		
		map.put("logversion", logversion);
		map.put("model",model);
		map.put("avg_latency",avg_latency);
		
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
			sinr = sinr1;
			signal_strength = signal_strength1;
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
		map.put("test_description", test_description);
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
		return appendSql(map,numType,file_index,testtime);
	}
	
	/**
	 * 插入SMS数据
	 */
	public  Map<StringBuffer, Boolean>  insertSMS(Map dataMap,String keyspace,String numType,String detailreport,String testtime){
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
		String appid = dataMap.get("appid")==null?"":dataMap.get("appid").toString();//appid
		map.put("appid", appid);
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
		String test_description = "";
		String net_type1 = "";
		String signal_strength1 = "";
		String sinr1 = "";
		String cell1 = "";
		String net_type2 = "";
		String signal_strength2 = "";
		String sinr2 = "";
		String cell2 = "";
		String wifi_ss = "";//WIFI SIGNAL STRENGTH
		

		String test_location = "";
		String connect_operator = "";
		
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
		if(longitude.isEmpty() || latitude.isEmpty()){
			
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
			}else if("信号强度".equals(key) || "signal strength".equals(key.toLowerCase())){
				signal_strength = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("发送内容".equals(key) || "content".equals(key.toLowerCase())){
				content = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("终端类型".equals(key) || "终端型号".equals(key) || "device type".equals(key.toLowerCase()) || "terminal model".equals(key.toLowerCase()) || "terminal type".equals(key.toLowerCase()) || "terminal mode".equals(key.toLowerCase()) || "device model".equals(key.toLowerCase()) || "terminal models".equals(key.toLowerCase())){
				model = (String)(dataMap.get(key)==null?"":dataMap.get(key));
			}else if("时延(ms)".equals(key) || "delay(ms)".equals(key.toLowerCase())){
				sending_delay = (String)(dataMap.get(key)==null?"":dataMap.get(key));
			}else if("手机号码".equals(key) || "phone number".equals(key.toLowerCase())){
				target_number = (String)(dataMap.get(key)==null?"":dataMap.get(key));
			}else if("测试次数".equals(key) || "test times".equals(key.trim().toLowerCase()) || "send sms counts".equals(key.toLowerCase())){
				test_times = (String)(dataMap.get(key)==null?"":dataMap.get(key));
			}else if("成功率".equals(key) || "success rate".equals(key.toLowerCase())){
				success_rate = (String)(dataMap.get(key)==null?"":dataMap.get(key));
			}else if("成功次数".equals(key) || "succ counts".equals(key.toLowerCase())){
				success_times = (String)(dataMap.get(key)==null?"":dataMap.get(key));
			}else if("是否成功".equals(key) || "success or not".equals(key.toLowerCase())){
				success_failure = (String)(dataMap.get(key)==null?"":dataMap.get(key));
			}else if("网络类型".equals(key) || "网络制式".equals(key) || "network type".equals(key.toLowerCase())){
				net_type = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("SINR".equals(key)){
				sinr = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("测试描述".equals(key) || "TestDescription".equals(key.toLowerCase())){
				test_description = (String) (dataMap.get(key)==null?"":dataMap.get(key));
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
			}else if("接入运营商".equals(key)){
				connect_operator = (String)(dataMap.get(key)==null?"":dataMap.get(key));
			}else if("测试地点".equals(key)){
				test_location = (String)(dataMap.get(key)==null?"":dataMap.get(key));
			}
		}
		
		map.put("connect_operator", connect_operator);
		map.put("test_location",test_location );
		
		map.put("logversion", logversion);
		if(content.getBytes().length>70){
			testtype = "long";
			map.put("testtype", testtype);
		}else{
			testtype = "short";
			map.put("testtype", testtype);
		}
		map.put("model",model);
		map.put("sending_delay", sending_delay);
		map.put("target_number", target_number);
		map.put("test_times", test_times);
		map.put("success_rate", success_rate);
		map.put("success_times", success_times);
		map.put("success_failure",success_failure);
		
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
			sinr = sinr1;
			signal_strength = signal_strength1;
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
		map.put("test_description", test_description);
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
		return appendSql(map,numType,file_index,testtime);
	}
	
	/**
	 * 插入短信查询数据
	 */
	public  Map<StringBuffer, Boolean>  insertSMSQuery(Map dataMap,String keyspace,String numType,String detailreport,String testtime){
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
		String appid = dataMap.get("appid")==null?"":dataMap.get("appid").toString();//appid
		map.put("appid", appid);
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
		String sms_send_delay = "";//短信发送时延
		String sms_query_delay = "";//短信接收时延
		String sms_receive_num = "";//接收号码
		
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
		String test_description = "";
		String net_type1 = "";
		String signal_strength1 = "";
		String sinr1 = "";
		String cell1 = "";
		String net_type2 = "";
		String signal_strength2 = "";
		String sinr2 = "";
		String cell2 = "";
		String wifi_ss = "";//WIFI SIGNAL STRENGTH
		

		String test_location = "";
		String connect_operator = "";
		
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
		if(longitude.isEmpty() || latitude.isEmpty()){
			
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
			}else if("信号强度".equals(key) || "signal strength".equals(key.toLowerCase())){
				signal_strength = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("发送内容".equals(key) || "content".equals(key.toLowerCase())){
				content = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("终端类型".equals(key) || "终端型号".equals(key) || "device type".equals(key.toLowerCase()) || "terminal model".equals(key.toLowerCase()) || "terminal type".equals(key.toLowerCase()) || "terminal mode".equals(key.toLowerCase()) || "device model".equals(key.toLowerCase()) || "terminal models".equals(key.toLowerCase())){
				model = (String)(dataMap.get(key)==null?"":dataMap.get(key));
			}else if("时延(ms)".equals(key) || "delay(ms)".equals(key.toLowerCase())){
				sending_delay = (String)(dataMap.get(key)==null?"":dataMap.get(key));
			}else if("手机号码".equals(key) || "phone number".equals(key.toLowerCase())){
				target_number = (String)(dataMap.get(key)==null?"":dataMap.get(key));
			}else if("测试次数".equals(key) || "test times".equals(key.trim().toLowerCase()) || "send sms counts".equals(key.toLowerCase())){
				test_times = (String)(dataMap.get(key)==null?"":dataMap.get(key));
			}else if("成功率".equals(key) || "success rate".equals(key.toLowerCase())){
				success_rate = (String)(dataMap.get(key)==null?"":dataMap.get(key));
			}else if("成功次数".equals(key) || "succ counts".equals(key.toLowerCase())){
				success_times = (String)(dataMap.get(key)==null?"":dataMap.get(key));
			}else if("是否成功".equals(key) || "success or not".equals(key.toLowerCase())){
				success_failure = (String)(dataMap.get(key)==null?"":dataMap.get(key));
			}else if("短信发送时延(ms)".equals(key) || "sending latency(ms)".equals(key.toLowerCase()) || "sending delay(ms)".equals(key.toLowerCase())){
				sms_send_delay = (String)(dataMap.get(key)==null?"":dataMap.get(key));
			}else if("短信查询时延(ms)".equals(key) || "receiving latency(ms)".equals(key.toLowerCase()) || "sms query time(ms)".equals(key.toLowerCase())){
				sms_query_delay = (String)(dataMap.get(key)==null?"":dataMap.get(key));
			}else if("接收短信号码".equals(key) || "Receiving Number".equals(key.toLowerCase()) || "sms received phone".equals(key.toLowerCase())){
				sms_receive_num = (String)(dataMap.get(key)==null?"":dataMap.get(key));
			}else if("网络类型".equals(key) || "网络制式".equals(key) || "network type".equals(key.toLowerCase())){
				net_type = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("SINR".equals(key)){
				sinr = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("测试描述".equals(key) || "TestDescription".equals(key.toLowerCase())){
				test_description = (String) (dataMap.get(key)==null?"":dataMap.get(key));
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
			}else if("接入运营商".equals(key)){
				connect_operator = (String)(dataMap.get(key)==null?"":dataMap.get(key));
			}else if("测试地点".equals(key)){
				test_location = (String)(dataMap.get(key)==null?"":dataMap.get(key));
			}
		}
		
		map.put("connect_operator", connect_operator);
		map.put("test_location",test_location );
		
		map.put("logversion", logversion);
		if(content.getBytes().length>70){
			testtype = "long";
			map.put("testtype", testtype);
		}else{
			testtype = "short";
			map.put("testtype", testtype);
		}
		map.put("model",model);
		map.put("sending_delay", sending_delay);
		map.put("target_number", target_number);
		map.put("test_times", test_times);
		map.put("success_rate", success_rate);
		map.put("success_times", success_times);
		map.put("success_failure",success_failure);
		map.put("sms_send_delay",sms_send_delay);
		map.put("sms_query_delay",sms_query_delay);
		map.put("sms_receive_num",sms_receive_num);
		
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
			sinr = sinr1;
			signal_strength = signal_strength1;
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
		map.put("test_description", test_description);
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
		return appendSql(map,numType,file_index,testtime);
	}
	
	private Map<StringBuffer, Boolean> appendSql (Map map,String numType,String fileIndex,String testtime){
		StringBuffer sql = new StringBuffer("");
		StringBuffer columnStr = new StringBuffer("(");
		StringBuffer valueStr = new StringBuffer(" values (");
		StringBuffer updateStr = new StringBuffer("");
		String table = "";
		String datatype = "";
		String issubmeter= ConfParser.issubmeter;
		/**
		 * 2015年12月11日 15:56:47修改
		 * 添加是否分表开关 若issubmeter=no 则不分表，其他情况都视为分表情况
		 */
		/*if(issubmeter.equals("no")){
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
			}else if(numType.equals("05005")){
				table = "sms_query";
			}else {
				return new HashMap();
			}
		}else{
		
		}*/
			if(numType.equals("01001")){
				table = "speed_test_"+testtime;
				datatype = "speed_test";
			}else if(numType.equals("02001")){
				table = "web_browsing_"+testtime;
				datatype = "web_browsing";
			}else if(numType.equals("02011")){
				table = "video_test_"+testtime;
				datatype = "video_test";
			}else if(numType.equals("03001") || numType.equals("03011")){
				table = "http_test_"+testtime;
				datatype = "http";
			}else if(numType.equals("04002")){
				table = "ping_"+testtime;
				datatype = "ping";
			}else if(numType.equals("04006")){
				table = "hand_over_"+testtime;
				datatype = "hand_over";
			}else if(numType.equals("05001")){
				table = "call_test_"+testtime;
				datatype = "call_test";
			}else if(numType.equals("05002")){
				table = "sms_"+testtime;
				datatype = "sms";
			}else if(numType.equals("05005")){
				table = "sms_query_"+testtime;
				datatype = "sms_query";
			}else {
				return new HashMap();
			}
			
			/**
			 * 创建新表
			 */
			boolean flage = queryTabExist(table);
			if(!flage){
				System.out.println(table+"    tablename");
				createTable(table);
				//ping_201507 ping_new_201507
				String newtable = table.replace(testtime, "new_"+testtime);
				System.out.println(testtime);
				boolean fl = queryTabExist(newtable);
				if(!fl){
					createNewTable(newtable);
				}
				
				boolean opFla = queryTabExist("date_tablename");
				if(!opFla){
					createDateTab("date_tablename");
				}
					boolean queryF = queryDataExist(table,newtable);
					if(!queryF){
						String tabsql = "insert into date_tablename (confdate,tablename,newtablename,datatype)" +
								" values('"+testtime+"','"+table+"','"+newtable+"','"+datatype+"')";
						String url = dsturl.substring(0, dsturl.lastIndexOf("/") + 1) + ResultsetTestData.basename;
						try {
							start(url, dstuser, dstpassword);
							insert(tabsql);
						}catch(Exception e){
							e.printStackTrace();
						}finally{
							try {
								close();
							} catch (SQLException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						}
					}
			}
			
		
		
		System.out.println("开始组装sql   查询是否存在数据************："+new Date().toLocaleString() +"   毫秒："+new Date().getTime());
		
		//boolean flag = this.queryExist(table,fileIndex);//有数据则为true 执行更新
		boolean flag = false;
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
			if(map!=null && map.size()>0){
				sql.append("insert into "+table+" ");
				try {
					map.put("haschange", "0");
					Set set = map.keySet();
					Iterator iter = set.iterator();
					while(iter.hasNext()){
						String name = (String)iter.next();
						String value = map.get(name)==null?"":map.get(name).toString();
						if(value.isEmpty()){
//							continue;
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
			}else{
				return null;
			}
		}
		System.out.println("组装完毕sql************："+new Date().toLocaleString() +"   毫秒："+new Date().getTime());
		Map resultMap = new HashMap<StringBuffer, Boolean>();
		resultMap.put(sql, flag);
		return resultMap;
	}


	/**
	 * 查询表是否存在
	 * @param table
	 * @return
	 * @return boolean
	 */
	public boolean queryTabExist(String table){
		String url = dsturl.substring(0, dsturl.lastIndexOf("/") + 1) + ResultsetTestData.basename;
		try {
			start(url, dstuser,dstpassword);
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		boolean flag = false;
		ResultSet resultSet = null;
		DatabaseMetaData databaseMetaData;
		try {
			databaseMetaData = (DatabaseMetaData) conn.getMetaData();
			resultSet = databaseMetaData.getTables(null, null, table, null);
			if (resultSet.next()) {
				flag = true;
			} else {
				flag = false;
			}
			resultSet.close();
		} catch (SQLException e) {
			flag = false;
			e.printStackTrace();
		} finally {
			if (resultSet != null) {
				try {
					resultSet.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			try {
				close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return flag;
	
	}
	
	/**
	 * 创建新表
	 * 
	 * @return void
	 */
	public void createTable(String newTableName){
		
		String sql = "";
		if(newTableName.startsWith("ping")){
			sql = "CREATE TABLE " + newTableName + " ( id int(10) NOT NULL AUTO_INCREMENT, " +
					"`device_org`  varchar(200)  DEFAULT NULL ,`gps`  varchar(200)  DEFAULT NULL ,`longitude`  varchar(200)  DEFAULT NULL ," +
					"`latitude`  varchar(200)  DEFAULT NULL ,`province`  varchar(200)  DEFAULT NULL ,`city`  varchar(200)  DEFAULT NULL ,`district`  varchar(200)  DEFAULT NULL ,`time`  decimal(20,0) NULL DEFAULT NULL ,`model`  varchar(200)  DEFAULT NULL ," +
					"`logversion`  varchar(50)  DEFAULT NULL ,`domain_address`  varchar(200)  DEFAULT NULL ,`max_rtt`  varchar(200)  DEFAULT NULL ,`min_rtt`  varchar(200)  DEFAULT NULL ,`avg_rtt`  varchar(200)  DEFAULT NULL ,`packet_loss_rate`  varchar(200)  DEFAULT NULL ," +
					"`times`  int(11) NULL DEFAULT NULL ,`location`  varchar(200)  DEFAULT NULL ,`net_type`  varchar(200)  DEFAULT NULL ,`signal_strength`  varchar(200)  DEFAULT NULL ,`sinr`  varchar(200)  DEFAULT NULL ,`lac_tac`  varchar(200)  DEFAULT NULL ,`cid_pci`  varchar(200)  DEFAULT NULL ,`imei`  varchar(200)  NOT NULL ," +
					"`file_path`  varchar(500)  NOT NULL ,`file_index`  varchar(255)  DEFAULT NULL ,`android_ios`  varchar(255)  DEFAULT NULL ,`net_type1`  varchar(200)  DEFAULT NULL ,`signal_strength1`  varchar(200)  DEFAULT NULL ,`sinr1`  varchar(200)  DEFAULT NULL ,`lac_tac1`  varchar(200)  DEFAULT NULL ,`cid_pci1`  varchar(200)  DEFAULT NULL ,`net_type2`  varchar(200)  DEFAULT NULL ,`signal_strength2`  varchar(200)  DEFAULT NULL ," +
					"`sinr2`  varchar(200)  DEFAULT NULL ,`lac_tac2`  varchar(200)  DEFAULT NULL ,`cid_pci2`  varchar(200)  DEFAULT NULL ,`haschange`  int(11) NULL DEFAULT NULL ,`detailreport`  varchar(11)  DEFAULT NULL ,`address_info`  varchar(100)  DEFAULT NULL ,`success_rate`  varchar(100)  DEFAULT NULL ," +
					"`quick_test_id`  int(200) NULL DEFAULT NULL ,`imsi`  varchar(200)  DEFAULT NULL ,`operator`  varchar(200)  DEFAULT NULL ,`appid`  varchar(200)  DEFAULT NULL ,`test_description`  varchar(200)  DEFAULT NULL ,`test_location`  varchar(200)  DEFAULT NULL ,`connect_operator`  varchar(200)  DEFAULT NULL ," +
					" PRIMARY KEY (id),INDEX `file_index` (`file_index`) USING BTREE ) CHARSET=utf8 ROW_FORMAT=COMPACT;";
		}else if(newTableName.startsWith("http_test")){
			sql = "CREATE TABLE " + newTableName + " ( id int(10) NOT NULL AUTO_INCREMENT, " +
					"`device_org`  varchar(200)  DEFAULT NULL ,`gps`  varchar(200)  DEFAULT NULL ,`longitude`  varchar(200)  DEFAULT NULL ,`latitude`  varchar(200)  DEFAULT NULL ," +
					"`province`  varchar(200)  DEFAULT NULL ,`city`  varchar(200)  DEFAULT NULL ,`district`  varchar(200)  DEFAULT NULL ,`time`  varchar(200)  DEFAULT NULL ," +
					"`model`  varchar(200)  DEFAULT NULL ,`logversion`  varchar(50)  DEFAULT NULL ,`file_type`  varchar(50)  DEFAULT NULL ,`url`  varchar(500)  DEFAULT NULL ," +
					"`resource_size`  varchar(200)  DEFAULT NULL ,`duration`  varchar(200)  DEFAULT NULL ,`avg_rate`  varchar(200)  DEFAULT NULL ," +
					"`max_rate`  varchar(200)  DEFAULT NULL COMMENT '最大速率' ,`avg_latency`  varchar(200)  DEFAULT NULL COMMENT '平均时延' ,`location`  varchar(200)  DEFAULT NULL ," +
					"`net_type`  varchar(200)  DEFAULT NULL ,`signal_strength`  varchar(200)  DEFAULT NULL ,`sinr`  varchar(200)  DEFAULT NULL ," +
					"`cid_pci`  varchar(200)  DEFAULT NULL ,`lac_tac`  varchar(200)  DEFAULT NULL ,`imei`  varchar(200)  NOT NULL ,`file_index`  varchar(255)  DEFAULT NULL ,`file_path`  varchar(500)  DEFAULT NULL ,`android_ios`  varchar(255)  DEFAULT NULL ," +
					"`net_type1`  varchar(200)  DEFAULT NULL ,`signal_strength1`  varchar(200)  DEFAULT NULL ,`sinr1`  varchar(200)  DEFAULT NULL ,`lac_tac1`  varchar(200)  DEFAULT NULL ,`cid_pci1`  varchar(200)  DEFAULT NULL ,`net_type2`  varchar(200)  DEFAULT NULL ," +
					"`signal_strength2`  varchar(200)  DEFAULT NULL ,`sinr2`  varchar(200)  DEFAULT NULL ,`lac_tac2`  varchar(200)  DEFAULT NULL ,`cid_pci2`  varchar(200)  DEFAULT NULL ,`haschange`  int(11) NULL DEFAULT NULL ,`address_info`  varchar(100)  DEFAULT NULL ," +
					"`success_rate`  varchar(100) NOT NULL DEFAULT '' ,`detailreport`  varchar(200)  DEFAULT NULL ,`quick_test_id`  int(200) NULL DEFAULT NULL ,`operator`  varchar(200)  DEFAULT NULL ,`imsi`  varchar(200)  DEFAULT NULL ,`appid`  varchar(200)  DEFAULT NULL ,`test_description`  varchar(200)  DEFAULT NULL ,`test_location`  varchar(200)  DEFAULT NULL ,`connect_operator`  varchar(200)  DEFAULT NULL ," +
					" PRIMARY KEY (id) ) CHARSET=utf8 ROW_FORMAT=COMPACT;";
		}else if(newTableName.startsWith("web_browsing")){
			sql = "CREATE TABLE " + newTableName + " ( id int(10) NOT NULL AUTO_INCREMENT," +
					"`device_org`  varchar(200)  DEFAULT NULL ,`gps`  varchar(200)  DEFAULT NULL ,`longitude`  varchar(200)  DEFAULT NULL ,`latitude`  varchar(200)  DEFAULT NULL ,`province`  varchar(200)  DEFAULT NULL ,`city`  varchar(200)  DEFAULT NULL ,`district`  varchar(200)  DEFAULT NULL ," +
					"`time`  decimal(20,0) NULL DEFAULT NULL ,`model`  varchar(200)  DEFAULT NULL ,`logversion`  varchar(50)  DEFAULT NULL ,`web_sit`  varchar(200)  DEFAULT NULL ,`sit_size`  varchar(200)  DEFAULT NULL ,`eighty_loading`  varchar(200)  DEFAULT NULL ," +
					"`eighty_rate`  varchar(200)  DEFAULT NULL ,`full_complete`  varchar(200)  DEFAULT NULL ,`reference`  varchar(200)  DEFAULT NULL ,`location`  varchar(200)  DEFAULT NULL ,`net_type`  varchar(200)  DEFAULT NULL ,`signal_strength`  varchar(200)  DEFAULT NULL ," +
					"`sinr`  varchar(200)  DEFAULT NULL ,`lac_tac`  varchar(200)  DEFAULT NULL ,`cid_pci`  varchar(200)  DEFAULT NULL ,`imei`  varchar(200)  NOT NULL ,`file_path`  varchar(500)  DEFAULT NULL ,`file_index`  varchar(255)  DEFAULT NULL ," +
					"`android_ios`  varchar(255)  DEFAULT NULL ,`net_type1`  varchar(200)  DEFAULT NULL ,`signal_strength1`  varchar(200)  DEFAULT NULL ,`sinr1`  varchar(200)  DEFAULT NULL ,`lac_tac1`  varchar(200)  DEFAULT NULL ,`cid_pci1`  varchar(200)  DEFAULT NULL ," +
					"`net_type2`  varchar(200)  DEFAULT NULL ,`signal_strength2`  varchar(200)  DEFAULT NULL ,`sinr2`  varchar(200)  DEFAULT NULL ,`lac_tac2`  varchar(200)  DEFAULT NULL ,`cid_pci2`  varchar(200)  DEFAULT NULL ,`haschange`  int(11) NULL DEFAULT NULL ," +
					"`address_info`  varchar(100)  DEFAULT NULL ,`success_rate`  varchar(100)  DEFAULT NULL ,`detailreport`  varchar(200)  DEFAULT NULL ,`quick_test_id`  int(200)  DEFAULT NULL ,`success_counts`  decimal(20,0)  DEFAULT NULL ,`operator`  varchar(200)  DEFAULT NULL ,`imsi`  varchar(200)  DEFAULT NULL ,`appid`  varchar(200)  DEFAULT NULL ,`test_description`  varchar(200)  DEFAULT NULL ,`test_location`  varchar(200)  DEFAULT NULL ,`connect_operator`  varchar(200)  DEFAULT NULL ," +
					" PRIMARY KEY (id) ) CHARSET=utf8 ROW_FORMAT=COMPACT;";
		}else if(newTableName.startsWith("video_test")){
			sql = "CREATE TABLE " + newTableName + " ( id int(10) NOT NULL AUTO_INCREMENT," +
					"`device_org`  varchar(200)  DEFAULT NULL ,`gps`  varchar(200)  DEFAULT NULL ,`longitude`  varchar(200)  DEFAULT NULL ,`latitude`  varchar(200)  DEFAULT NULL ,`province`  varchar(200)  DEFAULT NULL ,`city`  varchar(200)  DEFAULT NULL ,`district`  varchar(200)  DEFAULT NULL ,`time`  decimal(20,0)  DEFAULT NULL ,`model`  varchar(200)  DEFAULT NULL ," +
					"`logversion`  varchar(50)  DEFAULT NULL ,`end_time`  varchar(200)  DEFAULT NULL ,`test_type`  varchar(200)  DEFAULT NULL ,`address`  varchar(200)  DEFAULT NULL ,`buffer_times`  decimal(10,0)  DEFAULT NULL ,`delay`  varchar(200)  DEFAULT NULL ,`avg_delay`  decimal(16,2) NULL DEFAULT NULL ,`test_times`  decimal(10,0) NULL DEFAULT NULL ,`location`  varchar(200)  DEFAULT NULL ," +
					"`net_type`  varchar(200)  DEFAULT NULL ,`signal_strength`  varchar(200)  DEFAULT NULL ,`sinr`  varchar(200)  DEFAULT NULL ,`lac_tac`  varchar(200)  DEFAULT NULL ,`cid_pci`  varchar(200)  DEFAULT NULL ,`imei`  varchar(200)  DEFAULT NULL ,`file_path`  varchar(500)  DEFAULT NULL ,`file_index`  varchar(255)  DEFAULT NULL ,`android_ios`  varchar(200)  DEFAULT NULL ," +
					"`net_type1`  varchar(200)  DEFAULT NULL ,`signal_strength1`  varchar(200)  DEFAULT NULL ,`sinr1`  varchar(200)  DEFAULT NULL ,`lac_tac1`  varchar(200)  DEFAULT NULL ,`cid_pci1`  varchar(200)  DEFAULT NULL ,`net_type2`  varchar(200)  DEFAULT NULL ,`signal_strength2`  varchar(200)  DEFAULT NULL ,`sinr2`  varchar(200)  DEFAULT NULL ,`lac_tac2`  varchar(200)  DEFAULT NULL ," +
					"`cid_pci2`  varchar(200)  DEFAULT NULL ,`haschange`  int(11)  DEFAULT NULL ,`detailreport`  varchar(11)  DEFAULT NULL ,`address_info`  varchar(100)  DEFAULT NULL ,`quick_test_id`  varchar(200)  DEFAULT NULL ,`operator`  varchar(200)  DEFAULT NULL ,`imsi`  varchar(200)  DEFAULT NULL ,`appid`  varchar(200)  DEFAULT NULL ,`test_description`  varchar(200)  DEFAULT NULL ,`test_location`  varchar(200)  DEFAULT NULL ,`connect_operator`  varchar(200)  DEFAULT NULL ," +
					" PRIMARY KEY (id) ) CHARSET=utf8 ROW_FORMAT=COMPACT;";
		}else if(newTableName.startsWith("hand_over")){
			sql = "CREATE TABLE " + newTableName + " ( id int(10) NOT NULL AUTO_INCREMENT," +
					"`device_org`  varchar(200)  DEFAULT NULL ,`gps`  varchar(200)  DEFAULT NULL ,`longitude`  varchar(200)  DEFAULT NULL ,`latitude`  varchar(200)  DEFAULT NULL ,`province`  varchar(200)  DEFAULT NULL ,`city`  varchar(200)  DEFAULT NULL ,`district`  varchar(200)  DEFAULT NULL ,`time`  decimal(20,0)  DEFAULT NULL ,`model`  varchar(200)  DEFAULT NULL ," +
					"`logversion`  varchar(50)  DEFAULT NULL ,`duration`  varchar(200)  DEFAULT NULL ,`max_latency`  decimal(20,2)  DEFAULT NULL ,`min_latency`  decimal(20,2)  DEFAULT NULL ,`avg_latency`  decimal(20,2)  DEFAULT NULL ,`location`  varchar(200)  DEFAULT NULL ,`net_type`  varchar(200)  DEFAULT NULL ,`signal_strength`  varchar(100)  DEFAULT NULL ,`sinr`  varchar(200)  DEFAULT NULL ," +
					"`lac_tac`  varchar(200)  DEFAULT NULL ,`cid_pci`  varchar(200)  DEFAULT NULL ,`imei`  varchar(50)  NOT NULL ,`file_path`  varchar(500)  DEFAULT NULL ,`file_index`  varchar(255)  DEFAULT NULL ,`android_ios`  varchar(255)  DEFAULT NULL ,`net_type1`  varchar(200)  DEFAULT NULL ,`signal_strength1`  varchar(100)  DEFAULT NULL ,`sinr1`  varchar(200)  DEFAULT NULL ," +
					"`lac_tac1`  varchar(200)  DEFAULT NULL ,`cid_pci1`  varchar(200)  DEFAULT NULL ,`net_type2`  varchar(200)  DEFAULT NULL ,`signal_strength2`  varchar(100)  DEFAULT NULL ,`sinr2`  varchar(200)  DEFAULT NULL ,`lac_tac2`  varchar(200)  DEFAULT NULL ,`cid_pci2`  varchar(200)  DEFAULT NULL ,`haschange`  int(11)  DEFAULT NULL ,`address_info`  varchar(100)  DEFAULT NULL ,`test_location`  varchar(200)  DEFAULT NULL ,`connect_operator`  varchar(200)  DEFAULT NULL ," +
					"`detailreport`  varchar(200)  DEFAULT NULL ,`quick_test_id`  int(200)  DEFAULT NULL ,`operator`  varchar(200)  DEFAULT NULL ,`imsi`  varchar(200)  DEFAULT NULL ,`appid`  varchar(200)  DEFAULT NULL ,`test_description`  varchar(200)  DEFAULT NULL ," +
					"PRIMARY KEY (id) ) CHARSET=utf8 ROW_FORMAT=COMPACT;";
		}else if(newTableName.startsWith("call_test")){
			sql = "CREATE TABLE " + newTableName + " ( id int(10) NOT NULL AUTO_INCREMENT," +
					"`device_org`  varchar(200)  DEFAULT NULL ,`gps`  varchar(200)  DEFAULT NULL ,`longitude`  varchar(200)  DEFAULT NULL ,`latitude`  varchar(200)  DEFAULT NULL ,`province`  varchar(200)  DEFAULT NULL ,`city`  varchar(200)  DEFAULT NULL ,`district`  varchar(200)  DEFAULT NULL ,`time`  varchar(200)  DEFAULT NULL ,`model`  varchar(200)  DEFAULT NULL ,`logversion`  varchar(50)  DEFAULT NULL ,`continuing_time`  varchar(200)  DEFAULT NULL ," +
					"`connecting_time`  varchar(200)  DEFAULT NULL ,`connected_time`  varchar(200)  DEFAULT NULL ,`recording`  varchar(200)  DEFAULT NULL ,`phone_number`  varchar(50)  DEFAULT NULL ,`target_number`  varchar(50)  DEFAULT NULL ,`network`  varchar(200)  DEFAULT NULL ,`hand_over_situation`  varchar(200)  DEFAULT NULL ,`success_failure`  varchar(100)  DEFAULT NULL ,`success_rate`  varchar(50)  DEFAULT NULL ," +
					"`location`  varchar(200)  DEFAULT NULL ,`net_type`  varchar(200)  DEFAULT NULL ,`signal_strength`  varchar(200)  DEFAULT NULL ,`sinr`  varchar(200)  DEFAULT NULL ,`lac_tac`  varchar(200)  DEFAULT NULL ,`cid_pci`  varchar(200)  DEFAULT NULL ,`imei`  varchar(200)  NOT NULL ,`file_path`  varchar(500)  DEFAULT NULL ,`file_index`  varchar(255)  DEFAULT NULL ," +
					"`android_ios`  varchar(255)  DEFAULT NULL ,`net_type1`  varchar(200)  DEFAULT NULL ,`signal_strength1`  varchar(200)  DEFAULT NULL ,`sinr1`  varchar(200)  DEFAULT NULL ,`lac_tac1`  varchar(200)  DEFAULT NULL ,`cid_pci1`  varchar(200)  DEFAULT NULL ,`net_type2`  varchar(200)  DEFAULT NULL ,`signal_strength2`  varchar(200)  DEFAULT NULL ,`sinr2`  varchar(200)  DEFAULT NULL ," +
					"`lac_tac2`  varchar(200)  DEFAULT NULL ,`cid_pci2`  varchar(200)  DEFAULT NULL ,`haschange`  varchar(11)  DEFAULT NULL ,`csfb`  varchar(11)  DEFAULT '0' COMMENT '是否是csfb的数据' ,`address_info`  varchar(100)  DEFAULT NULL ,`backtolte`  varchar(200)  DEFAULT NULL ,`fallbacktocs`  varchar(200)  DEFAULT NULL ,`detailreport`  varchar(200)  DEFAULT NULL ,`quick_test_id`  int(200)  DEFAULT NULL ,`operator`  varchar(200)  DEFAULT NULL ,`imsi`  varchar(200)  DEFAULT NULL ,`appid`  varchar(200)  DEFAULT NULL ,`test_description`  varchar(200)  DEFAULT NULL ,`test_location`  varchar(200)  DEFAULT NULL ,`connect_operator`  varchar(200)  DEFAULT NULL ," +
					" PRIMARY KEY (id) ) CHARSET=utf8 ROW_FORMAT=COMPACT;";
		}else if(newTableName.startsWith("sms")&&!newTableName.startsWith("sms_query")){
			sql = "CREATE TABLE " + newTableName + " ( id int(10) NOT NULL AUTO_INCREMENT, " +
					"`device_org`  varchar(200)  DEFAULT NULL ,`gps`  varchar(200)  DEFAULT NULL ,`longitude`  varchar(200)  DEFAULT NULL ,`latitude`  varchar(200)  DEFAULT NULL ,`province`  varchar(200)  DEFAULT NULL ,`city`  varchar(200)  DEFAULT NULL ,`district`  varchar(200)  DEFAULT NULL ,`time`  varchar(200)  DEFAULT NULL ,`model`  varchar(200)  DEFAULT NULL ," +
					"`logversion`  varchar(50)  DEFAULT NULL ,`target_number`  varchar(200)  DEFAULT NULL ,`sending_delay`  varchar(200)  DEFAULT NULL ,`status`  varchar(50)  DEFAULT NULL ,`test_times`  varchar(50)  DEFAULT NULL ,`success_times`  varchar(50)  DEFAULT NULL ,`success_rate`  varchar(50)  DEFAULT NULL ,`location`  varchar(200)  DEFAULT NULL ,`net_type`  varchar(200)  NOT NULL ," +
					"`signal_strength`  varchar(200)  DEFAULT NULL ,`sinr`  varchar(200)  DEFAULT NULL ,`lac_tac`  varchar(200)  DEFAULT NULL ,`cid_pci`  varchar(200)  DEFAULT NULL ,`imei`  varchar(200)  NOT NULL ,`file_path`  varchar(500)  DEFAULT NULL ,`file_index`  varchar(255)  DEFAULT NULL ,`android_ios`  varchar(255)  DEFAULT NULL ,`net_type1`  varchar(200)  DEFAULT NULL ," +
					"`signal_strength1`  varchar(200)  DEFAULT NULL ,`sinr1`  varchar(200)  DEFAULT NULL ,`lac_tac1`  varchar(200)  DEFAULT NULL ,`cid_pci1`  varchar(200)  DEFAULT NULL ,`net_type2`  varchar(200)  DEFAULT NULL ,`signal_strength2`  varchar(200)  DEFAULT NULL ,`sinr2`  varchar(200)  DEFAULT NULL ,`lac_tac2`  varchar(200)  DEFAULT NULL ,`cid_pci2`  varchar(200)  DEFAULT NULL ," +
					"`haschange`  int(11)  DEFAULT NULL ,`address_info`  varchar(100)  DEFAULT NULL ,`success_failure`  varchar(100)  DEFAULT NULL ,`detailreport`  varchar(200)  DEFAULT NULL ,`quick_test_id`  int(200)  DEFAULT NULL ,`operator`  varchar(200)  DEFAULT NULL ,`imsi`  varchar(200)  DEFAULT NULL ,`testtype`  varchar(200)  DEFAULT NULL ,`appid`  varchar(200)  DEFAULT NULL ,`test_description`  varchar(200)  DEFAULT NULL ,`test_location`  varchar(200)  DEFAULT NULL ,`connect_operator`  varchar(200)  DEFAULT NULL ," +
					"PRIMARY KEY (id) ) CHARSET=utf8 ROW_FORMAT=COMPACT;";
		}else if(newTableName.startsWith("sms_query")){
			sql = "CREATE TABLE " + newTableName + " ( id int(10) NOT NULL AUTO_INCREMENT," +
					"`device_org`  varchar(200)  DEFAULT NULL ,`gps`  varchar(200)  DEFAULT NULL ,`longitude`  varchar(200)  DEFAULT NULL ,`latitude`  varchar(200)  DEFAULT NULL ,`province`  varchar(200)  DEFAULT NULL ,`city`  varchar(200)  DEFAULT NULL ,`district`  varchar(200)  DEFAULT NULL ,`time`  varchar(200)  DEFAULT NULL ,`model`  varchar(200)  DEFAULT NULL ,`logversion`  varchar(50)  DEFAULT NULL ," +
					"`target_number`  varchar(200)  DEFAULT NULL ,`sending_delay`  varchar(200)  DEFAULT NULL ,`sms_send_delay`  varchar(200)  DEFAULT NULL ,`sms_query_delay`  varchar(200)  DEFAULT NULL ,`sms_receive_num`  varchar(200)  DEFAULT NULL ,`status`  varchar(50)  DEFAULT NULL ,`test_times`  varchar(50)  DEFAULT NULL ,`success_times`  varchar(50)  DEFAULT NULL ,`success_rate`  varchar(50)  DEFAULT NULL ," +
					"`location`  varchar(200)  DEFAULT NULL ,`testtype`  varchar(200)  DEFAULT NULL ,`net_type`  varchar(200)  DEFAULT NULL ,`signal_strength`  varchar(200)  DEFAULT NULL ,`sinr`  varchar(200)  DEFAULT NULL ,`lac_tac`  varchar(200)  DEFAULT NULL ,`cid_pci`  varchar(200)  DEFAULT NULL ,`imei`  varchar(200)  NOT NULL ,`file_path`  varchar(500)  NOT NULL ," +
					"`file_index`  varchar(255)  DEFAULT NULL ,`android_ios`  varchar(255)  DEFAULT NULL ,`net_type1`  varchar(200)  DEFAULT NULL ,`signal_strength1`  varchar(200)  DEFAULT NULL ,`sinr1`  varchar(200)  DEFAULT NULL ,`lac_tac1`  varchar(200)  DEFAULT NULL ,`cid_pci1`  varchar(200)  DEFAULT NULL ,`net_type2`  varchar(200)  DEFAULT NULL ,`signal_strength2`  varchar(200)  DEFAULT NULL ," +
					"`sinr2`  varchar(200)  DEFAULT NULL ,`lac_tac2`  varchar(200)  DEFAULT NULL ,`cid_pci2`  varchar(200)  DEFAULT NULL ,`haschange`  int(11)  DEFAULT NULL ,`detailreport`  varchar(11)  DEFAULT NULL ,`address_info`  varchar(100)  DEFAULT NULL ," +
					"`success_failure`  varchar(100)  DEFAULT NULL ,`quick_test_id`  int(200)  DEFAULT NULL ,`imsi`  varchar(200)  DEFAULT NULL ,`operator`  varchar(200)  DEFAULT NULL ,`appid`  varchar(200)  DEFAULT NULL ,`test_description`  varchar(200)  DEFAULT NULL ,`test_location`  varchar(200)  DEFAULT NULL ,`connect_operator`  varchar(200)  DEFAULT NULL ," +
					" PRIMARY KEY (id) ) CHARSET=utf8 ROW_FORMAT=COMPACT;";
		}else if(newTableName.startsWith("speed_test")){
			sql = "CREATE TABLE " + newTableName + " ( id int(10) NOT NULL AUTO_INCREMENT," +
					"`device_org`  varchar(200)  DEFAULT NULL ,`gps`  varchar(200)  DEFAULT NULL ,`longitude`  varchar(200)  DEFAULT NULL ,`latitude`  varchar(200)  DEFAULT NULL ,`province`  varchar(200)  DEFAULT NULL ,`city`  varchar(200)  DEFAULT NULL ,`district`  varchar(200)  DEFAULT NULL ,`time`  decimal(20,0)  DEFAULT NULL ," +
					"`model`  varchar(200)  DEFAULT NULL ,`logversion`  varchar(50)  DEFAULT NULL ,`max_up`  varchar(200)  DEFAULT NULL ,`avg_up`  varchar(200)  DEFAULT NULL ,`up_file_size`  varchar(200)  DEFAULT NULL ,`max_down`  varchar(200)  DEFAULT NULL ,`avg_down`  varchar(200)  DEFAULT NULL ,`down_file_size`  varchar(200)  DEFAULT NULL ,`protocol`  varchar(200)  DEFAULT NULL ,`downlink`  int(11) DEFAULT NULL ,`uplink`  int(11)  DEFAULT NULL ,`delay`  varchar(200)  DEFAULT NULL ,`server_ip`  varchar(200)  DEFAULT NULL ,`location`  varchar(200)  DEFAULT NULL ,`net_type`  varchar(200)  DEFAULT NULL ,`signal_strength`  varchar(200)  DEFAULT NULL ," +
					"`sinr`  varchar(200)  DEFAULT NULL ,`lac_tac`  varchar(200)  DEFAULT NULL ,`cid_pci`  varchar(200)  DEFAULT NULL ,`imei`  varchar(200)  NOT NULL ,`file_path`  varchar(500)  DEFAULT NULL ,`file_index`  varchar(255)  DEFAULT NULL ,`android_ios`  varchar(255)  DEFAULT NULL ,`net_type1`  varchar(200)  DEFAULT NULL ,`signal_strength1`  varchar(200)  DEFAULT NULL ,`sinr1`  varchar(200)  DEFAULT NULL ,`lac_tac1`  varchar(200)  DEFAULT NULL ,`cid_pci1`  varchar(200)  DEFAULT NULL ,`net_type2`  varchar(200)  DEFAULT NULL ,`signal_strength2`  varchar(200)  DEFAULT NULL ,`sinr2`  varchar(200)  DEFAULT NULL ,`lac_tac2`  varchar(200)  DEFAULT NULL ," +
					"`cid_pci2`  varchar(200)  DEFAULT NULL ,`haschange`  int(11) NULL DEFAULT NULL ,`address_info`  varchar(100)  DEFAULT NULL ,`detailreport`  varchar(200)  DEFAULT NULL ,`quick_test_id`  int(200)  DEFAULT NULL ,`operator`  varchar(200)  DEFAULT NULL ,`imsi`  varchar(200)  DEFAULT NULL ,`appid`  varchar(200)  DEFAULT NULL ,`test_description`  varchar(200)  DEFAULT NULL ,`test_location`  varchar(200)  DEFAULT NULL ,`connect_operator`  varchar(200)  DEFAULT NULL ," +
					"PRIMARY KEY (id) ) CHARSET=utf8 ROW_FORMAT=COMPACT;";
		}
		
		
		
		
		
		String url = dsturl.substring(0, dsturl.lastIndexOf("/") + 1) + ResultsetTestData.basename;
		try {
			start(url, dstuser,dstpassword);
			statement.execute(sql);
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			try {
				close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	
	
	
	
	/**
	 * 创建new表 （ping_new_201510）
	 * @param newTableName
	 * @return void
	 */
	public void createNewTable(String newTableName){
		
		String sql = "";
		
		if(newTableName.startsWith("ping")){//ping_new 表创建语句
			sql = "CREATE TABLE " + newTableName + " ( id int(10) NOT NULL AUTO_INCREMENT, " +
					"`device_org`  varchar(200)  DEFAULT NULL ,`gps`  varchar(200)  DEFAULT NULL ,`longitude`  decimal(50,20)  DEFAULT NULL ,`latitude`  decimal(50,20)  DEFAULT NULL ,`province`  varchar(200)  DEFAULT NULL ,`city`  varchar(200)  DEFAULT NULL ,`district`  varchar(200)  DEFAULT NULL ,`time`  decimal(20,0)  DEFAULT NULL ,`model`  varchar(200)  DEFAULT NULL ," +
					"`logversion`  varchar(200)  DEFAULT NULL ,`domain_address`  varchar(200)  DEFAULT NULL ,`max_rtt`  varchar(200)  DEFAULT NULL ,`min_rtt`  varchar(200)  DEFAULT NULL ,`avg_rtt`  varchar(200)  DEFAULT NULL ,`packet_loss_rate`  varchar(200)  DEFAULT NULL ,`times`  int(11) NULL DEFAULT NULL ,`location`  varchar(200)  DEFAULT NULL ," +
					"`net_type`  varchar(200)  DEFAULT NULL ,`signal_strength`  decimal(20,0)  DEFAULT NULL ,`sinr`  varchar(200)  DEFAULT NULL ,`lac_tac`  varchar(200) NULL DEFAULT NULL ,`cid_pci`  varchar(200)  DEFAULT NULL ,`imei`  varchar(200)  NOT NULL ,`file_path`  varchar(500)  DEFAULT NULL ,`file_index`  varchar(255)  DEFAULT NULL ,`android_ios`  varchar(255)  DEFAULT NULL ," +
					"`net_type1`  varchar(200)  DEFAULT NULL ,`signal_strength1`  decimal(20,0)  DEFAULT NULL ,`sinr1`  varchar(200)  DEFAULT NULL ,`lac_tac1`  varchar(200)  DEFAULT NULL ,`cid_pci1`  varchar(200)  DEFAULT NULL ,`net_type2`  varchar(200)  DEFAULT NULL ,`signal_strength2`  decimal(20,0)  DEFAULT NULL ,`sinr2`  varchar(200)  DEFAULT NULL ," +
					"`lac_tac2`  varchar(200)  DEFAULT NULL ,`cid_pci2`  varchar(200)  DEFAULT NULL ,`address_info`  varchar(100)  DEFAULT NULL ,`haschange`  int(11)  DEFAULT NULL ,`success_rate`  varchar(200)  DEFAULT NULL ,`detailreport`  varchar(200)  DEFAULT NULL ,`quick_test_id`  int(200)  DEFAULT NULL ,`operator`  varchar(200)  DEFAULT NULL ," +
					"`imsi`  varchar(200)  DEFAULT NULL ,`normalornot`  varchar(200)  DEFAULT NULL ,`appid`  varchar(200)  DEFAULT NULL ,`test_description`  varchar(200)  DEFAULT NULL ,`test_location`  varchar(200)  DEFAULT NULL ,`connect_operator`  varchar(200)  DEFAULT NULL ," +
					" PRIMARY KEY (id),INDEX `lng_lat` (`latitude`),INDEX `lng_lat` (`longitude`) ) CHARSET=utf8 ROW_FORMAT=COMPACT;";
		}else if(newTableName.startsWith("http_test")){
			sql = "CREATE TABLE " + newTableName + " ( id int(10) NOT NULL AUTO_INCREMENT, " +
					"`device_org`  varchar(200)  DEFAULT NULL ,`gps`  varchar(200)  DEFAULT NULL ,`longitude`   decimal(50,20)  DEFAULT NULL ,`latitude`  decimal(50,20)  DEFAULT NULL ," +
					"`province`  varchar(200)  DEFAULT NULL ,`city`  varchar(200)  DEFAULT NULL ,`district`  varchar(200)  DEFAULT NULL ,`time`  varchar(200)  DEFAULT NULL ," +
					"`model`  varchar(200)  DEFAULT NULL ,`logversion`  varchar(50)  DEFAULT NULL ,`file_type`  varchar(50)  DEFAULT NULL ,`url`  varchar(500)  DEFAULT NULL ," +
					"`resource_size`  varchar(200)  DEFAULT NULL ,`duration`  varchar(200)  DEFAULT NULL ,`avg_rate`  varchar(200)  DEFAULT NULL ," +
					"`max_rate`  varchar(200)  DEFAULT NULL COMMENT '最大速率' ,`avg_latency`  varchar(200)  DEFAULT NULL COMMENT '平均时延' ,`location`  varchar(200)  DEFAULT NULL ," +
					"`net_type`  varchar(200)  DEFAULT NULL ,`signal_strength`  varchar(200)  DEFAULT NULL ,`sinr`  varchar(200)  DEFAULT NULL ," +
					"`cid_pci`  varchar(200)  DEFAULT NULL ,`lac_tac`  varchar(200)  DEFAULT NULL ,`imei`  varchar(200)  NOT NULL ,`file_index`  varchar(255)  DEFAULT NULL ,`file_path`  varchar(500)  DEFAULT NULL ,`android_ios`  varchar(255)  DEFAULT NULL ," +
					"`net_type1`  varchar(200)  DEFAULT NULL ,`signal_strength1`  varchar(200)  DEFAULT NULL ,`sinr1`  varchar(200)  DEFAULT NULL ,`lac_tac1`  varchar(200)  DEFAULT NULL ,`cid_pci1`  varchar(200)  DEFAULT NULL ,`net_type2`  varchar(200)  DEFAULT NULL ," +
					"`signal_strength2`  varchar(200)  DEFAULT NULL ,`sinr2`  varchar(200)  DEFAULT NULL ,`lac_tac2`  varchar(200)  DEFAULT NULL ,`cid_pci2`  varchar(200)  DEFAULT NULL ,`haschange`  int(11) NULL DEFAULT NULL ,`address_info`  varchar(100)  DEFAULT NULL ," +
					"`success_rate`  varchar(100) NOT NULL DEFAULT '' ,`detailreport`  varchar(200)  DEFAULT NULL ,`quick_test_id`  int(200) NULL DEFAULT NULL ,`operator`  varchar(200)  DEFAULT NULL ,`imsi`  varchar(200)  DEFAULT NULL ,`appid`  varchar(200)  DEFAULT NULL ," +
					"`test_description`  varchar(200)  DEFAULT NULL ,`normalornot`  varchar(200)  DEFAULT NULL ,`test_location`  varchar(200)  DEFAULT NULL ,`connect_operator`  varchar(200)  DEFAULT NULL ," +
					" PRIMARY KEY (id),INDEX `lng_lat` (`latitude`),INDEX `lng_lat` (`longitude`) ) CHARSET=utf8 ROW_FORMAT=COMPACT;";
		}else if(newTableName.startsWith("web_browsing")){
			sql = "CREATE TABLE " + newTableName + " ( id int(10) NOT NULL AUTO_INCREMENT," +
					"`device_org`  varchar(200)  DEFAULT NULL ,`gps`  varchar(200)  DEFAULT NULL ,`longitude`   decimal(50,20)  DEFAULT NULL ,`latitude` decimal(50,20)  DEFAULT NULL ,`province`  varchar(200)  DEFAULT NULL ,`city`  varchar(200)  DEFAULT NULL ,`district`  varchar(200)  DEFAULT NULL ," +
					"`time`  decimal(20,0) NULL DEFAULT NULL ,`model`  varchar(200)  DEFAULT NULL ,`logversion`  varchar(50)  DEFAULT NULL ,`web_sit`  varchar(200)  DEFAULT NULL ,`sit_size`  varchar(200)  DEFAULT NULL ,`eighty_loading`  varchar(200)  DEFAULT NULL ," +
					"`eighty_rate`  varchar(200)  DEFAULT NULL ,`full_complete`  varchar(200)  DEFAULT NULL ,`reference`  varchar(200)  DEFAULT NULL ,`location`  varchar(200)  DEFAULT NULL ,`net_type`  varchar(200)  DEFAULT NULL ,`signal_strength`  varchar(200)  DEFAULT NULL ," +
					"`sinr`  varchar(200)  DEFAULT NULL ,`lac_tac`  varchar(200)  DEFAULT NULL ,`cid_pci`  varchar(200)  DEFAULT NULL ,`imei`  varchar(200)  NOT NULL ,`file_path`  varchar(500)  DEFAULT NULL ,`file_index`  varchar(255)  DEFAULT NULL ," +
					"`android_ios`  varchar(255)  DEFAULT NULL ,`net_type1`  varchar(200)  DEFAULT NULL ,`signal_strength1`  varchar(200)  DEFAULT NULL ,`sinr1`  varchar(200)  DEFAULT NULL ,`lac_tac1`  varchar(200)  DEFAULT NULL ,`cid_pci1`  varchar(200)  DEFAULT NULL ," +
					"`net_type2`  varchar(200)  DEFAULT NULL ,`signal_strength2`  varchar(200)  DEFAULT NULL ,`sinr2`  varchar(200)  DEFAULT NULL ,`lac_tac2`  varchar(200)  DEFAULT NULL ,`cid_pci2`  varchar(200)  DEFAULT NULL ,`haschange`  int(11) NULL DEFAULT NULL ," +
					"`address_info`  varchar(100)  DEFAULT NULL ,`success_rate`  varchar(100)  DEFAULT NULL ,`detailreport`  varchar(200)  DEFAULT NULL ,`quick_test_id`  int(200)  DEFAULT NULL ,`success_counts`  decimal(20,0)  DEFAULT NULL ,`operator`  varchar(200)  DEFAULT NULL ," +
					"`imsi`  varchar(200)  DEFAULT NULL ,`appid`  varchar(200)  DEFAULT NULL ,`test_description`  varchar(200)  DEFAULT NULL ,`test_location`  varchar(200)  DEFAULT NULL ,`connect_operator`  varchar(200)  DEFAULT NULL ,`normalornot`  varchar(200)  DEFAULT NULL ," +
					" PRIMARY KEY (id) ,INDEX `lng_lat` (`latitude`),INDEX `lng_lat` (`longitude`) ) CHARSET=utf8 ROW_FORMAT=COMPACT;";
		}else if(newTableName.startsWith("video_test")){
			sql = "CREATE TABLE " + newTableName + " ( id int(10) NOT NULL AUTO_INCREMENT," +
					"`device_org`  varchar(200)  DEFAULT NULL ,`gps`  varchar(200)  DEFAULT NULL ,`longitude`  decimal(50,20)  DEFAULT NULL ,`latitude`  decimal(50,20)  DEFAULT NULL ,`province`  varchar(200)  DEFAULT NULL ,`city`  varchar(200)  DEFAULT NULL ,`district`  varchar(200)  DEFAULT NULL ,`time`  decimal(20,0)  DEFAULT NULL ,`model`  varchar(200)  DEFAULT NULL ," +
					"`logversion`  varchar(50)  DEFAULT NULL ,`end_time`  varchar(200)  DEFAULT NULL ,`test_type`  varchar(200)  DEFAULT NULL ,`address`  varchar(200)  DEFAULT NULL ,`buffer_times`  decimal(10,0)  DEFAULT NULL ,`delay`  varchar(200)  DEFAULT NULL ,`avg_delay`  decimal(16,2) NULL DEFAULT NULL ,`test_times`  decimal(10,0) NULL DEFAULT NULL ,`location`  varchar(200)  DEFAULT NULL ," +
					"`net_type`  varchar(200)  DEFAULT NULL ,`signal_strength`  varchar(200)  DEFAULT NULL ,`sinr`  varchar(200)  DEFAULT NULL ,`lac_tac`  varchar(200)  DEFAULT NULL ,`cid_pci`  varchar(200)  DEFAULT NULL ,`imei`  varchar(200)  DEFAULT NULL ,`file_path`  varchar(500)  DEFAULT NULL ,`file_index`  varchar(255)  DEFAULT NULL ,`android_ios`  varchar(200)  DEFAULT NULL ," +
					"`net_type1`  varchar(200)  DEFAULT NULL ,`signal_strength1`  varchar(200)  DEFAULT NULL ,`sinr1`  varchar(200)  DEFAULT NULL ,`lac_tac1`  varchar(200)  DEFAULT NULL ,`cid_pci1`  varchar(200)  DEFAULT NULL ,`net_type2`  varchar(200)  DEFAULT NULL ,`signal_strength2`  varchar(200)  DEFAULT NULL ,`sinr2`  varchar(200)  DEFAULT NULL ,`lac_tac2`  varchar(200)  DEFAULT NULL ," +
					"`cid_pci2`  varchar(200)  DEFAULT NULL ,`haschange`  int(11)  DEFAULT NULL ,`detailreport`  varchar(11)  DEFAULT NULL ,`address_info`  varchar(100)  DEFAULT NULL ,`quick_test_id`  varchar(200)  DEFAULT NULL ,`operator`  varchar(200)  DEFAULT NULL ,`imsi`  varchar(200)  DEFAULT NULL ,`appid`  varchar(200)  DEFAULT NULL ,`test_description`  varchar(200)  DEFAULT NULL ,`test_location`  varchar(200)  DEFAULT NULL ,`connect_operator`  varchar(200)  DEFAULT NULL ," +
					"`normalornot`  varchar(200)  DEFAULT NULL , PRIMARY KEY (id) ,INDEX `lng_lat` (`latitude`),INDEX `lng_lat` (`longitude`)) CHARSET=utf8 ROW_FORMAT=COMPACT;";
		}else if(newTableName.startsWith("hand_over")){
			sql = "CREATE TABLE " + newTableName + " ( id int(10) NOT NULL AUTO_INCREMENT," +
					"`device_org`  varchar(200)  DEFAULT NULL ,`gps`  varchar(200)  DEFAULT NULL ,`longitude`  decimal(50,20)  DEFAULT NULL ,`latitude`  decimal(50,20)  DEFAULT NULL ,`province`  varchar(200)  DEFAULT NULL ,`city`  varchar(200)  DEFAULT NULL ,`district`  varchar(200)  DEFAULT NULL ,`time`  decimal(20,0)  DEFAULT NULL ,`model`  varchar(200)  DEFAULT NULL ," +
					"`logversion`  varchar(50)  DEFAULT NULL ,`duration`  varchar(200)  DEFAULT NULL ,`max_latency`  decimal(20,2)  DEFAULT NULL ,`min_latency`  decimal(20,2)  DEFAULT NULL ,`avg_latency`  decimal(20,2)  DEFAULT NULL ,`location`  varchar(200)  DEFAULT NULL ,`net_type`  varchar(200)  DEFAULT NULL ,`signal_strength`  varchar(100)  DEFAULT NULL ,`sinr`  varchar(200)  DEFAULT NULL ," +
					"`lac_tac`  varchar(200)  DEFAULT NULL ,`cid_pci`  varchar(200)  DEFAULT NULL ,`imei`  varchar(50)  NOT NULL ,`file_path`  varchar(500)  DEFAULT NULL ,`file_index`  varchar(255)  DEFAULT NULL ,`android_ios`  varchar(255)  DEFAULT NULL ,`net_type1`  varchar(200)  DEFAULT NULL ,`signal_strength1`  varchar(100)  DEFAULT NULL ,`sinr1`  varchar(200)  DEFAULT NULL ," +
					"`lac_tac1`  varchar(200)  DEFAULT NULL ,`cid_pci1`  varchar(200)  DEFAULT NULL ,`net_type2`  varchar(200)  DEFAULT NULL ,`signal_strength2`  varchar(100)  DEFAULT NULL ,`sinr2`  varchar(200)  DEFAULT NULL ,`lac_tac2`  varchar(200)  DEFAULT NULL ,`cid_pci2`  varchar(200)  DEFAULT NULL ,`haschange`  int(11)  DEFAULT NULL ,`address_info`  varchar(100)  DEFAULT NULL ," +
					"`detailreport`  varchar(200)  DEFAULT NULL ,`quick_test_id`  int(200)  DEFAULT NULL ,`operator`  varchar(200)  DEFAULT NULL ,`imsi`  varchar(200)  DEFAULT NULL ,`appid`  varchar(200)  DEFAULT NULL ,`test_description`  varchar(200)  DEFAULT NULL ,`test_location`  varchar(200)  DEFAULT NULL ,`connect_operator`  varchar(200)  DEFAULT NULL ," +
					"`normalornot`  varchar(200)  DEFAULT NULL ,PRIMARY KEY (id) ,INDEX `lng_lat` (`latitude`),INDEX `lng_lat` (`longitude`)) CHARSET=utf8 ROW_FORMAT=COMPACT;";
		}else if(newTableName.startsWith("call_test")){
			sql = "CREATE TABLE " + newTableName + " ( id int(10) NOT NULL AUTO_INCREMENT," +
					"`device_org`  varchar(200)  DEFAULT NULL ,`gps`  varchar(200)  DEFAULT NULL ,`longitude`  decimal(50,20)  DEFAULT NULL ,`latitude`  decimal(50,20)  DEFAULT NULL ,`province`  varchar(200)  DEFAULT NULL ,`city`  varchar(200)  DEFAULT NULL ,`district`  varchar(200)  DEFAULT NULL ,`time`  varchar(200)  DEFAULT NULL ,`model`  varchar(200)  DEFAULT NULL ,`logversion`  varchar(50)  DEFAULT NULL ,`continuing_time`  varchar(200)  DEFAULT NULL ," +
					"`connecting_time`  varchar(200)  DEFAULT NULL ,`connected_time`  varchar(200)  DEFAULT NULL ,`recording`  varchar(200)  DEFAULT NULL ,`phone_number`  varchar(50)  DEFAULT NULL ,`target_number`  varchar(50)  DEFAULT NULL ,`network`  varchar(200)  DEFAULT NULL ,`hand_over_situation`  varchar(200)  DEFAULT NULL ,`success_failure`  varchar(100)  DEFAULT NULL ,`success_rate`  varchar(50)  DEFAULT NULL ," +
					"`location`  varchar(200)  DEFAULT NULL ,`net_type`  varchar(200)  DEFAULT NULL ,`signal_strength`  varchar(200)  DEFAULT NULL ,`sinr`  varchar(200)  DEFAULT NULL ,`lac_tac`  varchar(200)  DEFAULT NULL ,`cid_pci`  varchar(200)  DEFAULT NULL ,`imei`  varchar(200)  NOT NULL ,`file_path`  varchar(500)  DEFAULT NULL ,`file_index`  varchar(255)  DEFAULT NULL ," +
					"`android_ios`  varchar(255)  DEFAULT NULL ,`net_type1`  varchar(200)  DEFAULT NULL ,`signal_strength1`  varchar(200)  DEFAULT NULL ,`sinr1`  varchar(200)  DEFAULT NULL ,`lac_tac1`  varchar(200)  DEFAULT NULL ,`cid_pci1`  varchar(200)  DEFAULT NULL ,`net_type2`  varchar(200)  DEFAULT NULL ,`signal_strength2`  varchar(200)  DEFAULT NULL ,`sinr2`  varchar(200)  DEFAULT NULL ," +
					"`lac_tac2`  varchar(200)  DEFAULT NULL ,`cid_pci2`  varchar(200)  DEFAULT NULL ,`haschange`  varchar(11)  DEFAULT NULL ,`csfb`  varchar(11)  DEFAULT '0' COMMENT '是否是csfb的数据' ,`address_info`  varchar(100)  DEFAULT NULL ,`backtolte`  varchar(200)  DEFAULT NULL ,`fallbacktocs`  varchar(200)  DEFAULT NULL ,`detailreport`  varchar(200)  DEFAULT NULL ,`quick_test_id`  int(200)  DEFAULT NULL ,`operator`  varchar(200)  DEFAULT NULL ,`imsi`  varchar(200)  DEFAULT NULL ,`appid`  varchar(200)  DEFAULT NULL ,`test_description`  varchar(200)  DEFAULT NULL ,`test_location`  varchar(200)  DEFAULT NULL ,`connect_operator`  varchar(200)  DEFAULT NULL ," +
					"`normalornot`  varchar(200)  DEFAULT NULL , PRIMARY KEY (id),INDEX `lng_lat` (`latitude`),INDEX `lng_lat` (`longitude`) ) CHARSET=utf8 ROW_FORMAT=COMPACT;";
		}else if(newTableName.startsWith("sms")&&!newTableName.startsWith("sms_query")){
			sql = "CREATE TABLE " + newTableName + " ( id int(10) NOT NULL AUTO_INCREMENT, " +
					"`device_org`  varchar(200)  DEFAULT NULL ,`gps`  varchar(200)  DEFAULT NULL ,`longitude`  decimal(50,20)  DEFAULT NULL ,`latitude`  decimal(50,20)  DEFAULT NULL ,`province`  varchar(200)  DEFAULT NULL ,`city`  varchar(200)  DEFAULT NULL ,`district`  varchar(200)  DEFAULT NULL ,`time`  varchar(200)  DEFAULT NULL ,`model`  varchar(200)  DEFAULT NULL ," +
					"`logversion`  varchar(50)  DEFAULT NULL ,`target_number`  varchar(200)  DEFAULT NULL ,`sending_delay`  varchar(200)  DEFAULT NULL ,`status`  varchar(50)  DEFAULT NULL ,`test_times`  varchar(50)  DEFAULT NULL ,`success_times`  varchar(50)  DEFAULT NULL ,`success_rate`  varchar(50)  DEFAULT NULL ,`location`  varchar(200)  DEFAULT NULL ,`net_type`  varchar(200)  NOT NULL ," +
					"`signal_strength`  varchar(200)  DEFAULT NULL ,`sinr`  varchar(200)  DEFAULT NULL ,`lac_tac`  varchar(200)  DEFAULT NULL ,`cid_pci`  varchar(200)  DEFAULT NULL ,`imei`  varchar(200)  NOT NULL ,`file_path`  varchar(500)  DEFAULT NULL ,`file_index`  varchar(255)  DEFAULT NULL ,`android_ios`  varchar(255)  DEFAULT NULL ,`net_type1`  varchar(200)  DEFAULT NULL ," +
					"`signal_strength1`  varchar(200)  DEFAULT NULL ,`sinr1`  varchar(200)  DEFAULT NULL ,`lac_tac1`  varchar(200)  DEFAULT NULL ,`cid_pci1`  varchar(200)  DEFAULT NULL ,`net_type2`  varchar(200)  DEFAULT NULL ,`signal_strength2`  varchar(200)  DEFAULT NULL ,`sinr2`  varchar(200)  DEFAULT NULL ,`lac_tac2`  varchar(200)  DEFAULT NULL ,`cid_pci2`  varchar(200)  DEFAULT NULL ," +
					"`haschange`  int(11)  DEFAULT NULL ,`address_info`  varchar(100)  DEFAULT NULL ,`success_failure`  varchar(100)  DEFAULT NULL ,`detailreport`  varchar(200)  DEFAULT NULL ,`quick_test_id`  int(200)  DEFAULT NULL ,`operator`  varchar(200)  DEFAULT NULL ,`imsi`  varchar(200)  DEFAULT NULL ,`testtype`  varchar(200)  DEFAULT NULL ,`appid`  varchar(200)  DEFAULT NULL ,`test_description`  varchar(200)  DEFAULT NULL ,`test_location`  varchar(200)  DEFAULT NULL ,`connect_operator`  varchar(200)  DEFAULT NULL ," +
					"`normalornot`  varchar(200)  DEFAULT NULL ,PRIMARY KEY (id) ,INDEX `lng_lat` (`latitude`),INDEX `lng_lat` (`longitude`)) CHARSET=utf8 ROW_FORMAT=COMPACT;";
		}else if(newTableName.startsWith("sms_query")){
			sql = "CREATE TABLE " + newTableName + " ( id int(10) NOT NULL AUTO_INCREMENT," +
					"`gps`  varchar(200)  DEFAULT NULL ,`longitude`  decimal(50,20)  DEFAULT NULL ,`latitude`  decimal(50,20)  DEFAULT NULL ,`province`  varchar(200)  DEFAULT NULL ,`city`  varchar(200)  DEFAULT NULL ,`district`  varchar(200)  DEFAULT NULL ,`time`  varchar(200)  DEFAULT NULL ,`model`  varchar(200)  DEFAULT NULL ,`logversion`  varchar(50)  DEFAULT NULL ," +
					"`target_number`  varchar(200)  DEFAULT NULL ,`sending_delay`  varchar(200)  DEFAULT NULL ,`sms_send_delay`  varchar(200)  DEFAULT NULL ,`sms_query_delay`  varchar(200)  DEFAULT NULL ,`sms_receive_num`  varchar(200)  DEFAULT NULL ,`status`  varchar(50)  DEFAULT NULL ,`test_times`  varchar(50)  DEFAULT NULL ,`success_times`  varchar(50)  DEFAULT NULL ,`success_rate`  varchar(50)  DEFAULT NULL ," +
					"`location`  varchar(200)  DEFAULT NULL ,`testtype`  varchar(200)  DEFAULT NULL ,`net_type`  varchar(200)  DEFAULT NULL ,`signal_strength`  varchar(200)  DEFAULT NULL ,`sinr`  varchar(200)  DEFAULT NULL ,`lac_tac`  varchar(200)  DEFAULT NULL ,`cid_pci`  varchar(200)  DEFAULT NULL ,`imei`  varchar(200)  NOT NULL ,`file_path`  varchar(500)  NOT NULL ," +
					"`file_index`  varchar(255)  DEFAULT NULL ,`android_ios`  varchar(255)  DEFAULT NULL ,`net_type1`  varchar(200)  DEFAULT NULL ,`signal_strength1`  varchar(200)  DEFAULT NULL ,`sinr1`  varchar(200)  DEFAULT NULL ,`lac_tac1`  varchar(200)  DEFAULT NULL ,`cid_pci1`  varchar(200)  DEFAULT NULL ,`net_type2`  varchar(200)  DEFAULT NULL ,`signal_strength2`  varchar(200)  DEFAULT NULL ," +
					"`sinr2`  varchar(200)  DEFAULT NULL ,`lac_tac2`  varchar(200)  DEFAULT NULL ,`cid_pci2`  varchar(200)  DEFAULT NULL ,`haschange`  int(11)  DEFAULT NULL ,`detailreport`  varchar(11)  DEFAULT NULL ,`address_info`  varchar(100)  DEFAULT NULL ," +
					"`success_failure`  varchar(100)  DEFAULT NULL ,`quick_test_id`  int(200)  DEFAULT NULL ,`imsi`  varchar(200)  DEFAULT NULL ,`operator`  varchar(200)  DEFAULT NULL ,`appid`  varchar(200)  DEFAULT NULL ,`test_description`  varchar(200)  DEFAULT NULL ,`test_location`  varchar(200)  DEFAULT NULL ,`connect_operator`  varchar(200)  DEFAULT NULL ," +
					" `normalornot`  varchar(200)  DEFAULT NULL ,PRIMARY KEY (id) ,INDEX `lng_lat` (`latitude`),INDEX `lng_lat` (`longitude`)) CHARSET=utf8 ROW_FORMAT=COMPACT;";
		}else if(newTableName.startsWith("speed_test")){
			sql = "CREATE TABLE " + newTableName + " ( id int(10) NOT NULL AUTO_INCREMENT," +
					"`device_org`  varchar(200)  DEFAULT NULL ,`gps`  varchar(200)  DEFAULT NULL ,`longitude`  decimal(50,20)  DEFAULT NULL ,`latitude`  decimal(50,20)  DEFAULT NULL,`province`  varchar(200)  DEFAULT NULL ,`city`  varchar(200)  DEFAULT NULL ,`district`  varchar(200)  DEFAULT NULL ,`time`  decimal(20,0)  DEFAULT NULL ," +
					"`model`  varchar(200)  DEFAULT NULL ,`logversion`  varchar(50)  DEFAULT NULL ,`max_up`  varchar(200)  DEFAULT NULL ,`avg_up`  varchar(200)  DEFAULT NULL ,`up_file_size`  varchar(200)  DEFAULT NULL ,`max_down`  varchar(200)  DEFAULT NULL ,`avg_down`  varchar(200)  DEFAULT NULL ,`down_file_size`  varchar(200)  DEFAULT NULL ,`protocol`  varchar(200)  DEFAULT NULL ,`downlink`  int(11) DEFAULT NULL ,`uplink`  int(11)  DEFAULT NULL ,`delay`  varchar(200)  DEFAULT NULL ,`server_ip`  varchar(200)  DEFAULT NULL ,`location`  varchar(200)  DEFAULT NULL ,`net_type`  varchar(200)  DEFAULT NULL ,`signal_strength`  varchar(200)  DEFAULT NULL ," +
					"`sinr`  varchar(200)  DEFAULT NULL ,`lac_tac`  varchar(200)  DEFAULT NULL ,`cid_pci`  varchar(200)  DEFAULT NULL ,`imei`  varchar(200)  NOT NULL ,`file_path`  varchar(500)  DEFAULT NULL ,`file_index`  varchar(255)  DEFAULT NULL ,`android_ios`  varchar(255)  DEFAULT NULL ,`net_type1`  varchar(200)  DEFAULT NULL ,`signal_strength1`  varchar(200)  DEFAULT NULL ,`sinr1`  varchar(200)  DEFAULT NULL ,`lac_tac1`  varchar(200)  DEFAULT NULL ,`cid_pci1`  varchar(200)  DEFAULT NULL ,`net_type2`  varchar(200)  DEFAULT NULL ,`signal_strength2`  varchar(200)  DEFAULT NULL ,`sinr2`  varchar(200)  DEFAULT NULL ,`lac_tac2`  varchar(200)  DEFAULT NULL ," +
					"`cid_pci2`  varchar(200)  DEFAULT NULL ,`haschange`  int(11) NULL DEFAULT NULL ,`address_info`  varchar(100)  DEFAULT NULL ,`detailreport`  varchar(200)  DEFAULT NULL ,`quick_test_id`  int(200)  DEFAULT NULL ,`operator`  varchar(200)  DEFAULT NULL ,`imsi`  varchar(200)  DEFAULT NULL ,`appid`  varchar(200)  DEFAULT NULL ,`test_description`  varchar(200)  DEFAULT NULL ,`test_location`  varchar(200)  DEFAULT NULL ,`connect_operator`  varchar(200)  DEFAULT NULL ," +
					"`normalornot`  varchar(200)  DEFAULT NULL ,PRIMARY KEY (id),INDEX `lng_lat` (`latitude`),INDEX `lng_lat` (`longitude`) ) CHARSET=utf8 ROW_FORMAT=COMPACT;";
		}
		
		
		
		
		
		String url = dsturl.substring(0, dsturl.lastIndexOf("/") + 1) + ResultsetTestData.basename;
		try {
			start(url, dstuser,dstpassword);
			statement.execute(sql);
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			try {
				close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	
	
	
	
	
	
	
	
	
	
	
	
	public void createDateTab(String newTableName){
		
		String sql = "CREATE TABLE " + newTableName + " ( id int(10) NOT NULL AUTO_INCREMENT," +
				"`confdate`  varchar(11) not null,`tablename`  varchar(50) not null,`newtablename`  varchar(50) not null,`datatype`  varchar(50) not null," +
				" PRIMARY KEY (id) ) CHARSET=utf8 ROW_FORMAT=COMPACT;";
		
		String url = dsturl.substring(0, dsturl.lastIndexOf("/") + 1) + ResultsetTestData.basename;
		try {
			start(url, dstuser,dstpassword);
			statement.execute(sql);
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			try {
				close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
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
	
	/**
	 * 根据拼装好的sql查询appid
	 * @param sql
	 * @return
	 */
	public List getappIds(String sql){
		List list = new ArrayList();
		Connection conn1 = null;
		Statement statement1 = null;
		try {
			String driver = "com.mysql.jdbc.Driver";
			//String url = dsturl.substring(0,dsturl.lastIndexOf("/")+1)+"static_param";
			String url = dsturl;
			System.out.println("******************"+url);
			String user = dstuser;
			// String password = "cmrictpdata";
			String password = dstpassword;
			Class.forName(driver);
			conn1 = DriverManager.getConnection(url, user, password);
			statement1 = conn1.createStatement();
			System.out.println(sql);
			ResultSet rs = statement1.executeQuery(sql);
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
		return list;
	}
	
	/**
	 * 根据code查询数据库名
	 * @param code
	 * @return
	 * @return String
	 */
	public String getBaseName(String code){
		
		String sql = "select basename from code_basename where confcode='"+code+"'";
		System.out.println(sql);
		ResultSet rs = null;
		String basename = "";
		try {
			start(dsturl,dstuser,dstpassword);
			
			rs = statement.executeQuery(sql);
			while(rs.next()){
				basename= rs.getString("basename");
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			try {
				close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return basename;
	}
	
	/**
	 * 根据code查询数据库名
	 * @param code
	 * @return
	 * @return String
	 */
	public boolean queryDataExist (String table,String newtable){
		
		String sql = "select count(*) count from date_tablename where tablename='"+table+"' and newtablename= '"+newtable+"'";
		System.out.println(sql);
		ResultSet rs = null;
		int count = 0;
		boolean fal = false;
		String url = dsturl.substring(0, dsturl.lastIndexOf("/") + 1) + ResultsetTestData.basename;
		try {
			start(url, dstuser, dstpassword);
			
			rs = statement.executeQuery(sql);
			while(rs.next()){
				count = rs.getInt("count");
				if(count>0){
					fal = true;
				}
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			try {
				close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return fal;
	}
	
	/**
	 * 连接数据库，若不存在则创建该数据库
	 * @param code
	 * @param srcBaseName
	 * @return void
	 */
	public void startOrCreateDB( String baseName) {

		// 应首先查询该数据库是否存在
		String url = dsturl.substring(0, dsturl.lastIndexOf("/") + 1) + baseName;
		try {
			start(url, dstuser, dstpassword);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			// 创建相应的数据库
			String sql = "create DATABASE " + baseName;
			try {
				try {
					start(dsturl, dstuser, dstpassword);
				} catch (Exception e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				statement.execute(sql);
				createpro(baseName);
			} catch (SQLException e2) {
				e2.printStackTrace();
			}finally{
				try {
					close();
				} catch (SQLException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}
			try {
				start(url, dstuser, dstpassword);
			} catch (Exception e1) {
				e1.printStackTrace();
			}
		} finally {
			try {
				this.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	public void createpro(String baseName){
		
		String url = dsturl.substring(0, dsturl.lastIndexOf("/") + 1) + baseName;
		String pro1sql  = "CREATE  PROCEDURE `pro_get_scandata_bytablename`(IN `param1` varchar(128),IN `param2` varchar(128),OUT `result` varchar(1024))" +
				"BEGIN  DECLARE i int DEFAULT 0; DECLARE num int; DECLARE temp_count int DEFAULT 0; DECLARE temp_typeno VARCHAR (20) DEFAULT '';" +
				"DECLARE temp_name VARCHAR(100); DECLARE strsSuffix CHAR(1) DEFAULT ','; DECLARE strs VARCHAR(2048) DEFAULT '';" +
				"DECLARE _Table_Cur CURSOR FOR SELECT newtablename,confdate from date_tablename WHERE tablename not LIKE 'pc_%' AND  confdate=param2 ;" +
				"	SELECT count(*) INTO num from date_tablename WHERE tablename not LIKE 'pc_%' AND  confdate=param2  ;OPEN _Table_Cur;while i < num DO " +
				"FETCH _Table_Cur INTO temp_name, temp_typeno; SET @temp_count_insql = 0; IF temp_typeno = '05001' THEN SET @SQLSTR = CONCAT( \"SELECT id into @temp_count_insql from \", temp_name, \" WHERE device_org = ? AND csfb = 0 LIMIT 1\" );" +
				"ELSEIF temp_typeno = '05001.C' THEN SET @SQLSTR = CONCAT(\"SELECT id into @temp_count_insql from \",temp_name,\" WHERE device_org = ? AND csfb = 1 LIMIT 1\"); ELSE  SET @SQLSTR = CONCAT(\"SELECT id into @temp_count_insql from \",temp_name,\" WHERE device_org = ? LIMIT 1\");  END if; IF temp_typeno = '05001.C' THEN set temp_name = \"call_test_new.C\"; END if;" +
				"SET @p = param1; PREPARE tempSTMT FROM @SQLSTR; EXECUTE tempSTMT USING @p; set temp_count = @temp_count_insql; IF temp_count = '' THEN  " +
				"set temp_count = 0; END if; DEALLOCATE PREPARE tempSTMT; IF i = (num - 1) THEN SET strsSuffix = ''; END IF; " +
				"SET strs = CONCAT(strs,CONCAT('\"',SUBSTRING_INDEX(temp_name,CONCAT('_',param2),1),'\":',temp_count),strsSuffix); SET i = i+1;  END WHILE; SET result = strs; CLOSE _Table_Cur; END;";
		
		String pro2sql = "CREATE  PROCEDURE `pro_get_scandata_orgs`(IN `p1` varchar(128),IN `p2` varchar(128),IN `p3` varchar(128),OUT `result` varchar(4096)) " +
				"BEGIN DECLARE str VARCHAR(1024); DECLARE i INT DEFAULT 0; DECLARE strs VARCHAR(4096) DEFAULT '['; DECLARE obj VARCHAR(1024) DEFAULT ''; DECLARE objSuffix CHAR(1) DEFAULT ','; DECLARE count INT DEFAULT LENGTH(p1) - LENGTH(REPLACE(p1,p2,'')); " +
				"DECLARE lastChar CHAR(1) DEFAULT SUBSTR(p1 FROM LENGTH(p1) FOR 1); IF lastChar = p2 THEN set count = count - 1; END IF; WHILE i <= count DO  set @pos = LOCATE(p2,p1);set str = substring_index(p1,p2,1);set p1 = SUBSTR(p1 FROM (@pos + 1) FOR (LENGTH(p1) - (@pos -  1)));" +
				"CALL pro_get_scandata_bytablename(str,p3,@hasdata); IF i = count THEN SET objSuffix = '';END IF;SET obj = CONCAT('{\"',str,'\":{',@hasdata,'}}',objSuffix);SET strs = CONCAT(strs,obj);set i = i + 1;END WHILE;SET result = CONCAT(strs,']');SELECT result;END;" ;
		try {
			start(url, dstuser, dstpassword);
			
			System.out.println(pro1sql);
			System.out.println(pro2sql);
			statement.execute(pro1sql);
			statement.execute(pro2sql);
			
		}catch(Exception e){
			e.printStackTrace();
		}finally {
			try {
				this.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	
}






/*System.out.println("开始准备插入数据************："+new Date().toLocaleString() +"   毫秒："+new Date().getTime());
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
	System.out.println("结束插入数据  开始备份文件************："+new Date().toLocaleString() +"   毫秒："+new Date().getTime());
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
	System.out.println("结束备份************："+new Date().toLocaleString() +"   毫秒："+new Date().getTime());
}*/