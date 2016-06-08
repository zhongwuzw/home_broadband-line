package com.opencassandra.v01.dao.impl;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.math.BigDecimal;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.sf.json.JSONObject;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;

import com.opencassandra.descfile.ConfParser;



public class InsertMysqlDaoByZip{
	Statement statement;
	Connection conn;
	StringBuffer sql = new StringBuffer();
	private Pattern pattern2 = Pattern.compile("\\d+");
	private String[] dateStr = new String[] { "yyyy", "MM", "dd", "HH", "mm",
			"ss", "SSS" };
	private static String ak = ConfParser.ak;
	//数据库配置
	private String call_test_table = "call_test_new";
	private String http_test_table = "http_test_new";
	private String ping_table = "ping_new";
	private String sms_table = "sms_new";
	private String speed_test_table = "speed_test_new";
	private String web_browsing_table = "web_browsing_new";
	private String video_table = "video_test";
	
	public static void main(String[] args) {
//		String gpsStr = "116°20′42.0″E 39°53′42.2″N";
//		String [] str = transGpsPoint(gpsStr);
//		System.out.println(str[0]);
//		System.out.println(str[1]);
//		String result = "";
//		try {
//			result = testPost(str[0], str[1]);
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		System.out.println(subLalo1(result));
		InsertMysqlDaoByZip insertZip = new InsertMysqlDaoByZip();
		//测试获取平均信号强度
//		double maxQuick = 0;
//		double count = 0;
//		count = insertZip.getAvgSignalStrength(123);
//		maxQuick = insertZip.getDataAvg(2,"ping_new","avg_rtt");
//		System.out.println(count);
		//获取网站地址
//		String web_site = insertZip.getBrowsingWebSite(12);
//		System.out.println(web_site);
//		insertZip.updateZipAnalyse(12);
		System.out.println();
	}
	public static void fun1() {// ASCII转换为字符串   
		   
        String s = "32 126";// ASCII码   
   
        String[] chars = s.split(" ");   
        System.out.println("ASCII 汉字 \n----------------------");   
        for (int i = 0; i < chars.length; i++) {   
            System.out.println(chars[i] + " "  
                    + (char) Integer.parseInt(chars[i]));   
        }   
    }   
	public static void fun2() {// 字符串转换为ASCII码   
		   
        String s = "新年快乐！";// 字符串   
   
        char[] chars = s.toCharArray(); // 把字符中转换为字符数组   
   
        System.out.println("\n\n汉字 ASCII\n----------------------");   
        for (int i = 0; i < chars.length; i++) {// 输出结果   
   
            System.out.println(" " + chars[i] + " " + (int) chars[i]);   
        }   
    }   
	/**
     * 判断字符是否是中文
     *
     * @param c 字符
     * @return 是否是中文
     */
    public static boolean isChinese(char c) {
        Character.UnicodeBlock ub = Character.UnicodeBlock.of(c);
        if (ub == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS
                || ub == Character.UnicodeBlock.CJK_COMPATIBILITY_IDEOGRAPHS
                || ub == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS_EXTENSION_A
                || ub == Character.UnicodeBlock.GENERAL_PUNCTUATION
                || ub == Character.UnicodeBlock.CJK_SYMBOLS_AND_PUNCTUATION
                || ub == Character.UnicodeBlock.HALFWIDTH_AND_FULLWIDTH_FORMS) {
            return true;
        }
        return false;
    }
	/**
     * 判断字符串是否是乱码
     *
     * @param strName 字符串
     * @return 是否是乱码
     */
    public static boolean isMessyCode(String strName) {
        Pattern p = Pattern.compile("\\s*|\t*|\r*|\n*");
        Matcher m = p.matcher(strName);
        String after = m.replaceAll("");
        String temp = after.replaceAll("\\p{P}", "");
        char[] ch = temp.trim().toCharArray();
        float chLength = ch.length;
        float count = 0;
        for (int i = 0; i < ch.length; i++) {
            char c = ch[i];
            if (!Character.isLetterOrDigit(c)) {
                if (!isChinese(c)) {
                	return true;
//                    count = count + 1;
                }
            }
        }
        return false;
//        float result = count / chLength;
//        if (result > 0.1) {
//            return true;
//        } else {
//            return false;
//        }

    }
	public void start() {
		String driver = "com.mysql.jdbc.Driver";
		String url = ConfParser.url;
//		String url = "jdbc:mysql://218.206.179.109:3306/testdataanalyse";
		String user = ConfParser.user;
		String password = ConfParser.password;
//		String password = "Bi123456";
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
	public void insertMysqlDB(Map dataMap,File file,String fileType,Integer maxQuick) {
		boolean flag = false;
		StringBuffer sql = new StringBuffer("");
		if(!fileType.isEmpty()){
			if(fileType.equals("zipFile")){
				sql  = insertZipFile(dataMap,fileType,maxQuick);
			}
		}
		if(sql.toString().isEmpty()){
			flag = false;
		}else{
			start();
			try {
				insert(sql.toString());
				flag = true;
				String destFilePath = ConfParser.backReportPath;
				String newfilePath = file.getAbsolutePath();
				File destFile = new File(newfilePath.replace(
						ConfParser.org_prefix, destFilePath));
				File newFile = new File(newfilePath);
				if (!newFile.exists()) {
					System.out.println("文件不存在 无法进行备份等操作");
				} else {
					if (!newFile.exists() && destFile.exists()) {
						System.out.println("文件已备份");
					} else {
						if (destFile.exists()) {
							destFile.delete();
						}
						if (!destFile.exists()) {
							if (!destFile.getParentFile().exists()) {
								destFile.getParentFile().mkdirs();
							}
							destFile.createNewFile();
							FileUtils.copyFile(newFile, destFile);
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
			}
		}
		System.out.println("插入Zip描述文件数据："+flag);
	}
	
	public StringBuffer insertZipFile(Map dataMap,String fileType,Integer maxQuick){
		Map map = new HashMap();
		Set keySet = dataMap.keySet();
		Iterator iter = keySet.iterator();
		map.put("quick_test_id", maxQuick);
		String org = (String)(dataMap.get("org_key")==null?"":dataMap.get("org_key"));
		map.put("org_key", org);
		String test_time = "";
		String province = "";
		String operator = "";
		String description = "";
		String country = "";
		String city = "";
		String terminal_type = "";
		String imei = "";
		String address = "";
		String network_type = "";
		String cellid = "";
		String testing_lat = "";
		String testing_lon = "";
		
		String lac = "";
		String cid = "";
		String pci = "";
		String tac = "";
		String network1_type = "";
		String network2_type = "";
		String cell1 = "";
		String cell2 = "";
		
		boolean flag1 = false;
		boolean flag2 = false;
		
		if(dataMap.containsKey("网络(1)网络制式") || dataMap.containsKey("Network(1) Type") || dataMap.containsKey("Network (1) standard")){
			flag1 = true;
		}
		if(dataMap.containsKey("网络(2)网络制式") || dataMap.containsKey("Network(2) Type") || dataMap.containsKey("Network (1) standard")){
			flag2 = true;
		}
		
		while(iter.hasNext()){
			String key = iter.next()+"";
			if(key.isEmpty()){
				continue;
			}
			if ("Test Time".equals(key)) {
				test_time = (String) (dataMap.get(key) == null ? "" : dataMap
						.get(key));
				// 格式化时间
				String dataLong = "";
				String datatimeStr = test_time;
				Date date1 = null;
				try {
					if (datatimeStr == null || datatimeStr.isEmpty()) {
						dataLong = "";
					} else {
						String test_date = "";
						String testTime = "";
						if(datatimeStr.contains(" ")){
							test_date = datatimeStr.split(" ")[0];
							testTime = datatimeStr.split(" ")[1];
						}
						map.put("test_date", test_date);
						map.put("test_time", testTime);
//						String timeStrs[] = pattern2.split(datatimeStr);
//						StringBuffer rex = new StringBuffer("");
//						for (int j = 0; j < timeStrs.length; j++) {
//							rex.append(timeStrs[j]);
//							rex.append(dateStr[j]);
//						}
//						SimpleDateFormat formatter = new SimpleDateFormat(rex
//								.toString());
//						date1 = formatter.parse(datatimeStr);
//						dataLong = Long.toString(date1.getTime());
					}
				} catch (Exception e) {
					e.printStackTrace();
					System.out.println("转换时间有误");
				}
			}else if("Country".equals(key)){
				country = (String) (dataMap.get(key)==null?"":dataMap.get(key)); 
				map.put("country", country);
			}else if("operator".equals(key.toLowerCase())){
				operator = (String) (dataMap.get(key)==null?"":dataMap.get(key)); 
				map.put("operator", operator);
			}else if("description".equals(key.toLowerCase())){
				description = (String) (dataMap.get(key)==null?"":dataMap.get(key)); 
				map.put("description", description);
			}else if("Province".equals(key)){
				province = (String) (dataMap.get(key)==null?"":dataMap.get(key)); 
				map.put("province", province);
			}else if("terminal type".equals(key.toLowerCase()) || "terminal model".equals(key.toLowerCase())){
				terminal_type = (String) (dataMap.get(key)==null?"":dataMap.get(key)); 
				map.put("terminal_type", terminal_type);
			}else if("City".equals(key)){
				city = (String) (dataMap.get(key)==null?"":dataMap.get(key));
				map.put("city", city);
			}else if("IMEI".equals(key)){
				imei = (String) (dataMap.get(key)==null?"":dataMap.get(key)); 
				map.put("imei", imei);
			}else if("Address".equals(key)){
				address = (String) (dataMap.get(key)==null?"":dataMap.get(key)); 
				map.put("address", address);
			}else if("network type".equals(key.toLowerCase()) || "network model".equals(key.toLowerCase())){
				network_type = (String) (dataMap.get(key)==null?"":dataMap.get(key)); 
			}else if("Cell ID".equals(key)){
				cellid = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("Testing LAT".equals(key)){
				testing_lat = (String) (dataMap.get(key)==null?"":dataMap.get(key));
				map.put("testing_lat", testing_lat);
			}else if("Testing LON".equals(key)){
				testing_lon = (String) (dataMap.get(key)==null?"":dataMap.get(key));
				map.put("testing_lon", testing_lon);
			}else if("LAC".equals(key)){
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
				if(lacCid.indexOf("/")!=-1 || lacCid.indexOf("N/A")!=-1){
					String[] cellInfo = lacCid.split("/");
					lac = cellInfo[0];
					cid = cellInfo[1];
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
				}
			}else if("网络(1)网络制式".equals(key) || "network(1) type".equals(key.toLowerCase()) || "network (1) standard".equals(key.toLowerCase())){
				network1_type = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("网络(2)网络制式".equals(key) || "network(2) type".equals(key.toLowerCase()) || "network (2) standard".equals(key.toLowerCase())){
				network2_type = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("网络(1)小区信息".equals(key) || "network(1) cell".equals(key.toLowerCase()) || "network(1)".equals(key.toLowerCase())){
				cell1 = (String) (dataMap.get(key)==null?"":dataMap.get(key));
				map.put("network1_cell", cell1);
			}else if("网络(2)小区信息".equals(key) || "network(2) cell".equals(key.toLowerCase()) || "network(2)".equals(key.toLowerCase())){
				cell2 = (String) (dataMap.get(key)==null?"":dataMap.get(key));
				map.put("network2_cell", cell2);
			}
			
		}
		//网络(1)信息存在时 当前数据返回的是网络(1)的信息
		if(flag1 && network1_type.equals(network_type)){
			network_type = network1_type;
			cellid = cell1;
		}
		//网络(2)信息存在时 当前数据返回的是网络(2)的信息
		if(flag2 && network2_type.equals(network_type)){
			network_type = network2_type;
			cellid = cell2;
		}
		if(cellid.contains("/")){
			cellid = cellid.split("/")[1];
		}
		if((testing_lon.equals("0") || testing_lon.equals("0.0") || testing_lon.startsWith("0")) && (testing_lat.equals("0") || testing_lat.equals("0.0") || testing_lat.startsWith("0"))){
			map.put("testing_lat", "");
			map.put("testing_lon", "");
		}
		map.put("cellid", cellid);
		map.put("network_type",network_type);
		map.put("network1_type", network1_type);
		map.put("network2_type", network2_type);
		sql = appendSql(map,fileType,maxQuick);
		return sql;
	}
	
	public String insertMysqlDB(Map dataMap,String keyspace,String numType,String dataLong,File file,String detailreport,Integer maxQuick){
		StringBuffer sql = new StringBuffer("");
		Set set = dataMap.keySet();
		Iterator iter = set.iterator();
		while(iter.hasNext()){
			String name = (String)iter.next();
			String value = (String)dataMap.get(name);
			System.out.println(name+":"+value);
		}
		
		String destFilePath = ConfParser.backReportPath;
		String filePath = file.getAbsolutePath();
		
		String path1 = filePath.substring(0, filePath.lastIndexOf("."));
		String filenameorg = path1.substring(path1.lastIndexOf("."), path1.length());
		String []files = new String[]{filenameorg,".detail",".deviceInfo",".monitor"};
		String []filepaths = new String[4];
		boolean flag1 = false;//判定源文件是否被处理
		if(detailreport.equals("")){
			flag1 = false;
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
		}else{
			flag1 = true;
		}
		
		boolean flag = false;
		if(!numType.isEmpty()){
			if(numType.equals("01001")){
				sql  = insertSpeedTest(dataMap,keyspace,numType,dataLong,detailreport,maxQuick);
			}else if(numType.equals("02001")){
				sql = insertWebBrowser(dataMap,keyspace,numType,dataLong,detailreport,maxQuick);
			}else if(numType.equals("02011")){
				sql = insertVideoTest(dataMap,keyspace,numType,dataLong,detailreport,maxQuick);
			}else if(numType.equals("03001") || numType.equals("03011") ){
				sql = insertHTTP(dataMap,keyspace,numType,dataLong,detailreport,maxQuick);
			}else if(numType.equals("04002")){
				sql = insertPING(dataMap,keyspace,numType,dataLong,detailreport,maxQuick);
			}else if(numType.equals("05001")){
				sql = insertCall(dataMap,keyspace,numType,dataLong,detailreport,maxQuick);
			}else if(numType.equals("04006")){
				sql = insertResideTest(dataMap,keyspace,numType,dataLong,detailreport,maxQuick);
			}else if(numType.equals("05002")){
				sql = insertSMS(dataMap,keyspace,numType,dataLong,detailreport,maxQuick);
			}
			
			if(sql.toString().isEmpty()){
				flag = false;
			}else{
				start();
				try {
					insert(sql.toString());
					flag = true;
					if(!flag1){//falg1为true，文件已经备份过不需处理
						for (int i = 0; i < filepaths.length; i++) {
							String newfilePath = filepaths[i];
							File destFile = new File(newfilePath.replace(ConfParser.org_prefix, destFilePath));
							File newFile = new File(newfilePath);
							if(!newFile.exists()){
								continue;
							}
							if(!newFile.exists() && destFile.exists()){
								System.out.println("文件已备份");
							}else{
								if(destFile.exists()){
									destFile.delete();
								}
								if(!destFile.exists()){
									if (!destFile.getParentFile().exists()) {
										destFile.getParentFile().mkdirs();
									}
									destFile.createNewFile();
									FileUtils.copyFile(newFile, destFile);
									newFile.delete();	
								}
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
				}
			}
		}
		System.out.println("更新数据："+flag);
		return detailreport;
	}
	/**
	 * 插入speedTest数据
	 */
	public StringBuffer insertSpeedTest(Map dataMap,String keyspace,String numType,String dataLong,String detailreport,Integer maxQuick){
		List nameList = new ArrayList();
		List valueList = new ArrayList();
		Map map = new HashMap();
		Set keySet = dataMap.keySet();
		Iterator iter = keySet.iterator();
		map.put("quick_test_id", maxQuick);
		
		String imei = (String)(dataMap.get("imei")==null?"":dataMap.get("imei"));
		String file_path = (String)(dataMap.get("filePath")==null?"":dataMap.get("filePath"));
		String detailreportvalue = (String)(dataMap.get("detailreport")==null?"":dataMap.get("detailreport"));
		map.put("detailreport", detailreport);
		map.put("imei",imei);
		map.put("file_path",file_path.replace("\\", "|").replace("//", "|"));
		
		String lac = "";
		String cid = "";
		String tac = "";
		String pci = "";
		String lac_tac = "";
		String cid_pci = "";
		String gpsStr = "";
		String latitude = "";
		String longitude = "";
		String model = "";//终端类型
		String logversion = "";//软件版本
		String device_org = keyspace;//终端分组
		String time = dataLong;//测试时间
		
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
			}else if("LAC".equals(key)){
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
				if(lacCid.indexOf("/")!=-1 || lacCid.indexOf("N/A")!=-1){
					String[] cellInfo = lacCid.split("/");
					lac = cellInfo[0];
					cid = cellInfo[1];
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
				}
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
			if(tac!=null && !tac.equals("")){
				lac1 = tac;
			}
			if(cellInfo[1]!=null && !cellInfo[1].equals("")){
				cid1 = cellInfo[1];
			}
			if(pci!=null && !pci.equals("")){
				cid1 = pci;
			}
			if(tac!=null && !tac.equals("") && cellInfo[0]!=null && !cellInfo[0].equals("")){
				lac1 = cellInfo[0]+"|"+tac;
			}
			if(pci!=null && !pci.equals("") && cellInfo[1]!=null && !cellInfo[1].equals("")){
				cid1 = cellInfo[1]+"|"+pci;
			}
		}
		String lac2 = "";
		String cid2 = "";
		if(cell2.indexOf("/")!=-1 || cell2.indexOf("N/A")!=-1){
			String[] cellInfo = cell2.split("/");
			if(cellInfo[0]!=null && !cellInfo[0].equals("")){
				lac2 = cellInfo[0];
			}
			if(tac!=null && !tac.equals("")){
				lac2 = tac;
			}
			if(cellInfo[1]!=null && !cellInfo[1].equals("")){
				cid2 = cellInfo[1];
			}
			if(pci!=null && !pci.equals("")){
				cid2 = pci;
			}
			if(tac!=null && !tac.equals("") && cellInfo[0]!=null && !cellInfo[0].equals("")){
				lac2 = cellInfo[0]+"|"+tac;
			}
			if(pci!=null && !pci.equals("") && cellInfo[1]!=null && !cellInfo[1].equals("")){
				cid2 = cellInfo[1]+"|"+pci;
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
			if(tac!=null && !tac.equals("")){
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
	public StringBuffer insertWebBrowser(Map dataMap,String keyspace,String numType,String dataLong,String detailreport,Integer maxQuick){
		List nameList = new ArrayList();
		List valueList = new ArrayList();
		Map map = new HashMap();
		Set keySet = dataMap.keySet();
		Iterator iter = keySet.iterator();
		map.put("quick_test_id", maxQuick);
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
		String time = dataLong;//测试时间
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
			}else if("LAC".equals(key)){
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
				if(lacCid.indexOf("/")!=-1 || lacCid.indexOf("N/A")!=-1){
					String[] cellInfo = lacCid.split("/");
					lac = cellInfo[0];
					cid = cellInfo[1];
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
			if(tac!=null && !tac.equals("")){
				lac1 = tac;
			}
			if(cellInfo[1]!=null && !cellInfo[1].equals("")){
				cid1 = cellInfo[1];
			}
			if(pci!=null && !pci.equals("")){
				cid1 = pci;
			}
			if(tac!=null && !tac.equals("") && cellInfo[0]!=null && !cellInfo[0].equals("")){
				lac1 = cellInfo[0]+"|"+tac;
			}
			if(pci!=null && !pci.equals("") && cellInfo[1]!=null && !cellInfo[1].equals("")){
				cid1 = cellInfo[1]+"|"+pci;
			}
		}
		String lac2 = "";
		String cid2 = "";
		if(cell2.indexOf("/")!=-1 || cell2.indexOf("N/A")!=-1){
			String[] cellInfo = cell2.split("/");
			if(cellInfo[0]!=null && !cellInfo[0].equals("")){
				lac2 = cellInfo[0];
			}
			if(tac!=null && !tac.equals("")){
				lac2 = tac;
			}
			if(cellInfo[1]!=null && !cellInfo[1].equals("")){
				cid2 = cellInfo[1];
			}
			if(pci!=null && !pci.equals("")){
				cid2 = pci;
			}
			if(tac!=null && !tac.equals("") && cellInfo[0]!=null && !cellInfo[0].equals("")){
				lac2 = cellInfo[0]+"|"+tac;
			}
			if(pci!=null && !pci.equals("") && cellInfo[1]!=null && !cellInfo[1].equals("")){
				cid2 = cellInfo[1]+"|"+pci;
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
			if(tac!=null && !tac.equals("")){
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
	public StringBuffer insertVideoTest(Map dataMap,String keyspace,String numType,String dataLong,String detailreport,Integer maxQuick){
		List nameList = new ArrayList();
		List valueList = new ArrayList();
		Map map = new HashMap();
		Set keySet = dataMap.keySet();
		Iterator iter = keySet.iterator();
		map.put("quick_test_id", maxQuick);
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
		String time = dataLong;//测试时间
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
				
			}else if("CID".equals(key)){
				String cidStr = (String) (dataMap.get(key)==null?"":dataMap.get(key));
				if(cidStr.indexOf("=")!=-1){
					cid = cidStr.substring(cidStr.indexOf("=")+1,cidStr.length());
				}else{
					cid = cidStr;
				}
			}else if("LAC/CID".equals(key)){
				String lacCid = (String) (dataMap.get(key)==null?"":dataMap.get(key));
				if(lacCid.indexOf("/")!=-1 || lacCid.indexOf("N/A")!=-1){
					String[] cellInfo = lacCid.split("/");
					lac = cellInfo[0];
					cid = cellInfo[1];
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
			if(tac!=null && !tac.equals("")){
				lac1 = tac;
			}
			if(cellInfo[1]!=null && !cellInfo[1].equals("")){
				cid1 = cellInfo[1];
			}
			if(pci!=null && !pci.equals("")){
				cid1 = pci;
			}
			if(tac!=null && !tac.equals("") && cellInfo[0]!=null && !cellInfo[0].equals("")){
				lac1 = cellInfo[0]+"|"+tac;
			}
			if(pci!=null && !pci.equals("") && cellInfo[1]!=null && !cellInfo[1].equals("")){
				cid1 = cellInfo[1]+"|"+pci;
			}
		}
		String lac2 = "";
		String cid2 = "";
		if(cell2.indexOf("/")!=-1 || cell2.indexOf("N/A")!=-1){
			String[] cellInfo = cell2.split("/");
			if(cellInfo[0]!=null && !cellInfo[0].equals("")){
				lac2 = cellInfo[0];
			}
			if(tac!=null && !tac.equals("")){
				lac2 = tac;
			}
			if(cellInfo[1]!=null && !cellInfo[1].equals("")){
				cid2 = cellInfo[1];
			}
			if(pci!=null && !pci.equals("")){
				cid2 = pci;
			}
			if(tac!=null && !tac.equals("") && cellInfo[0]!=null && !cellInfo[0].equals("")){
				lac2 = cellInfo[0]+"|"+tac;
			}
			if(pci!=null && !pci.equals("") && cellInfo[1]!=null && !cellInfo[1].equals("")){
				cid2 = cellInfo[1]+"|"+pci;
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
			if(tac!=null && !tac.equals("")){
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
	public StringBuffer insertHTTP(Map dataMap,String keyspace,String numType,String dataLong,String detailreport,Integer maxQuick){
		List nameList = new ArrayList();
		List valueList = new ArrayList();
		Map map = new HashMap();
		Set keySet = dataMap.keySet();
		Iterator iter = keySet.iterator();
		map.put("quick_test_id", maxQuick);
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
		String time = dataLong;//测试时间
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
			}else if("文件".equals(key) || "File".equals(key) || "upload address".equals(key.toLowerCase())){
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
			}else if("LAC".equals(key)){
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
				if(lacCid.indexOf("/")!=-1 || lacCid.indexOf("N/A")!=-1){
					String[] cellInfo = lacCid.split("/");
					lac = cellInfo[0];
					cid = cellInfo[1];
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
		
		//处理上传类型时url取文件中的值
		if(file_type.equals("上传") || file_type.toLowerCase().equals("upload")){
			map.put("url", url);
		}else{
			if(!testfileurl.isEmpty()){
				map.put("url", testfileurl);
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
			if(tac!=null && !tac.equals("")){
				lac1 = tac;
			}
			if(cellInfo[1]!=null && !cellInfo[1].equals("")){
				cid1 = cellInfo[1];
			}
			if(pci!=null && !pci.equals("")){
				cid1 = pci;
			}
			if(tac!=null && !tac.equals("") && cellInfo[0]!=null && !cellInfo[0].equals("")){
				lac1 = cellInfo[0]+"|"+tac;
			}
			if(pci!=null && !pci.equals("") && cellInfo[1]!=null && !cellInfo[1].equals("")){
				cid1 = cellInfo[1]+"|"+pci;
			}
		}
		String lac2 = "";
		String cid2 = "";
		if(cell2.indexOf("/")!=-1 || cell2.indexOf("N/A")!=-1){
			String[] cellInfo = cell2.split("/");
			if(cellInfo[0]!=null && !cellInfo[0].equals("")){
				lac2 = cellInfo[0];
			}
			if(tac!=null && !tac.equals("")){
				lac2 = tac;
			}
			if(cellInfo[1]!=null && !cellInfo[1].equals("")){
				cid2 = cellInfo[1];
			}
			if(pci!=null && !pci.equals("")){
				cid2 = pci;
			}
			if(tac!=null && !tac.equals("") && cellInfo[0]!=null && !cellInfo[0].equals("")){
				lac2 = cellInfo[0]+"|"+tac;
			}
			if(pci!=null && !pci.equals("") && cellInfo[1]!=null && !cellInfo[1].equals("")){
				cid2 = cellInfo[1]+"|"+pci;
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
			if(tac!=null && !tac.equals("")){
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
	public StringBuffer insertPING(Map dataMap,String keyspace,String numType,String dataLong,String detailreport,Integer maxQuick){
		List nameList = new ArrayList();
		List valueList = new ArrayList();
		Map map = new HashMap();
		Set keySet = dataMap.keySet();
		Iterator iter = keySet.iterator();
		map.put("quick_test_id", maxQuick);
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
		String time = dataLong;//测试时间
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
			}else if("LAC".equals(key)){
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
				if(lacCid.indexOf("/")!=-1 || lacCid.indexOf("N/A")!=-1){
					String[] cellInfo = lacCid.split("/");
					lac = cellInfo[0];
					cid = cellInfo[1];
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
			if(tac!=null && !tac.equals("")){
				lac1 = tac;
			}
			if(cellInfo[1]!=null && !cellInfo[1].equals("")){
				cid1 = cellInfo[1];
			}
			if(pci!=null && !pci.equals("")){
				cid1 = pci;
			}
			if(tac!=null && !tac.equals("") && cellInfo[0]!=null && !cellInfo[0].equals("")){
				lac1 = cellInfo[0]+"|"+tac;
			}
			if(pci!=null && !pci.equals("") && cellInfo[1]!=null && !cellInfo[1].equals("")){
				cid1 = cellInfo[1]+"|"+pci;
			}
		}
		String lac2 = "";
		String cid2 = "";
		if(cell2.indexOf("/")!=-1 || cell2.indexOf("N/A")!=-1){
			String[] cellInfo = cell2.split("/");
			if(cellInfo[0]!=null && !cellInfo[0].equals("")){
				lac2 = cellInfo[0];
			}
			if(tac!=null && !tac.equals("")){
				lac2 = tac;
			}
			if(cellInfo[1]!=null && !cellInfo[1].equals("")){
				cid2 = cellInfo[1];
			}
			if(pci!=null && !pci.equals("")){
				cid2 = pci;
			}
			if(tac!=null && !tac.equals("") && cellInfo[0]!=null && !cellInfo[0].equals("")){
				lac2 = cellInfo[0]+"|"+tac;
			}
			if(pci!=null && !pci.equals("") && cellInfo[1]!=null && !cellInfo[1].equals("")){
				cid2 = cellInfo[1]+"|"+pci;
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
			if(tac!=null && !tac.equals("")){
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
	public StringBuffer insertCall(Map dataMap,String keyspace,String numType,String dataLong,String detailreport,Integer maxQuick){
		List nameList = new ArrayList();
		List valueList = new ArrayList();
		Map map = new HashMap();
		Set keySet = dataMap.keySet();
		Iterator iter = keySet.iterator();
		map.put("quick_test_id", maxQuick);
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
		String time = dataLong;//测试时间
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
			}else if("LAC".equals(key)){
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
				if(lacCid.indexOf("/")!=-1 || lacCid.indexOf("N/A")!=-1){
					String[] cellInfo = lacCid.split("/");
					lac = cellInfo[0];
					cid = cellInfo[1];
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
				}
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
			if(tac!=null && !tac.equals("")){
				lac1 = tac;
			}
			if(cellInfo[1]!=null && !cellInfo[1].equals("")){
				cid1 = cellInfo[1];
			}
			if(pci!=null && !pci.equals("")){
				cid1 = pci;
			}
			if(tac!=null && !tac.equals("") && cellInfo[0]!=null && !cellInfo[0].equals("")){
				lac1 = cellInfo[0]+"|"+tac;
			}
			if(pci!=null && !pci.equals("") && cellInfo[1]!=null && !cellInfo[1].equals("")){
				cid1 = cellInfo[1]+"|"+pci;
			}
		}
		String lac2 = "";
		String cid2 = "";
		if(cell2.indexOf("/")!=-1 || cell2.indexOf("N/A")!=-1){
			String[] cellInfo = cell2.split("/");
			if(cellInfo[0]!=null && !cellInfo[0].equals("")){
				lac2 = cellInfo[0];
			}
			if(tac!=null && !tac.equals("")){
				lac2 = tac;
			}
			if(cellInfo[1]!=null && !cellInfo[1].equals("")){
				cid2 = cellInfo[1];
			}
			if(pci!=null && !pci.equals("")){
				cid2 = pci;
			}
			if(tac!=null && !tac.equals("") && cellInfo[0]!=null && !cellInfo[0].equals("")){
				lac2 = cellInfo[0]+"|"+tac;
			}
			if(pci!=null && !pci.equals("") && cellInfo[1]!=null && !cellInfo[1].equals("")){
				cid2 = cellInfo[1]+"|"+pci;
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
			if(tac!=null && !tac.equals("")){
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
	public StringBuffer insertResideTest(Map dataMap,String keyspace,String numType,String dataLong,String detailreport,Integer maxQuick){
		List nameList = new ArrayList();
		List valueList = new ArrayList();
		Map map = new HashMap();
		Set keySet = dataMap.keySet();
		Iterator iter = keySet.iterator();
		map.put("quick_test_id", maxQuick);
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
		String time = dataLong;//测试时间
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
			}else if("LAC".equals(key)){
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
				if(lacCid.indexOf("/")!=-1 || lacCid.indexOf("N/A")!=-1){
					String[] cellInfo = lacCid.split("/");
					lac = cellInfo[0];
					cid = cellInfo[1];
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
			if(tac!=null && !tac.equals("")){
				lac1 = tac;
			}
			if(cellInfo[1]!=null && !cellInfo[1].equals("")){
				cid1 = cellInfo[1];
			}
			if(pci!=null && !pci.equals("")){
				cid1 = pci;
			}
			if(tac!=null && !tac.equals("") && cellInfo[0]!=null && !cellInfo[0].equals("")){
				lac1 = cellInfo[0]+"|"+tac;
			}
			if(pci!=null && !pci.equals("") && cellInfo[1]!=null && !cellInfo[1].equals("")){
				cid1 = cellInfo[1]+"|"+pci;
			}
		}
		String lac2 = "";
		String cid2 = "";
		if(cell2.indexOf("/")!=-1 || cell2.indexOf("N/A")!=-1){
			String[] cellInfo = cell2.split("/");
			if(cellInfo[0]!=null && !cellInfo[0].equals("")){
				lac2 = cellInfo[0];
			}
			if(tac!=null && !tac.equals("")){
				lac2 = tac;
			}
			if(cellInfo[1]!=null && !cellInfo[1].equals("")){
				cid2 = cellInfo[1];
			}
			if(pci!=null && !pci.equals("")){
				cid2 = pci;
			}
			if(tac!=null && !tac.equals("") && cellInfo[0]!=null && !cellInfo[0].equals("")){
				lac2 = cellInfo[0]+"|"+tac;
			}
			if(pci!=null && !pci.equals("") && cellInfo[1]!=null && !cellInfo[1].equals("")){
				cid2 = cellInfo[1]+"|"+pci;
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
			if(tac!=null && !tac.equals("")){
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
	public StringBuffer insertSMS(Map dataMap,String keyspace,String numType,String dataLong,String detailreport,Integer maxQuick){
		List nameList = new ArrayList();
		List valueList = new ArrayList();
		Map map = new HashMap();
		Set keySet = dataMap.keySet();
		Iterator iter = keySet.iterator();
		map.put("quick_test_id", maxQuick);
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
		String time = dataLong;//测试时间
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
			}else if("LAC".equals(key)){
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
				if(lacCid.indexOf("/")!=-1 || lacCid.indexOf("N/A")!=-1){
					String[] cellInfo = lacCid.split("/");
					lac = cellInfo[0];
					cid = cellInfo[1];
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
			if(tac!=null && !tac.equals("")){
				lac1 = tac;
			}
			if(cellInfo[1]!=null && !cellInfo[1].equals("")){
				cid1 = cellInfo[1];
			}
			if(pci!=null && !pci.equals("")){
				cid1 = pci;
			}
			if(tac!=null && !tac.equals("") && cellInfo[0]!=null && !cellInfo[0].equals("")){
				lac1 = cellInfo[0]+"|"+tac;
			}
			if(pci!=null && !pci.equals("") && cellInfo[1]!=null && !cellInfo[1].equals("")){
				cid1 = cellInfo[1]+"|"+pci;
			}
		}
		String lac2 = "";
		String cid2 = "";
		if(cell2.indexOf("/")!=-1 || cell2.indexOf("N/A")!=-1){
			String[] cellInfo = cell2.split("/");
			if(cellInfo[0]!=null && !cellInfo[0].equals("")){
				lac2 = cellInfo[0];
			}
			if(tac!=null && !tac.equals("")){
				lac2 = tac;
			}
			if(cellInfo[1]!=null && !cellInfo[1].equals("")){
				cid2 = cellInfo[1];
			}
			if(pci!=null && !pci.equals("")){
				cid2 = pci;
			}
			if(tac!=null && !tac.equals("") && cellInfo[0]!=null && !cellInfo[0].equals("")){
				lac2 = cellInfo[0]+"|"+tac;
			}
			if(pci!=null && !pci.equals("") && cellInfo[1]!=null && !cellInfo[1].equals("")){
				cid2 = cellInfo[1]+"|"+pci;
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
			if(tac!=null && !tac.equals("")){
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
	
	private StringBuffer appendSql(Map map,String fileType,Integer quick_test_id){
		StringBuffer sql = new StringBuffer("");
		StringBuffer columnStr = new StringBuffer("(");
		StringBuffer valueStr = new StringBuffer(" values (");
		StringBuffer updateStr = new StringBuffer("");
		String table = "";
		if(fileType.equals("zipFile")){
			table = "quick_test_info";
		}else if(fileType.equals("analyse")){
			table = "quick_test_analyse";
		}else {
			return new StringBuffer("");
		}
		
		boolean flag = this.queryExist(table,quick_test_id);//有数据则为true 执行更新
		if(flag){
			sql.append("update "+table+" set ");
			try {
				Set set = map.keySet();
				Iterator iter = set.iterator();
				while(iter.hasNext()){
					String name = (String)iter.next();
					if(name.isEmpty()){
						continue;
					}
					String value = map.get(name)+"";
					if(value.isEmpty() || value.equals("0") || isMessyCode(value)){
						continue;
					}
					updateStr.append(name +" = '"+value+"', ");
				}
			
				while(updateStr.toString().trim().endsWith(",")){
					updateStr = new StringBuffer(updateStr.toString().substring(0,updateStr.toString().lastIndexOf(",")));
				}
				sql.append(updateStr);
				sql.append("where quick_test_id = '"+quick_test_id+"'");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}else{
			sql.append("insert into "+table+" ");
			try {
				Set set = map.keySet();
				Iterator iter = set.iterator();
				while(iter.hasNext()){
					String name = (String)iter.next();
					if(name.isEmpty()){
						continue;
					}
					String value = map.get(name)+"";
					if(value.isEmpty() || value.equals("0") || isMessyCode(value)){
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
		}else if(numType.equals("03001") || numType.equals("03011") ){
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
		boolean flag = this.queryExist(table,fileIndex);//有数据则为true 执行更新
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
	public Integer queryMaxQuick(String table){
		String sql  = "select max(quick_test_id) max_id from "+table;
		Integer max_quick = 0;
		start();
		try {
			ResultSet rs = (ResultSet)statement.executeQuery(sql);
			if(rs.next()){
				max_quick = rs.getInt("max_id");
				if(max_quick == null){
					max_quick = 0;
				}
			}
		} catch (SQLException e) {
			System.out.println("执行查询最大quick_test_id错误");
			e.printStackTrace();
		}finally{
			try {
				close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return max_quick;
	}
	//判断quick_test_info或者quick_test_analyse表中是否已存在数据
	public boolean queryExist(String table,Integer quick_test_id){
		
		String sql  = "select * from "+table+" where quick_test_id = "+quick_test_id;
		start();
		try {
			ResultSet rs = (ResultSet)statement.executeQuery(sql);
			if(rs.next()){
				return true;
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
		return false;
	}
	public boolean queryExist(String table,String fileIndex){
		
		String sql  = "select * from "+table+" where file_index = '"+fileIndex+"'";
		start();
		try {
			ResultSet rs = (ResultSet)statement.executeQuery(sql);
			if(rs.next()){
				return true;
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
	//处理相同quick_test_id下的数据 更新数据描述表
	public boolean updateZipAnalyse(Integer quick_test_id) {
		Format fm = new DecimalFormat("#.###");//double类型的造型
		boolean flag = false;
		double signals_strength = 0;//信号强度
		signals_strength = getAvgSignalStrength(quick_test_id);
		String browsing_webSite = "";//网站地址
		String browsing_time = "";//网站地址的时延
		browsing_webSite = getBrowsingWebSite(quick_test_id);
		int count = 0;
		double time_sum = 0;
		
		if(!browsing_webSite.isEmpty()){
			String[] webSites = browsing_webSite.split(",");
			String webSiteTime = "";
			for (int i = 1; i < webSites.length; i++) {
				String web_site = webSites[i];
				if(!web_site.isEmpty()){
					double web_site_time = 0;
					double web_site_sumTime = 0;
					double [] dou = this.getBrowsingWebSiteTime(quick_test_id,web_site);
					web_site_time = dou[0];
					web_site_sumTime = dou[1];
					time_sum += web_site_sumTime;
					webSiteTime = fm.format(web_site_time);
					browsing_time += ","+webSiteTime;
				}
			}
			count = this.getBrowsingCount(quick_test_id);
			if(count==0){
				browsing_time = "0"+browsing_time;
			}else{
				browsing_time = fm.format(time_sum/(double)count)+browsing_time;	
			}
		}else{
			browsing_time = "";
		}
		double downloading_time = 0;//下载时间
		double upload_time = 0;//上传时间
		downloading_time = getDownUploadTime("download",quick_test_id);
		upload_time = getDownUploadTime("upload",quick_test_id);
		double video_time = getVideoDelay(quick_test_id);
		double speed_test_downSpeed = getDataAvg(quick_test_id,speed_test_table,"avg_down");
		double speed_test_upSpeed = getDataAvg(quick_test_id,speed_test_table,"avg_up");
		double sinr_avg = getSinrAvg(quick_test_id);
		double ping_delay_avg = getDataAvg(quick_test_id,ping_table,"avg_rtt");

		String downString = fm.format(downloading_time);;
		String upString = fm.format(upload_time);
		String videoString = fm.format(video_time);
		String signalString = new BigDecimal(signals_strength+"").setScale(0, BigDecimal.ROUND_HALF_UP).toString();
		String speedtest_uploading_speed = fm.format(speed_test_upSpeed);
		String speedtest_downloading_speed = fm.format(speed_test_downSpeed);
		String sinrString = fm.format(sinr_avg);
		String pingString = fm.format(ping_delay_avg);
		System.out.println(speed_test_downSpeed+","+speedtest_downloading_speed+","+sinrString);
		
		Map map = new HashMap();
		map.put("signals_strength", signalString);
		map.put("browsing_website", browsing_webSite);
		map.put("browsing_time", browsing_time);
		map.put("downloading_time", downString);
		map.put("uploading_time", upString);
		map.put("video_time", videoString);
		map.put("speedtest_downloading_speed", speedtest_downloading_speed);
		map.put("speedtest_uploading_speed", speedtest_uploading_speed);
		map.put("quick_test_id", quick_test_id);
		map.put("sinr", sinrString);
		map.put("ping_avg", pingString);
		StringBuffer insert_sql = this.appendSql(map, "analyse",quick_test_id);
		System.out.println(signals_strength+",browsing:"+browsing_webSite+",time:"+browsing_time+",down"+downloading_time+",uplaod:"+upload_time+",video:"+video_time+",downspeed:"+speed_test_downSpeed+",uploadspeed:"+speed_test_upSpeed+",quick_testId:"+quick_test_id);
		System.out.println(insert_sql);
		start();
		try {
			statement.execute(insert_sql.toString());
			flag = true;
		} catch (Exception e) {
			e.printStackTrace();
			flag = false;
		}
		return flag;
	}
	
	public Integer getBrowsingCount(Integer quick_test_id){
		Integer result = 0;
		String sql = "select sum(success_counts) sum from "+web_browsing_table+" where quick_test_id = "+quick_test_id+" and success_rate != 0";
		start();
		try {
			ResultSet rs = statement.executeQuery(sql);
			String delay = "";
			if (rs.next()) {
				result = rs.getInt("sum");
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return result;
	}
	public double getVideoDelay(Integer quick_test_id){
		List<Double> list = new ArrayList();
		double avg = 0;
		String sql = "select delay delay,test_times testtimes from "+video_table+" where quick_test_id = "+quick_test_id;
		start();
		try {
			ResultSet rs = statement.executeQuery(sql);
			String delay = "";
			double testTimes = 0;
			while (rs.next()) {
				delay = rs.getString("delay");
				testTimes = rs.getInt("testtimes");
				if(delay!=null && !delay.isEmpty()){
					String [] str = delay.split(",");
					double sum = 0;
					for (int i = 0; i < str.length; i++) {
						double num = Double.parseDouble(str[i]);
						sum += num;
					}
					if(testTimes==0){
						list.add(sum);
					}else{
						list.add(sum/testTimes);
					}
				}
			}
			double sum = 0;
			for (int i = 0; i < list.size(); i++) {
				double val = list.get(i);
				sum += val;
			}
			if(list.size()==0){
				avg = 0.0;
			}else{
				avg = sum/list.size();	
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return avg;
	}
	
	public double getSinrAvg(Integer quick_test_id){
		double call_test_avg = 0;
		double http_test_avg = 0;
		double ping_avg = 0;
		double sms_avg = 0;
		double speed_test_avg = 0;
		double web_browsing_avg = 0;
		double video_test_avg = 0;
		
		call_test_avg = getDataAvg(quick_test_id, call_test_table, "sinr");
		http_test_avg = getDataAvg(quick_test_id, http_test_table, "sinr");
		ping_avg = getDataAvg(quick_test_id, ping_table, "sinr");
		sms_avg = getDataAvg(quick_test_id, sms_table, "sinr");
		speed_test_avg = getDataAvg(quick_test_id, speed_test_table, "sinr");
		web_browsing_avg = getDataAvg(quick_test_id, web_browsing_table, "sinr");
		video_test_avg = getDataAvg(quick_test_id,video_table,"sinr");
		
		double[] signals = new double[7];
		signals[0] = call_test_avg;
		signals[1] = http_test_avg;
		signals[2] = ping_avg;
		signals[3] = sms_avg;
		signals[4] = speed_test_avg;
		signals[5] = web_browsing_avg;
		signals[6] = video_test_avg;
		int count = 0;
		for(int i = 0;i<signals.length;i++){
			double num = signals[i];
			System.out.println(signals[i]);
			if(num==0.0){
				continue;
			}
			count++;
		}
		double sum = call_test_avg+http_test_avg+ping_avg+sms_avg+speed_test_avg+web_browsing_avg+video_test_avg;
		System.out.println("sum:"+sum+",count:"+count);
		if(count==0){
			return 0.0;
		}else{
			double avg = sum/count;
			return avg;	
		}
	}
	
	//根据表名，quick_test_id，以及要查询的字段值（double类型）获取平均值
	public double getDataAvg(Integer quick_test_id,String table,String column){
		String sql = "select avg("+column+") avg from "+ table +" where quick_test_id = "+quick_test_id;
		if(column.equals("sinr")){
			sql = sql + " and sinr != 'NaN'";
		}
		double result = 0;
		start();
		try {
			ResultSet rs = statement.executeQuery(sql);
			if (rs.next()) {
				result = rs.getDouble("avg");
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return result;
	}
	//获取http_test中下载或上传时长
	public double getDownUploadTime(String fileType,Integer quick_test_id){
		double result = 0;
		String sql = "select avg(duration) avg from "+http_test_table+" where quick_test_id = "+quick_test_id+" and file_type = '"+fileType+"' and success_rate != 0";
		start();
		try {
			ResultSet rs = statement.executeQuery(sql);
			if (rs.next()) {
				result = rs.getDouble("avg");
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return result;
	}
	//获取web_browsing下网站地址的平均时延
	public double[] getBrowsingWebSiteTime(Integer quick_test_id, String web_site) {
		String sql = "select avg(full_complete) avg,avg(full_complete*success_counts) avg_sum from "
				+ web_browsing_table + " where quick_test_id = " + quick_test_id + " and web_sit = '"+web_site+"' and success_rate != 0";
		start();
		double dou[] = new double[2];
		double avg = 0;
		double avg_sum = 0;
		try {
			ResultSet rs = statement.executeQuery(sql);
			if (rs.next()) {
				avg = rs.getDouble("avg");
				avg_sum = rs.getDouble("avg_sum");
				dou[0] = avg;
				dou[1] = avg_sum;
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return dou;
	}
	//获取web_browsing下同一quick_test_id的网站地址
	public String getBrowsingWebSite(Integer quick_test_id){
		String sql = "select web_sit website from "+web_browsing_table+" where quick_test_id = "+quick_test_id+" and success_rate != 0 GROUP BY web_sit ";
		start();
		List list = new ArrayList();
		String result = "Overall";
		try {
			ResultSet rs = statement.executeQuery(sql);
			while(rs.next()){
				String web_site = rs.getString("website");
				list.add(web_site);
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
		if(list.size()==0){
			result = "";
		}else{
			for (int i = 0; i < list.size(); i++) {
				String name = list.get(i).toString();
				result += ","+name;
			}
		}
		
		return result;
	}
	//处理同一quick_test_id的信号强度
	public double getAvgSignalStrength(Integer quick_test_id){
		double call_test_avg = 0;
		double http_test_avg = 0;
		double ping_avg = 0;
		double sms_avg = 0;
		double speed_test_avg = 0;
		double web_browsing_avg = 0;
		double video_test_avg = 0;
		
		call_test_avg = getAvgData(call_test_table, quick_test_id);
		http_test_avg = getAvgData(http_test_table, quick_test_id);
		ping_avg = getAvgData(ping_table, quick_test_id);
		sms_avg = getAvgData(sms_table, quick_test_id);
		speed_test_avg = getAvgData(speed_test_table, quick_test_id);
		web_browsing_avg = getAvgData(web_browsing_table, quick_test_id);
		video_test_avg = getAvgData(video_table,quick_test_id);
		
		double[] signals = new double[7];
		signals[0] = call_test_avg;
		signals[1] = http_test_avg;
		signals[2] = ping_avg;
		signals[3] = sms_avg;
		signals[4] = speed_test_avg;
		signals[5] = web_browsing_avg;
		signals[6] = video_test_avg;
		int count = 0;
		for(int i = 0;i<signals.length;i++){
			double num = signals[i];
//			System.out.println(num);
			if(num==0.0){
//				System.out.println("true");
				continue;
			}
			count++;
		}
		double sum = call_test_avg+http_test_avg+ping_avg+sms_avg+speed_test_avg+web_browsing_avg+video_test_avg;
		System.out.println("sum:"+sum+",count:"+count);
		if(count==0){
			return 0.0;
		}else{
			double avg = sum/count;
			return avg;	
		}
	}
	
	public double getAvgData(String table,int quick_test_id){
		String sql = "select avg(signal_strength) avg from "+table+" where quick_test_id = "+quick_test_id;
		start();
		double avg = 0;
		try {
			ResultSet rs = statement.executeQuery(sql);
			if(rs.next()){
				avg = rs.getDouble("avg");
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
		return avg;
	}
	
}
