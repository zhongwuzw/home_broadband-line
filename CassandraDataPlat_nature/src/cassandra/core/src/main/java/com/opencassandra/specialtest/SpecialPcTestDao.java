package com.opencassandra.specialtest;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DecimalFormat;
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

import com.mysql.jdbc.DatabaseMetaData;
import com.opencassandra.descfile.ConfParser;

/**
 * 对pc分支程序的数据库处理
 * 
 * @author：kxc
 * @date：Mar 16, 2016
 */
public class SpecialPcTestDao {
	Statement statement;
	Connection conn;
	StringBuffer sql = new StringBuffer();
	private static String ak = ConfParser.ak;
	private String[] dateStr = new String[] { "yyyy", "MM", "dd", "HH", "mm", "ss", "SSS" };

	// 数据入库的地址
	static String dsturl = ConfParser.dsturl;
	static String dstuser = ConfParser.dstuser;
	static String dstpassword = ConfParser.dstpassword;

	public static void main(String[] args) {
		System.out.println("NULL:" + isNum(""));
	}

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

	/**
	 * sql单条插入
	 * 
	 * @param sql
	 * @return
	 * @return boolean
	 */
	public boolean insert(String sql) {

		System.out.println("sql:" + sql);
		boolean flag = false;
		if (sql.isEmpty()) {
			flag = false;
		} else {
			try {
				flag = statement.execute(sql);
			} catch (SQLException e) {
				e.printStackTrace();
				return false;
			}
		}
		return flag;
	}

	/**
	 * 对字符串是否是数字进行判断
	 * 
	 * @param str
	 * @return
	 * @return boolean
	 */
	public static boolean isNum(String str) {
		return str.matches("^[-+]?(([0-9]+)([.]([0-9]+))?|([.]([0-9]+))?)$");
	}

	/**
	 * 根据标识进行数据的插入
	 * 
	 * @param dataMap
	 * @param numType
	 * @param org
	 * @param file
	 * @return
	 */
	public boolean insertMysqlDB(Map dataMap, String numType, String org, File file, String testtime) {
		StringBuffer sql = new StringBuffer("");
		boolean flag = false;
		if (!numType.isEmpty()) {
			if (numType.equals("01001")) {
				// 生成要插入数据的sql
				sql = insertSpeedTest(dataMap, numType, org, testtime);
			} else if (numType.equals("02001")) {
				sql = insertWebBrowser(dataMap, numType, org, testtime);
			} else if (numType.equals("03001")) {
				sql = insertHTTP(dataMap, numType, org, testtime);
			} else if (numType.equals("04002")) {
				sql = insertPING(dataMap, numType, org, testtime);
			} else if (numType.equals("04003")) {// DNS
				sql = insertDNS(dataMap, numType, org, testtime);
			} else if (numType.equals("04004")) {// Traceroute
				sql = insertTraceroute(dataMap, numType, org, testtime);
			} else if (numType.equals("04005")) {// MTU
				sql = insertMTU(dataMap, numType, org, testtime);
			} else if (numType.equals("04006")) {
				sql = insertResideTest(dataMap, numType, org, testtime);
			} else if (numType.equals("05001")) {
				sql = insertCall(dataMap, numType, org, testtime);
			} else if (numType.equals("05002")) {
				sql = insertSMS(dataMap, numType, org, testtime);
			} else {
				return false;
			}
			if (sql.toString().isEmpty()) {
				flag = false;
			} else {
				String url = dsturl.substring(0, dsturl.lastIndexOf("/") + 1) + SpecialPcTestData.basename;
				try {
					start(url, dstuser, dstpassword);
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
			}
		}
		return flag;
	}

	/**
	 * 对时间格式的处理 date--->time
	 * 
	 * @param timeStr
	 * @return
	 * @return String
	 */
	public String getNewDate(String timeStr) {
		Pattern pattern2 = Pattern.compile("\\d+");
		String dataLong = "";
		String datatimeStr = timeStr;
		Date date1 = null;
		try {
			if (datatimeStr == null || datatimeStr.isEmpty()) {
				dataLong = "";
			} else {
				String timeStrs[] = pattern2.split(datatimeStr);
				StringBuffer rex = new StringBuffer("");
				for (int j = 0; j < timeStrs.length; j++) {
					rex.append(timeStrs[j]);
					rex.append(dateStr[j]);
				}
				SimpleDateFormat formatter = new SimpleDateFormat(rex.toString());
				date1 = formatter.parse(datatimeStr);
				dataLong = Long.toString(date1.getTime());
			}

		} catch (ParseException e) {
			e.printStackTrace();
			System.out.println("转换时间有误");
		}
		return dataLong;
	}

	/**
	 * 插入speedTest数据
	 * 
	 * @param dataMap
	 * @param numType
	 * @param org
	 * @param testtime
	 * @return
	 * @return StringBuffer
	 */
	public StringBuffer insertSpeedTest(Map dataMap, String numType, String org, String testtime) {
		List nameList = new ArrayList();
		List valueList = new ArrayList();
		Map map = new HashMap();
		Set keySet = dataMap.keySet();
		Iterator iter = keySet.iterator();
		String file_path = (String) (dataMap.get("filePath") == null ? "" : dataMap.get("filePath"));
		String file_index = (String) dataMap.get("file_index");
		map.put("file_index", file_index);
		map.put("file_path", file_path.replace("\\", "|").replace("//", "|"));

		String pc_org = org;
		map.put("device_org", pc_org);
		String start_time = "";// 测试开始时间
		String end_time = "";// 测试结束时间
		String protocol = "";// 协议类型
		String rate_type = "";// 速率类型
		String max_rate_up = "";// 最大上行速率
		String max_rate_down = "";// 最大下行速率
		String avg_rate_up = "";// 最大上行平均速率
		String avg_rate_down = "";// 最大下行平均速率
		String delay = "";// 时延
		String delay_packet_size = "";// 时延包大小
		String buffer_size = "";// 缓冲区
		String timeout = "";// 网络超时
		String number_of_links = "";// 测试线程数配置
		String test_description = "";
		String operator = "";
		String bandwidth = "";
		String consumerid = "";// 用户id
		String businessid = "";// 业务id
		String mac = "";// MAC地址

		while (iter.hasNext()) {
			String key = iter.next() + "";
			if (key == null || key.isEmpty()) {
				continue;
			}

			if ("测试开始".equals(key)) {
				start_time = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("start_time", getNewDate(start_time));
			} else if ("测试结束".equals(key)) {
				end_time = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("end_time", getNewDate(end_time));
			} else if ("协议类型".equals(key)) {
				protocol = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("protocol", protocol);
			} else if ("速率类型".equals(key)) {
				rate_type = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("rate_type", rate_type);
			} else if ("时延(ms)".equals(key)) {
				delay = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				if (delay.equals("--") || delay.equals("N/A") || delay.isEmpty()) {
					delay = "";
				}
				map.put("delay", delay);
			} else if ("时延包大小(byte)".equals(key)) {
				delay_packet_size = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("delay_packet_size", delay_packet_size);
			} else if ("缓冲区(byte)".equals(key)) {
				buffer_size = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("buffer_size", buffer_size);
			} else if ("上行最大速率(Mbps)".equals(key)) {
				max_rate_up = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("max_rate_up", max_rate_up);
			} else if ("下行最大速率(Mbps)".equals(key)) {
				max_rate_down = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("max_rate_down", max_rate_down);
			} else if ("上行平均速率(Mbps)".equals(key)) {
				avg_rate_up = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("avg_rate_up", avg_rate_up);
			} else if ("下行平均速率(Mbps)".equals(key)) {
				avg_rate_down = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("avg_rate_down", avg_rate_down);
			} else if ("网络超时(ms)".equals(key)) {
				timeout = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("timeout", timeout);
			} else if ("测试线程数配置".equals(key)) {
				number_of_links = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("number_of_links", number_of_links);
			} else if ("测试地点".equals(key)) {
				test_description = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("test_description", test_description);
			} else if ("运营商".equals(key)) {
				operator = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("operator", operator);
			} else if ("签约带宽".equals(key)) {
				bandwidth = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("bandwidth", bandwidth);
			} else if ("用户ID".equals(key)) {
				consumerid = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("consumerid", consumerid);
			} else if ("业务ID".equals(key)) {
				businessid = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("businessid", businessid);
			} else if ("MAC1".equals(key)) {
				mac = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("mac", mac);
			}
		}
		sql = appendSql(map, numType, file_index, testtime);
		return sql;
	}

	/**
	 * 插入WebBrowser数据
	 * 
	 * @param dataMap
	 * @param numType
	 * @param org
	 * @param testtime
	 * @return
	 * @return StringBuffer
	 */
	public StringBuffer insertWebBrowser(Map dataMap, String numType, String org, String testtime) {
		List nameList = new ArrayList();
		List valueList = new ArrayList();
		Map map = new HashMap();
		Set keySet = dataMap.keySet();
		Iterator iter = keySet.iterator();
		String file_path = (String) (dataMap.get("filePath") == null ? "" : dataMap.get("filePath"));
		String file_index = (String) dataMap.get("file_index");
		map.put("file_index", file_index);
		map.put("file_path", file_path.replace("\\", "|").replace("//", "|"));

		String pc_org = org;
		map.put("device_org", pc_org);
		String app_id = (String) dataMap.get("app_id");
		map.put("app_id",app_id);
		String start_time = "";// 测试开始时间
		String end_time = "";// 测试结束时间

		String test_location = "";
		String operator = "";
		String contracted_bandwidth = "";

		String user_id = "";// 用户id
		String service_id = "";// 业务id

		String computer_name = "";// 计算机名称d
		String os_name = "";// 系统名称
		String cpu = "";// 处理器
		String system_type = "";// 系统类型

		String times = "";// 测试次数
		String avg_delay = "";// 平均80%时延
		String avg_success_rate = "";// 平均成功率

		
		String ip1 = "";//ip1
		String ip2 = "";
		String ip3 = "";
		String mac1 = "";
		String mac2 = "";
		String mac3 = "";
		
		while (iter.hasNext()) {
			String key = iter.next() + "";
			if (key == null || key.isEmpty()) {
				continue;
			}
			if ("测试开始".equals(key)) {
				start_time = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("start_time", getNewDate(start_time));
			} else if ("测试结束".equals(key)) {
				end_time = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("end_time", getNewDate(end_time));
			} else if ("测试地点".equals(key)) {
				test_location = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("test_location", test_location);
			} else if ("运营商".equals(key)) {
				operator = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("operator", operator);
			} else if ("签约带宽".equals(key)) {
				contracted_bandwidth = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("contracted_bandwidth", contracted_bandwidth);
			} else if ("用户ID".equals(key)) {
				user_id = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("user_id", user_id);
			} else if ("业务ID".equals(key)) {
				service_id = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("service_id", service_id);
			}  else if ("计算机名称".equals(key)) {
				computer_name = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("computer_name", computer_name);
			} else if ("系统名称".equals(key)) {
				os_name = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("os_name", os_name);
			} else if ("处理器".equals(key)) {
				cpu = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("cpu", cpu);
			} else if ("系统类型".equals(key)) {
				system_type = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("system_type", system_type);
			} else if ("平均80%时延".equals(key)) {
				avg_delay = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("avg_delay", avg_delay);
			} else if ("测试次数".equals(key)) {
				times = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("times", times);
			} else if ("平均成功率".equals(key)) {
				avg_success_rate = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("avg_success_rate", avg_success_rate);
			}else if ("MAC1".equals(key)) {
				mac1 = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("mac1", mac1);
			} else if ("MAC2".equals(key)) {
				mac2 = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("mac2", mac2);
			} else if ("MAC3".equals(key)) {
				mac3 = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("mac3", mac3);
			} else if ("IP1".equals(key)) {
				ip1 = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("ip1", ip1);
			} else if ("IP2".equals(key)) {
				ip2 = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("ip2", ip2);
			} else if ("IP3".equals(key)) {
				ip3 = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("ip3", ip3);
			} 
		}
		map.put("mac1", mac1);
		sql = appendSql(map, numType, file_index, testtime);
		return sql;
	}

	/**
	 * 插入HTTP数据
	 * 
	 * @param dataMap
	 * @param numType
	 * @param org
	 * @param testtime
	 * @return
	 * @return StringBuffer
	 */
	public StringBuffer insertHTTP(Map dataMap, String numType, String org, String testtime) {
		List nameList = new ArrayList();
		List valueList = new ArrayList();
		Map map = new HashMap();
		Set keySet = dataMap.keySet();
		Iterator iter = keySet.iterator();
		String file_path = (String) (dataMap.get("filePath") == null ? "" : dataMap.get("filePath"));
		String file_index = (String) dataMap.get("file_index");
		map.put("file_index", file_index);
		map.put("file_path", file_path.replace("\\", "|").replace("//", "|"));

		String pc_org = org;
		map.put("device_org", pc_org);
		String start_time = "";// 测试开始时间
		String end_time = "";// 测试结束时间
		String type = "";// 业务类型
		String transfer_progress = "";// 传输进度
		String avg_speed = "";// 平均速率（Mbps）
		String file_size = "";// 文件大小(MB)
		String duration = "";// 时长(ms)
		String max_speed = "";// 最大速率（Mbps）
		String min_speed = "";// 最小速率（Mbps）
		String link_out_time = "";// 网络连接超时（ms）

		String test_description = "";
		String operator = "";
		String bandwidth = "";
		String consumerid = "";// 用户id
		String businessid = "";// 业务id
		String mac = "";// MAC地址

		while (iter.hasNext()) {
			String key = iter.next() + "";
			if (key == null || key.isEmpty()) {
				continue;
			}
			if ("测试开始".equals(key)) {
				start_time = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("start_time", getNewDate(start_time));
			} else if ("测试结束".equals(key)) {
				end_time = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("end_time", getNewDate(end_time));
			} else if ("业务类型".equals(key)) {
				type = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("service_type", type);
			} else if ("传输进度".equals(key)) {
				transfer_progress = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("transfer_progress", transfer_progress);
			} else if ("平均速率(Mbps)".equals(key)) {
				avg_speed = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("overall_speed", avg_speed);
			} else if ("文件大小(MB)".equals(key)) {
				file_size = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("resource_size", file_size);
			} else if ("时长(ms)".equals(key)) {
				duration = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("transfer_duration", duration);
			} else if ("最大速率(Mbps)".equals(key)) {
				max_speed = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("max_rate", max_speed);
			} else if ("最小速率(Mbps)".equals(key)) {
				min_speed = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("min_rate", min_speed);
			} else if ("网络连接超时(ms)".equals(key)) {
				link_out_time = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("network_timeout", link_out_time);
			} else if ("测试地点".equals(key)) {
				test_description = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("test_description", test_description);
			} else if ("运营商".equals(key)) {
				operator = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("operator", operator);
			} else if ("签约带宽".equals(key)) {
				bandwidth = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("bandwidth", bandwidth);
			} else if ("用户ID".equals(key)) {
				consumerid = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("consumerid", consumerid);
			} else if ("业务ID".equals(key)) {
				businessid = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("businessid", businessid);
			} else if ("MAC1".equals(key)) {
				mac = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("mac", mac);
			}
		}
		sql = appendSql(map, numType, file_index, testtime);
		return sql;
	}

	/**
	 * 插入PING数据
	 * 
	 * @param dataMap
	 * @param numType
	 * @param org
	 * @param testtime
	 * @return
	 * @return StringBuffer
	 */
	public StringBuffer insertPING(Map dataMap, String numType, String org, String testtime) {
		List nameList = new ArrayList();
		List valueList = new ArrayList();
		Map map = new HashMap();
		Set keySet = dataMap.keySet();
		Iterator iter = keySet.iterator();
		String file_path = (String) (dataMap.get("filePath") == null ? "" : dataMap.get("filePath"));
		String file_index = (String) dataMap.get("file_index");
		map.put("file_index", file_index);
		map.put("file_path", file_path.replace("\\", "|").replace("//", "|"));

		String pc_org = org;
		map.put("device_org", pc_org);
		
		String app_id = (String) dataMap.get("app_id");
		map.put("app_id",app_id);
		
		String start_time = "";// 测试开始时间
		String end_time = "";// 测试结束时间
		String location = "";// 归属地
		String times = "";// 测试次数
		String avg_delay = "";// 平均时延

		String test_location = "";
		String operator = "";
		String contracted_bandwidth = "";

		String user_id = "";// 用户id
		String service_id = "";// 业务id

		String computer_name = "";// 计算机名称d
		String os_name = "";// 系统名称
		String cpu = "";// 处理器
		String system_type = "";// 系统类型
		String further_request = "";// 是否持续请求
		String echo_request = "";// 回显请求书
		String package_size = "";// 发送包大小
		String section = "";// 是否分段
		String ttl = "";// 生存时间（TTL）
		String resolutionhost_name = "";// 地址解析为主机名
		String routing_number = "";// 技术活跃点路由个数
		String hops_timestamp = "";// 计数活跃点时间戳个数
		String overtime = "";// 才超时时延
		String ip_type = "";// IP地址类型
		String loose_source_route = "";// 松散源路由
		String strict_source_route = "";// 严格源路由
		String test_reverse_address = "";// 是否测试反向路由
		String use_source_address = "";// 要使用的源地址
		String type_of_service = "";// 服务类型(TOS)
		String replays = "";// Replays
		String sleep_time = "";// SleepTime
		String loss_rate = "";// 丢包率

		String ip1 = "";//ip1
		String ip2 = "";
		String ip3 = "";
		String mac1 = "";
		String mac2 = "";
		String mac3 = "";
		
		
		while (iter.hasNext()) {
			String key = iter.next() + "";
			if (key == null || key.isEmpty()) {
				continue;
			}
			if ("测试开始".equals(key)) {
				start_time = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("start_time", getNewDate(start_time));
			} else if ("测试结束".equals(key)) {
				end_time = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("end_time", getNewDate(end_time));
			} else if ("归属地".equals(key)) {
				location = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("location", location);
			} else if ("测试次数".equals(key)) {
				times = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("times", times);
			} else if ("测试地点".equals(key)) {
				test_location = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("test_location", test_location);
			} else if ("运营商".equals(key)) {
				operator = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("operator", operator);
			} else if ("签约带宽".equals(key)) {
				contracted_bandwidth = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("contracted_bandwidth", contracted_bandwidth);
			} else if ("用户ID".equals(key)) {
				user_id = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("user_id", user_id);
			} else if ("业务ID".equals(key)) {
				service_id = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("service_id", service_id);
			}  else if ("计算机名称".equals(key)) {
				computer_name = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("computer_name", computer_name);
			} else if ("系统名称".equals(key)) {
				os_name = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("os_name", os_name);
			} else if ("处理器".equals(key)) {
				cpu = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("cpu", cpu);
			} else if ("系统类型".equals(key)) {
				system_type = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("system_type", system_type);
			} else if ("是否持续请求".equals(key)) {
				further_request = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("further_request", further_request);
			} else if ("回显请求数".equals(key)) {
				echo_request = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("echo_request", echo_request);
			} else if ("发送包大小".equals(key)) {
				package_size = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("package_size", package_size);
			} else if ("平均时延".equals(key)) {
				avg_delay = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("avg_delay", avg_delay);
			}  else if ("是否分段".equals(key)) {
				section = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("section", section);
			} else if ("生存时间(TTL)".equals(key)) {
				ttl = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("ttl", ttl);
			} else if ("地址解析为主机名".equals(key)) {
				resolutionhost_name = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("resolutionhost_name", resolutionhost_name);
			} else if ("计数活跃点路由个数".equals(key)) {
				routing_number = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("routing_number", routing_number);
			} else if ("计数跃点时间戳个数".equals(key)) {
				hops_timestamp = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("hops_timestamp", hops_timestamp);
			} else if ("超时时延".equals(key)) {
				overtime = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("overtime", overtime);
			} else if ("IP地址类型".equals(key)) {
				ip_type = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("ip_type", ip_type);
			} else if ("松散源路由".equals(key)) {
				loose_source_route = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("loose_source_route", loose_source_route);
			} else if ("严格源路由".equals(key)) {
				strict_source_route = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("strict_source_route", strict_source_route);
			} else if ("是否测试反向路由".equals(key)) {
				test_reverse_address = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("test_reverse_address", test_reverse_address);
			} else if ("要使用的源地址".equals(key)) {
				use_source_address = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("use_source_address", use_source_address);
			} else if ("服务类型(TOS)".equals(key)) {
				type_of_service = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("type_of_service", type_of_service);
			} else if ("Replays".equals(key)) {
				replays = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("replays", replays);
			} else if ("SleepTime".equals(key)) {
				sleep_time = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("sleep_time", sleep_time);
			} else if ("丢包率".equals(key)) {
				loss_rate = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("loss_rate", loss_rate);
			}else if ("MAC1".equals(key)) {
				mac1 = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("mac1", mac1);
			} else if ("MAC2".equals(key)) {
				mac2 = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("mac2", mac2);
			} else if ("MAC3".equals(key)) {
				mac3 = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("mac3", mac3);
			} else if ("IP1".equals(key)) {
				ip1 = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("ip1", ip1);
			} else if ("IP2".equals(key)) {
				ip2 = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("ip2", ip2);
			} else if ("IP3".equals(key)) {
				ip3 = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("ip3", ip3);
			} 
		}
		map.put("mac1", mac1);
		sql = appendSql(map, numType, file_index, testtime);
		return sql;
	}

	/**
	 * 插入DNS数据
	 * 
	 * @param dataMap
	 * @param numType
	 * @param org
	 * @param testtime
	 * @return
	 * @return StringBuffer
	 */
	public StringBuffer insertDNS(Map dataMap, String numType, String org, String testtime) {
		List nameList = new ArrayList();
		List valueList = new ArrayList();
		Map map = new HashMap();
		Set keySet = dataMap.keySet();
		Iterator iter = keySet.iterator();
		String file_path = (String) (dataMap.get("filePath") == null ? "" : dataMap.get("filePath"));
		String file_index = (String) dataMap.get("file_index");
		map.put("file_index", file_index);
		map.put("file_path", file_path.replace("\\", "|").replace("//", "|"));

		String pc_org = org;
		map.put("device_org", pc_org);
		String start_time = "";// 测试开始时间
		String end_time = "";// 测试结束时间
		String target_address = "";// 目标地址
		String nick_name = "";// 别名
		String ip_address = "";// ip地址
		String dns_server = "";// dns服务器
		String server_address = "";// DNS服务器地址
		String delay = "";// 解析时长

		String test_description = "";
		String operator = "";
		String bandwidth = "";

		String consumerid = "";// 用户id
		String businessid = "";// 业务id
		String mac = "";// MAC地址
		while (iter.hasNext()) {
			String key = iter.next() + "";
			if (key == null || key.isEmpty()) {
				continue;
			}
			if ("测试开始".equals(key)) {
				start_time = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("start_time", getNewDate(start_time));
			} else if ("测试结束".equals(key)) {
				end_time = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("end_time", getNewDate(end_time));
			} else if ("目标地址".equals(key)) {
				target_address = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("target_address", target_address);
			} else if ("IP地址".equals(key)) {
				ip_address = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("ip_address", ip_address);
			} else if ("别名".equals(key)) {
				nick_name = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("nick_name", nick_name);
			} else if ("DNS服务器".equals(key)) {
				dns_server = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("dns_server", dns_server);
			} else if ("DNS服务器地址".equals(key)) {
				server_address = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("server_address", server_address);
			} else if ("解析时长(ms)".equals(key.toLowerCase()) || "解析时长（ms）".equals(key.toLowerCase())) {
				delay = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				if (isNum(delay)) {
					map.put("delay", delay);
				} else {

				}

			} else if ("测试地点".equals(key)) {
				test_description = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("test_description", test_description);
			} else if ("运营商".equals(key)) {
				operator = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("operator", operator);
			} else if ("签约带宽".equals(key)) {
				bandwidth = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("bandwidth", bandwidth);
			} else if ("用户ID".equals(key)) {
				consumerid = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("consumerid", consumerid);
			} else if ("业务ID".equals(key)) {
				businessid = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("businessid", businessid);
			} else if ("MAC1".equals(key)) {
				mac = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("mac", mac);
			}
		}
		sql = appendSql(map, numType, file_index, testtime);
		return sql;
	}

	/**
	 * 插入Traceroute数据
	 * 
	 * @param dataMap
	 * @param numType
	 * @param org
	 * @param testtime
	 * @return
	 * @return StringBuffer
	 */
	public StringBuffer insertTraceroute(Map dataMap, String numType, String org, String testtime) {
		List nameList = new ArrayList();
		List valueList = new ArrayList();
		Map map = new HashMap();
		Set keySet = dataMap.keySet();
		Iterator iter = keySet.iterator();
		String file_path = (String) (dataMap.get("filePath") == null ? "" : dataMap.get("filePath"));
		String file_index = (String) dataMap.get("file_index");
		map.put("file_index", file_index);
		map.put("file_path", file_path.replace("\\", "|").replace("//", "|"));

		String pc_org = org;
		map.put("device_org", pc_org);
		String start_time = "";// 测试开始时间
		String end_time = "";// 测试结束时间
		String hop = "";// Hop
		String total_duration = "";// 总时长
		String success_fail = "";// 是否成功
		String max_num_of_hops = "";// 最大跳数
		String timeout = "";// 超时时间（ms）

		String test_description = "";
		String operator = "";
		String bandwidth = "";
		String consumerid = "";// 用户id
		String businessid = "";// 业务id
		String mac = "";// MAC地址

		while (iter.hasNext()) {
			String key = iter.next() + "";
			if (key == null || key.isEmpty()) {
				continue;
			}
			if ("测试开始".equals(key)) {
				start_time = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("start_time", getNewDate(start_time));
			} else if ("测试结束".equals(key)) {
				end_time = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("end_time", getNewDate(end_time));
			} else if ("Hop".equals(key)) {
				hop = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("hop", hop);
			} else if ("总时长(ms)".equals(key)) {
				total_duration = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("total_duration", total_duration);
			} else if ("是否成功".equals(key)) {
				success_fail = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("success_fail", success_fail);
			} else if ("最大跳数".equals(key)) {
				max_num_of_hops = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("max_num_of_hops", max_num_of_hops);
			} else if ("超时时间(ms)".equals(key)) {
				timeout = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("timeout", timeout);
			} else if ("测试地点".equals(key)) {
				test_description = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("test_description", test_description);
			} else if ("运营商".equals(key)) {
				operator = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("operator", operator);
			} else if ("签约带宽".equals(key)) {
				bandwidth = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("bandwidth", bandwidth);
			} else if ("用户ID".equals(key)) {
				consumerid = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("consumerid", consumerid);
			} else if ("业务ID".equals(key)) {
				businessid = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("businessid", businessid);
			} else if ("MAC1".equals(key)) {
				mac = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("mac", mac);
			}
		}
		sql = appendSql(map, numType, file_index, testtime);
		return sql;
	}

	/**
	 * 插入MTU数据
	 * 
	 * @param dataMap
	 * @param numType
	 * @param org
	 * @param testtime
	 * @return
	 * @return StringBuffer
	 */
	public StringBuffer insertMTU(Map dataMap, String numType, String org, String testtime) {
		List nameList = new ArrayList();
		List valueList = new ArrayList();
		Map map = new HashMap();
		Set keySet = dataMap.keySet();
		Iterator iter = keySet.iterator();
		String file_path = (String) (dataMap.get("filePath") == null ? "" : dataMap.get("filePath"));
		String file_index = (String) dataMap.get("file_index");
		map.put("file_index", file_index);
		map.put("file_path", file_path.replace("\\", "|").replace("//", "|"));

		String pc_org = org;
		map.put("device_org", pc_org);
		String start_time = "";// 测试开始时间
		String end_time = "";// 测试结束时间
		String target_address = "";// 目标地址
		String ip_address = "";// ip地址
		String location = "";// 归属地
		String min_mtu = "";// 最小mtu
		String delay = "";// 时延
		String timeout = "";// 超时时间
		String max_package_size = "";// 最大包长
		String min_package_size = "";// 最小包长

		String test_description = "";
		String operator = "";
		String bandwidth = "";

		String consumerid = "";// 用户id
		String businessid = "";// 业务id
		String mac = "";// MAC地址

		while (iter.hasNext()) {
			String key = iter.next() + "";
			if (key == null || key.isEmpty()) {
				continue;
			}
			if ("测试开始".equals(key)) {
				start_time = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("start_time", getNewDate(start_time));
			} else if ("测试结束".equals(key)) {
				end_time = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("end_time", getNewDate(end_time));
			} else if ("目标地址".equals(key)) {
				target_address = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("target_address", target_address);
			} else if ("IP地址".equals(key)) {
				ip_address = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("ip_address", ip_address);
			} else if ("归属地".equals(key)) {
				location = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("location", location);
			} else if ("最小MTU值".equals(key)) {
				min_mtu = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("min_mtu", min_mtu);
			} else if ("时延(ms)".equals(key)) {
				delay = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				if (delay.equals("--") || delay.equals("N/A") || delay.isEmpty()) {
					delay = "";
				}
				map.put("delay", delay);
			} else if ("超时时间(ms)".equals(key)) {
				timeout = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("timeout", timeout);
			} else if ("最大包长(byte)".equals(key)) {
				max_package_size = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("max_package_size", max_package_size);
			} else if ("最小包长(byte)".equals(key)) {
				min_package_size = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("min_package_size", min_package_size);
			} else if ("测试地点".equals(key)) {
				test_description = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("test_description", test_description);
			} else if ("运营商".equals(key)) {
				operator = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("operator", operator);
			} else if ("签约带宽".equals(key)) {
				bandwidth = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("bandwidth", bandwidth);
			} else if ("用户ID".equals(key)) {
				consumerid = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("consumerid", consumerid);
			} else if ("业务ID".equals(key)) {
				businessid = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("businessid", businessid);
			} else if ("MAC1".equals(key)) {
				mac = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("mac", mac);
			}
		}
		sql = appendSql(map, numType, file_index, testtime);
		return sql;
	}

	/**
	 * 插入call数据
	 * 
	 * @param dataMap
	 * @param numType
	 * @param org
	 * @param testtime
	 * @return
	 * @return StringBuffer
	 */
	public StringBuffer insertCall(Map dataMap, String numType, String org, String testtime) {
		List nameList = new ArrayList();
		List valueList = new ArrayList();
		Map map = new HashMap();
		Set keySet = dataMap.keySet();
		Iterator iter = keySet.iterator();
		String file_path = (String) (dataMap.get("filePath") == null ? "" : dataMap.get("filePath"));
		String file_index = (String) dataMap.get("file_index");
		map.put("file_index", file_index);
		map.put("file_path", file_path.replace("\\", "|").replace("//", "|"));

		String longitude = "";
		String latitude = "";
		String province = "";// 测试省
		String district = "";// 测试district
		String city = "";// 测试city
		String model = "";// 终端类型
		String logversion = "";// 软件版本

		String setup_delay = "";
		String recording = "";
		String phone_number = "";
		String target_number = "";
		String network = "";
		String hand_over_situation = "";
		String success_failure = "";
		String fallbacktocs = "";
		String backtolte = "";

		String location = "";// 测试地点
		String gpsStr = "";
		String net_type = "";// 网络类型
		String signal_strength = "";// 信号强度
		String sinr = "";// SINR
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
		String wifi_ss = "";// WIFI SIGNAL STRENGTH
		String consumerid = "";// 用户id
		String businessid = "";// 业务id
		String mac = "";// MAC地址
		boolean flag1 = false;
		boolean flag2 = false;

		String spaceGpsStr = "";// 测试地点
		// 获取GPS位置信息----start
		if (dataMap.containsKey("GPS信息")) {
			gpsStr = (String) (dataMap.get("GPS信息") == null ? "" : dataMap.get("GPS信息"));
		} else if (dataMap.containsKey("GPS位置信息")) {
			gpsStr = (String) (dataMap.get("GPS位置信息") == null ? "" : dataMap.get("GPS位置信息"));
		} else if (dataMap.containsKey("GPS位置")) {
			gpsStr = (String) (dataMap.get("GPS位置") == null ? "" : dataMap.get("GPS位置"));
		} else if (dataMap.containsKey("测试位置")) {
			gpsStr = (String) (dataMap.get("测试位置") == null ? "" : dataMap.get("测试位置"));
		} else if (dataMap.containsKey("测试GPS位置")) {
			gpsStr = (String) (dataMap.get("测试GPS位置") == null ? "" : dataMap.get("测试GPS位置"));
		} else {
			gpsStr = "";
		}
		map.put("gps", gpsStr);
		if (gpsStr.contains(" ")) {
			String[] gps = transGpsPoint(gpsStr);
			if (gps != null && gps[0] != null && gps[1] != null) {
				longitude = gps[0];
				latitude = gps[1];
			}
		}
		map.put("longitude", longitude);
		map.put("latitude", latitude);
		// 获取GPS位置信息----end
		if (dataMap.containsKey("网络(1)网络制式") || dataMap.containsKey("Network(1) Type") || dataMap.containsKey("Network (1) standard")) {
			flag1 = true;
		}
		if (dataMap.containsKey("网络(2)网络制式") || dataMap.containsKey("Network(2) Type") || dataMap.containsKey("Network (1) standard")) {
			flag2 = true;
		}
		while (iter.hasNext()) {
			String key = iter.next() + "";
			if (key == null || key.isEmpty()) {
				continue;
			}

			if ("平均速率(Kbps)".equals(key) || "平均速率(Mbps)".equals(key) || "平均速率（Mbps）".equals(key) || "average speed (mbps)".equals(key.toLowerCase())
					|| "average speed (kbps)".equals(key.toLowerCase()) || "avg speed(kbps)".equals(key.toLowerCase()) || "avg speed(mbps)".equals(key.toLowerCase())) {
			} else if ("logversion".equals(key)) {
				logversion = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("logversion", logversion);
			} else if ("信号强度".equals(key) || "Signal Strength".equals(key)) {
				signal_strength = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
			} else if ("终端类型".equals(key) || "终端型号".equals(key) || "device type".equals(key.toLowerCase()) || "terminal model".equals(key.toLowerCase()) || "terminal type".equals(key.toLowerCase())
					|| "terminal mode".equals(key.toLowerCase()) || "device model".equals(key.toLowerCase()) || "terminal models".equals(key.toLowerCase())) {
				model = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("model", model);
			} else if ("是否成功".equals(key) || "success or not".equals(key.toLowerCase())) {
				success_failure = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("success_failure", success_failure);
			} else if ("接续时长(ms)".equals(key) || "Continuing Time(ms)".equals(key) || "接通时长(ms)".equals(key) || "Connecting time(ms)".equals(key) || "接通时长(s)".equals(key)
					|| "Connected time(s)".equals(key)) {
				setup_delay = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("setup_delay", setup_delay);
			} else if ("手机号码".equals(key) || "Phone Number".equals(key)) {
				target_number = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("target_number", target_number);
			} else if ("网络".equals(key) || "Network".equals(key)) {
				network = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("network", network);
			} else if ("fallbackToCs(ms)".equals(key) || "FallBack To CS(s)".equals(key)) {
				hand_over_situation = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("hand_over_situation", hand_over_situation);
				fallbacktocs = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
			} else if ("backToLte(ms)".equals(key) || "Back To Lte(ms)".equals(key)) {
				backtolte = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
			} else if ("是否录音".equals(key) || "Call Recording".equals(key)) {
				recording = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("recording", recording);
			} else if ("LAC".equals(key)) {
				String lacStr = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				if (lacStr.indexOf("=") != -1) {
					lac = lacStr.substring(lacStr.indexOf("=") + 1, lacStr.length());
				} else {
					lac = lacStr;
				}
			} else if ("CID".equals(key)) {
				String cidStr = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				if (cidStr.indexOf("=") != -1) {
					cid = cidStr.substring(cidStr.indexOf("=") + 1, cidStr.length());
				} else {
					cid = cidStr;
				}
			} else if ("LAC/CID".equals(key)) {
				String lacCid = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				if (lacCid.indexOf("/") != -1 || lacCid.indexOf("N/A") != -1) {
					String[] cellInfo = lacCid.split("/");
					lac = cellInfo[0];
					cid = cellInfo[1];
				}
			} else if ("TAC".equals(key)) {
				String tacStr = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				if (tacStr.indexOf("=") != -1) {
					tac = tacStr.substring(tacStr.indexOf("=") + 1, tacStr.length() - 1);
				} else {
					tac = tacStr;
				}
			} else if ("PCI".equals(key)) {
				String pciStr = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				if (pciStr.indexOf("=") != -1) {
					pci = pciStr.substring(pciStr.indexOf("=") + 1, pciStr.length() - 1);
				} else {
					pci = pciStr;
				}
			} else if ("TAC/PCI".equals(key)) {
				String tacPci = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				if (tacPci.equals("N/A/N/A")) {
					tac = "N/A";
					pci = "N/A";
				} else {
					if (tacPci.indexOf("/") != -1 || tacPci.indexOf("N/A") != -1) {
						String[] cellInfo = tacPci.split("/");
						tac = cellInfo[0];
						pci = cellInfo[1];
					}
				}
			} else if ("网络类型".equals(key) || "网络制式".equals(key) || "network type".equals(key.toLowerCase())) {
				net_type = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
			} else if ("SINR".equals(key)) {
				sinr = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
			} else if ("网络(1)网络制式".equals(key) || "network(1) type".equals(key.toLowerCase()) || "network (1) standard".equals(key.toLowerCase())) {
				net_type1 = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
			} else if ("网络(1)信号强度".equals(key) || "network(1) signal strength".equals(key.toLowerCase())) {
				signal_strength1 = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
			} else if ("网络(1)小区信息".equals(key) || "network(1) cell".equals(key.toLowerCase()) || "network(1)".equals(key.toLowerCase())) {
				cell1 = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
			} else if ("网络(1)SINR".equals(key) || "network(1) sinr".equals(key.toLowerCase())) {
				sinr1 = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
			} else if ("网络(2)网络制式".equals(key) || "network(2) type".equals(key.toLowerCase()) || "network (2) standard".equals(key.toLowerCase())) {
				net_type2 = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
			} else if ("网络(2)信号强度".equals(key) || "network(2) signal strength".equals(key.toLowerCase())) {
				signal_strength2 = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
			} else if ("网络(2)小区信息".equals(key) || "network(2) cell".equals(key.toLowerCase()) || "network(2)".equals(key.toLowerCase())) {
				cell2 = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
			} else if ("网络(2)SINR".equals(key) || "network(2) sinr".equals(key.toLowerCase())) {
				sinr2 = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
			} else if ("wifi signal strength".equals(key.trim().toLowerCase())) {
				wifi_ss = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
			} else if ("用户ID".equals(key)) {
				consumerid = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("consumerid", consumerid);
			} else if ("业务ID".equals(key)) {
				businessid = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("businessid", businessid);
			} else if ("MAC1".equals(key)) {
				mac = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("mac", mac);
			}
		}
		if (file_path.contains("05001.C")) {
			map.put("csfb", 1);
			map.put("backtolte", backtolte);
			map.put("fallbacktocs", fallbacktocs);
		} else {
			map.put("csfb", 0);
		}
		String lac1 = "";
		String cid1 = "";
		if (cell1.indexOf("/") != -1 || cell1.indexOf("N/A") != -1) {
			String[] cellInfo = cell1.split("/");
			if (cellInfo[0] != null && !cellInfo[0].equals("")) {
				lac1 = cellInfo[0];
			}
			if (tac != null && !tac.equals("")) {
				lac1 = tac;
			}
			if (cellInfo[1] != null && !cellInfo[1].equals("")) {
				cid1 = cellInfo[1];
			}
			if (pci != null && !pci.equals("")) {
				cid1 = pci;
			}
			if (tac != null && !tac.equals("") && cellInfo[0] != null && !cellInfo[0].equals("")) {
				lac1 = cellInfo[0] + "|" + tac;
			}
			if (pci != null && !pci.equals("") && cellInfo[1] != null && !cellInfo[1].equals("")) {
				cid1 = cellInfo[1] + "|" + pci;
			}
		}
		String lac2 = "";
		String cid2 = "";
		if (cell2.indexOf("/") != -1 || cell2.indexOf("N/A") != -1) {
			String[] cellInfo = cell2.split("/");
			if (cellInfo[0] != null && !cellInfo[0].equals("")) {
				lac2 = cellInfo[0];
			}
			if (tac != null && !tac.equals("")) {
				lac2 = tac;
			}
			if (cellInfo[1] != null && !cellInfo[1].equals("")) {
				cid2 = cellInfo[1];
			}
			if (pci != null && !pci.equals("")) {
				cid2 = pci;
			}
			if (tac != null && !tac.equals("") && cellInfo[0] != null && !cellInfo[0].equals("")) {
				lac2 = cellInfo[0] + "|" + tac;
			}
			if (pci != null && !pci.equals("") && cellInfo[1] != null && !cellInfo[1].equals("")) {
				cid2 = cellInfo[1] + "|" + pci;
			}
		}
		// 网络(1)信息存在时 当前数据返回的是网络(1)的信息
		if (flag1 && net_type1.equals(net_type)) {
			sinr = sinr1;
			net_type = net_type1;
			signal_strength = signal_strength1;
			lac_tac = lac1;
			cid_pci = cid1;
		}
		// 网络(2)信息存在时 当前数据返回的是网络(2)的信息
		if (flag2 && net_type2.equals(net_type)) {
			sinr = sinr2;
			net_type = net_type2;
			signal_strength = signal_strength2;
			lac_tac = lac2;
			cid_pci = cid2;
		}
		if (!(flag1 && net_type.equals(net_type1)) && !(flag2 && net_type.equals(net_type2))) {
			if (tac != null && !tac.equals("")) {
				lac_tac = tac;
			}
			if (pci != null && !pci.equals("")) {
				cid_pci = pci;
			}
			if (tac != null && !tac.equals("") && lac != null && !lac.equals("")) {
				lac_tac = lac + "|" + tac;
			}
			if (pci != null && !pci.equals("") && cid != null && !cid.equals("")) {
				cid_pci = cid + "|" + pci;
			}
		}
		map.put("lac_tac", lac_tac);
		map.put("cid_pci", cid_pci);
		map.put("sinr", sinr);
		if (net_type.equals("WIFI")) {
			map.put("signal_strength", wifi_ss);
		} else {
			map.put("signal_strength", signal_strength);
		}
		map.put("net_type", net_type);

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
		if (model.toLowerCase().contains("iphone")) {
			map.put("android_ios", "ios");
		} else {
			map.put("android_ios", "android");
		}
		sql = appendSql(map, numType, file_index, testtime);
		return sql;
	}

	/**
	 * 插入ResideTest数据
	 * 
	 * @param dataMap
	 * @param numType
	 * @param org
	 * @param testtime
	 * @return
	 * @return StringBuffer
	 */
	public StringBuffer insertResideTest(Map dataMap, String numType, String org, String testtime) {
		List nameList = new ArrayList();
		List valueList = new ArrayList();
		Map map = new HashMap();
		Set keySet = dataMap.keySet();
		Iterator iter = keySet.iterator();
		String file_path = (String) (dataMap.get("filePath") == null ? "" : dataMap.get("filePath"));
		String file_index = (String) dataMap.get("file_index");
		map.put("file_index", file_index);
		map.put("file_path", file_path.replace("\\", "|").replace("//", "|"));

		String longitude = "";
		String latitude = "";
		String province = "";// 测试省
		String district = "";// 测试district
		String city = "";// 测试city
		String model = "";// 终端类型
		String logversion = "";// 软件版本

		String duration = "";
		String max_latency = "";
		String min_latency = "";
		String avg_latency = "";

		String location = "";// 测试地点
		String gpsStr = "";
		String net_type = "";// 网络类型
		String signal_strength = "";// 信号强度
		String sinr = "";// SINR
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
		String wifi_ss = "";// WIFI SIGNAL STRENGTH
		String consumerid = "";// 用户id
		String businessid = "";// 业务id
		String mac = "";// MAC地址
		boolean flag1 = false;
		boolean flag2 = false;

		String spaceGpsStr = "";// 测试地点
		// 获取GPS位置信息----start
		if (dataMap.containsKey("GPS信息")) {
			gpsStr = (String) (dataMap.get("GPS信息") == null ? "" : dataMap.get("GPS信息"));
		} else if (dataMap.containsKey("GPS位置信息")) {
			gpsStr = (String) (dataMap.get("GPS位置信息") == null ? "" : dataMap.get("GPS位置信息"));
		} else if (dataMap.containsKey("GPS位置")) {
			gpsStr = (String) (dataMap.get("GPS位置") == null ? "" : dataMap.get("GPS位置"));
		} else if (dataMap.containsKey("测试位置")) {
			gpsStr = (String) (dataMap.get("测试位置") == null ? "" : dataMap.get("测试位置"));
		} else if (dataMap.containsKey("测试GPS位置")) {
			gpsStr = (String) (dataMap.get("测试GPS位置") == null ? "" : dataMap.get("测试GPS位置"));
		} else {
			gpsStr = "";
		}
		map.put("gps", gpsStr);
		if (gpsStr.contains(" ")) {
			String[] gps = transGpsPoint(gpsStr);
			if (gps != null && gps[0] != null && gps[1] != null) {
				longitude = gps[0];
				latitude = gps[1];
			}
		}
		map.put("longitude", longitude);
		map.put("latitude", latitude);
		// 获取GPS位置信息----end
		if (dataMap.containsKey("网络(1)网络制式") || dataMap.containsKey("Network(1) Type") || dataMap.containsKey("Network (1) standard")) {
			flag1 = true;
		}
		if (dataMap.containsKey("网络(2)网络制式") || dataMap.containsKey("Network(2) Type") || dataMap.containsKey("Network (1) standard")) {
			flag2 = true;
		}
		while (iter.hasNext()) {
			String key = iter.next() + "";
			if (key == null || key.isEmpty()) {
				continue;
			}

			if ("平均速率(Kbps)".equals(key) || "平均速率(Mbps)".equals(key) || "平均速率（Mbps）".equals(key) || "average speed (mbps)".equals(key.toLowerCase())
					|| "average speed (kbps)".equals(key.toLowerCase()) || "avg speed(kbps)".equals(key.toLowerCase()) || "avg speed(mbps)".equals(key.toLowerCase())) {
			} else if ("logversion".equals(key)) {
				logversion = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("logversion", logversion);
			} else if ("信号强度".equals(key) || "Signal Strength".equals(key)) {
				signal_strength = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
			} else if ("终端类型".equals(key) || "终端型号".equals(key) || "device type".equals(key.toLowerCase()) || "terminal model".equals(key.toLowerCase()) || "terminal type".equals(key.toLowerCase())
					|| "terminal mode".equals(key.toLowerCase()) || "device model".equals(key.toLowerCase()) || "terminal models".equals(key.toLowerCase())) {
				model = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("model", model);
			} else if ("平均时长(ms)".equals(key) || "平均时延".equals(key) || "平均时延(ms)".equals(key) || "Avg Latency(ms)".equals(key) || "Avg Time(ms)".equals(key) || "Latency".equals(key)
					|| "Delay".equals(key) || "Delay(ms)".equals(key) || "Handover Latency".equals(key)) {
				avg_latency = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("avg_latency", avg_latency);
			} else if ("LAC".equals(key)) {
				String lacStr = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				if (lacStr.indexOf("=") != -1) {
					lac = lacStr.substring(lacStr.indexOf("=") + 1, lacStr.length());
				} else {
					lac = lacStr;
				}
			} else if ("CID".equals(key)) {
				String cidStr = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				if (cidStr.indexOf("=") != -1) {
					cid = cidStr.substring(cidStr.indexOf("=") + 1, cidStr.length());
				} else {
					cid = cidStr;
				}
			} else if ("LAC/CID".equals(key)) {
				String lacCid = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				if (lacCid.indexOf("/") != -1 || lacCid.indexOf("N/A") != -1) {
					String[] cellInfo = lacCid.split("/");
					lac = cellInfo[0];
					cid = cellInfo[1];
				}
			} else if ("TAC".equals(key)) {
				String tacStr = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				if (tacStr.indexOf("=") != -1) {
					tac = tacStr.substring(tacStr.indexOf("=") + 1, tacStr.length() - 1);
				} else {
					tac = tacStr;
				}
			} else if ("PCI".equals(key)) {
				String pciStr = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				if (pciStr.indexOf("=") != -1) {
					pci = pciStr.substring(pciStr.indexOf("=") + 1, pciStr.length() - 1);
				} else {
					pci = pciStr;
				}
			} else if ("TAC/PCI".equals(key)) {
				String tacPci = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				if (tacPci.equals("N/A/N/A")) {
					tac = "N/A";
					pci = "N/A";
				} else {
					if (tacPci.indexOf("/") != -1 || tacPci.indexOf("N/A") != -1) {
						String[] cellInfo = tacPci.split("/");
						tac = cellInfo[0];
						pci = cellInfo[1];
					}
				}
			} else if ("网络类型".equals(key) || "网络制式".equals(key) || "network type".equals(key.toLowerCase())) {
				net_type = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
			} else if ("SINR".equals(key)) {
				sinr = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
			} else if ("网络(1)网络制式".equals(key) || "network(1) type".equals(key.toLowerCase()) || "network (1) standard".equals(key.toLowerCase())) {
				net_type1 = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
			} else if ("网络(1)信号强度".equals(key) || "network(1) signal strength".equals(key.toLowerCase())) {
				signal_strength1 = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
			} else if ("网络(1)小区信息".equals(key) || "network(1) cell".equals(key.toLowerCase()) || "network(1)".equals(key.toLowerCase())) {
				cell1 = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
			} else if ("网络(1)SINR".equals(key) || "network(1) sinr".equals(key.toLowerCase())) {
				sinr1 = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
			} else if ("网络(2)网络制式".equals(key) || "network(2) type".equals(key.toLowerCase()) || "network (2) standard".equals(key.toLowerCase())) {
				net_type2 = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
			} else if ("网络(2)信号强度".equals(key) || "network(2) signal strength".equals(key.toLowerCase())) {
				signal_strength2 = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
			} else if ("网络(2)小区信息".equals(key) || "network(2) cell".equals(key.toLowerCase()) || "network(2)".equals(key.toLowerCase())) {
				cell2 = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
			} else if ("网络(2)SINR".equals(key) || "network(2) sinr".equals(key.toLowerCase())) {
				sinr2 = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
			} else if ("wifi signal strength".equals(key.trim().toLowerCase())) {
				wifi_ss = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
			} else if ("用户ID".equals(key)) {
				consumerid = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("consumerid", consumerid);
			} else if ("业务ID".equals(key)) {
				businessid = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("businessid", businessid);
			} else if ("MAC1".equals(key)) {
				mac = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("mac", mac);
			}
		}
		String lac1 = "";
		String cid1 = "";
		if (cell1.indexOf("/") != -1 || cell1.indexOf("N/A") != -1) {
			String[] cellInfo = cell1.split("/");
			if (cellInfo[0] != null && !cellInfo[0].equals("")) {
				lac1 = cellInfo[0];
			}
			if (tac != null && !tac.equals("")) {
				lac1 = tac;
			}
			if (cellInfo[1] != null && !cellInfo[1].equals("")) {
				cid1 = cellInfo[1];
			}
			if (pci != null && !pci.equals("")) {
				cid1 = pci;
			}
			if (tac != null && !tac.equals("") && cellInfo[0] != null && !cellInfo[0].equals("")) {
				lac1 = cellInfo[0] + "|" + tac;
			}
			if (pci != null && !pci.equals("") && cellInfo[1] != null && !cellInfo[1].equals("")) {
				cid1 = cellInfo[1] + "|" + pci;
			}
		}
		String lac2 = "";
		String cid2 = "";
		if (cell2.indexOf("/") != -1 || cell2.indexOf("N/A") != -1) {
			String[] cellInfo = cell2.split("/");
			if (cellInfo[0] != null && !cellInfo[0].equals("")) {
				lac2 = cellInfo[0];
			}
			if (tac != null && !tac.equals("")) {
				lac2 = tac;
			}
			if (cellInfo[1] != null && !cellInfo[1].equals("")) {
				cid2 = cellInfo[1];
			}
			if (pci != null && !pci.equals("")) {
				cid2 = pci;
			}
			if (tac != null && !tac.equals("") && cellInfo[0] != null && !cellInfo[0].equals("")) {
				lac2 = cellInfo[0] + "|" + tac;
			}
			if (pci != null && !pci.equals("") && cellInfo[1] != null && !cellInfo[1].equals("")) {
				cid2 = cellInfo[1] + "|" + pci;
			}
		}
		// 网络(1)信息存在时 当前数据返回的是网络(1)的信息
		if (flag1 && net_type1.equals(net_type)) {
			sinr = sinr1;
			net_type = net_type1;
			signal_strength = signal_strength1;
			lac_tac = lac1;
			cid_pci = cid1;
		}
		// 网络(2)信息存在时 当前数据返回的是网络(2)的信息
		if (flag2 && net_type2.equals(net_type)) {
			sinr = sinr2;
			net_type = net_type2;
			signal_strength = signal_strength2;
			lac_tac = lac2;
			cid_pci = cid2;
		}
		if (!(flag1 && net_type.equals(net_type1)) && !(flag2 && net_type.equals(net_type2))) {
			if (tac != null && !tac.equals("")) {
				lac_tac = tac;
			}
			if (pci != null && !pci.equals("")) {
				cid_pci = pci;
			}
			if (tac != null && !tac.equals("") && lac != null && !lac.equals("")) {
				lac_tac = lac + "|" + tac;
			}
			if (pci != null && !pci.equals("") && cid != null && !cid.equals("")) {
				cid_pci = cid + "|" + pci;
			}
		}
		map.put("lac_tac", lac_tac);
		map.put("cid_pci", cid_pci);
		map.put("sinr", sinr);
		if (net_type.equals("WIFI")) {
			map.put("signal_strength", wifi_ss);
		} else {
			map.put("signal_strength", signal_strength);
		}
		map.put("net_type", net_type);

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
		if (model.toLowerCase().contains("iphone")) {
			map.put("android_ios", "ios");
		} else {
			map.put("android_ios", "android");
		}
		sql = appendSql(map, numType, file_index, testtime);
		return sql;
	}

	/**
	 * 插入SMS数据
	 * 
	 * @param dataMap
	 * @param numType
	 * @param org
	 * @param testtime
	 * @return
	 * @return StringBuffer
	 */
	public StringBuffer insertSMS(Map dataMap, String numType, String org, String testtime) {
		List nameList = new ArrayList();
		List valueList = new ArrayList();
		Map map = new HashMap();
		Set keySet = dataMap.keySet();
		Iterator iter = keySet.iterator();
		String file_path = (String) (dataMap.get("filePath") == null ? "" : dataMap.get("filePath"));
		String file_index = (String) dataMap.get("file_index");
		map.put("file_index", file_index);
		map.put("file_path", file_path.replace("\\", "|").replace("//", "|"));

		String longitude = "";
		String latitude = "";
		String province = "";// 测试省
		String district = "";// 测试district
		String city = "";// 测试city
		String model = "";// 终端类型
		String logversion = "";// 软件版本

		String target_number = "";// 目标号码
		String sending_delay = "";// 发送时延
		String success_times = "";// 成功次数
		String test_times = "";// 测试次数
		String success_rate = "";// 成功率
		String success_failure = "";

		String location = "";// 测试地点
		String gpsStr = "";
		String net_type = "";// 网络类型
		String signal_strength = "";// 信号强度
		String sinr = "";// SINR
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
		String wifi_ss = "";// WIFI SIGNAL STRENGTH

		String consumerid = "";// 用户id
		String businessid = "";// 业务id
		String mac = "";// MAC地址

		boolean flag1 = false;
		boolean flag2 = false;

		String spaceGpsStr = "";// 测试地点
		// 获取GPS位置信息----start
		if (dataMap.containsKey("GPS信息")) {
			gpsStr = (String) (dataMap.get("GPS信息") == null ? "" : dataMap.get("GPS信息"));
		} else if (dataMap.containsKey("GPS位置信息")) {
			gpsStr = (String) (dataMap.get("GPS位置信息") == null ? "" : dataMap.get("GPS位置信息"));
		} else if (dataMap.containsKey("GPS位置")) {
			gpsStr = (String) (dataMap.get("GPS位置") == null ? "" : dataMap.get("GPS位置"));
		} else if (dataMap.containsKey("测试位置")) {
			gpsStr = (String) (dataMap.get("测试位置") == null ? "" : dataMap.get("测试位置"));
		} else if (dataMap.containsKey("测试GPS位置")) {
			gpsStr = (String) (dataMap.get("测试GPS位置") == null ? "" : dataMap.get("测试GPS位置"));
		} else {
			gpsStr = "";
		}
		map.put("gps", gpsStr);
		if (gpsStr.contains(" ")) {
			String[] gps = transGpsPoint(gpsStr);
			if (gps != null && gps[0] != null && gps[1] != null) {
				longitude = gps[0];
				latitude = gps[1];
			}
		}
		map.put("longitude", longitude);
		map.put("latitude", latitude);

		// 获取GPS位置信息----end
		if (dataMap.containsKey("网络(1)网络制式") || dataMap.containsKey("Network(1) Type") || dataMap.containsKey("Network (1) standard")) {
			flag1 = true;
		}
		if (dataMap.containsKey("网络(2)网络制式") || dataMap.containsKey("Network(2) Type") || dataMap.containsKey("Network (1) standard")) {
			flag2 = true;
		}
		while (iter.hasNext()) {
			String key = iter.next() + "";
			if (key == null || key.isEmpty()) {
				continue;
			}
			if ("平均速率(Kbps)".equals(key) || "平均速率(Mbps)".equals(key) || "平均速率（Mbps）".equals(key) || "average speed (mbps)".equals(key.toLowerCase())
					|| "average speed (kbps)".equals(key.toLowerCase()) || "avg speed(kbps)".equals(key.toLowerCase()) || "avg speed(mbps)".equals(key.toLowerCase())) {
			} else if ("logversion".equals(key.toLowerCase())) {
				logversion = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("logversion", logversion);
			} else if ("信号强度".equals(key) || "signal strength".equals(key.toLowerCase())) {
				signal_strength = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
			} else if ("终端类型".equals(key) || "终端型号".equals(key) || "device type".equals(key.toLowerCase()) || "terminal model".equals(key.toLowerCase()) || "terminal type".equals(key.toLowerCase())
					|| "terminal mode".equals(key.toLowerCase()) || "device model".equals(key.toLowerCase()) || "terminal models".equals(key.toLowerCase())) {
				model = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("model", model);
			} else if ("时延(ms)".equals(key) || "delay(ms)".equals(key.toLowerCase())) {
				sending_delay = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("sending_delay", sending_delay);
			} else if ("手机号码".equals(key) || "phone number".equals(key.toLowerCase())) {
				target_number = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("target_number", target_number);
			} else if ("测试次数".equals(key) || "test times".equals(key.trim().toLowerCase()) || "send sms counts".equals(key.toLowerCase())) {
				test_times = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("test_times", test_times);
			} else if ("成功率".equals(key) || "success rate".equals(key.toLowerCase())) {
				success_rate = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("success_rate", success_rate);
			} else if ("成功次数".equals(key) || "succ counts".equals(key.toLowerCase())) {
				success_times = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("success_times", success_times);
			} else if ("是否成功".equals(key) || "success or not".equals(key.toLowerCase())) {
				success_failure = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("success_failure", success_failure);
			} else if ("LAC".equals(key)) {
				String lacStr = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				if (lacStr.indexOf("=") != -1) {
					lac = lacStr.substring(lacStr.indexOf("=") + 1, lacStr.length());
				} else {
					lac = lacStr;
				}
			} else if ("CID".equals(key)) {
				String cidStr = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				if (cidStr.indexOf("=") != -1) {
					cid = cidStr.substring(cidStr.indexOf("=") + 1, cidStr.length());
				} else {
					cid = cidStr;
				}
			} else if ("LAC/CID".equals(key)) {
				String lacCid = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				if (lacCid.indexOf("/") != -1 || lacCid.indexOf("N/A") != -1) {
					String[] cellInfo = lacCid.split("/");
					lac = cellInfo[0];
					cid = cellInfo[1];
				}
			} else if ("TAC".equals(key)) {
				String tacStr = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				if (tacStr.indexOf("=") != -1) {
					tac = tacStr.substring(tacStr.indexOf("=") + 1, tacStr.length() - 1);
				} else {
					tac = tacStr;
				}
			} else if ("PCI".equals(key)) {
				String pciStr = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				if (pciStr.indexOf("=") != -1) {
					pci = pciStr.substring(pciStr.indexOf("=") + 1, pciStr.length() - 1);
				} else {
					pci = pciStr;
				}
			} else if ("TAC/PCI".equals(key)) {
				String tacPci = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				if (tacPci.equals("N/A/N/A")) {
					tac = "N/A";
					pci = "N/A";
				} else {
					if (tacPci.indexOf("/") != -1 || tacPci.indexOf("N/A") != -1) {
						String[] cellInfo = tacPci.split("/");
						tac = cellInfo[0];
						pci = cellInfo[1];
					}
				}
			} else if ("网络类型".equals(key) || "网络制式".equals(key) || "network type".equals(key.toLowerCase())) {
				net_type = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
			} else if ("SINR".equals(key)) {
				sinr = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
			} else if ("网络(1)网络制式".equals(key) || "network(1) type".equals(key.toLowerCase()) || "network (1) standard".equals(key.toLowerCase())) {
				net_type1 = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
			} else if ("网络(1)信号强度".equals(key) || "network(1) signal strength".equals(key.toLowerCase()) || "network (1) signal strength".equals(key.toLowerCase())) {
				signal_strength1 = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
			} else if ("网络(1)小区信息".equals(key) || "network(1) cell".equals(key.toLowerCase()) || "network(1)".equals(key.toLowerCase()) || "network (1) cell".equals(key.toLowerCase())) {
				cell1 = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
			} else if ("网络(1)SINR".equals(key) || "network(1) sinr".equals(key.toLowerCase()) || "network (1) sinr".equals(key.toLowerCase())) {
				sinr1 = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
			} else if ("网络(2)网络制式".equals(key) || "network(2) type".equals(key.toLowerCase()) || "network (2) standard".equals(key.toLowerCase())) {
				net_type2 = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
			} else if ("网络(2)信号强度".equals(key) || "network(2) signal strength".equals(key.toLowerCase()) || "network (2) signal strength".equals(key.toLowerCase())) {
				signal_strength2 = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
			} else if ("网络(2)小区信息".equals(key) || "network(2) cell".equals(key.toLowerCase()) || "network(2)".equals(key.toLowerCase()) || "network(2) cell".equals(key.toLowerCase())) {
				cell2 = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
			} else if ("网络(2)SINR".equals(key) || "network(2) sinr".equals(key.toLowerCase()) || "network (2) sinr".equals(key.toLowerCase())) {
				sinr2 = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
			} else if ("wifi signal strength".equals(key.trim().toLowerCase())) {
				wifi_ss = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
			} else if ("用户ID".equals(key)) {
				consumerid = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("consumerid", consumerid);
			} else if ("业务ID".equals(key)) {
				businessid = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("businessid", businessid);
			} else if ("MAC1".equals(key)) {
				mac = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("mac", mac);
			}
		}
		String lac1 = "";
		String cid1 = "";
		if (cell1.indexOf("/") != -1 || cell1.indexOf("N/A") != -1) {
			String[] cellInfo = cell1.split("/");
			if (cellInfo[0] != null && !cellInfo[0].equals("")) {
				lac1 = cellInfo[0];
			}
			if (tac != null && !tac.equals("")) {
				lac1 = tac;
			}
			if (cellInfo[1] != null && !cellInfo[1].equals("")) {
				cid1 = cellInfo[1];
			}
			if (pci != null && !pci.equals("")) {
				cid1 = pci;
			}
			if (tac != null && !tac.equals("") && cellInfo[0] != null && !cellInfo[0].equals("")) {
				lac1 = cellInfo[0] + "|" + tac;
			}
			if (pci != null && !pci.equals("") && cellInfo[1] != null && !cellInfo[1].equals("")) {
				cid1 = cellInfo[1] + "|" + pci;
			}
		}
		String lac2 = "";
		String cid2 = "";
		if (cell2.indexOf("/") != -1 || cell2.indexOf("N/A") != -1) {
			String[] cellInfo = cell2.split("/");
			if (cellInfo[0] != null && !cellInfo[0].equals("")) {
				lac2 = cellInfo[0];
			}
			if (tac != null && !tac.equals("")) {
				lac2 = tac;
			}
			if (cellInfo[1] != null && !cellInfo[1].equals("")) {
				cid2 = cellInfo[1];
			}
			if (pci != null && !pci.equals("")) {
				cid2 = pci;
			}
			if (tac != null && !tac.equals("") && cellInfo[0] != null && !cellInfo[0].equals("")) {
				lac2 = cellInfo[0] + "|" + tac;
			}
			if (pci != null && !pci.equals("") && cellInfo[1] != null && !cellInfo[1].equals("")) {
				cid2 = cellInfo[1] + "|" + pci;
			}
		}
		// 网络(1)信息存在时 当前数据返回的是网络(1)的信息
		if (flag1 && net_type1.equals(net_type)) {
			sinr = sinr1;
			net_type = net_type1;
			signal_strength = signal_strength1;
			lac_tac = lac1;
			cid_pci = cid1;
		}
		// 网络(2)信息存在时 当前数据返回的是网络(2)的信息
		if (flag2 && net_type2.equals(net_type)) {
			sinr = sinr2;
			net_type = net_type2;
			signal_strength = signal_strength2;
			lac_tac = lac2;
			cid_pci = cid2;
		}
		if (!(flag1 && net_type.equals(net_type1)) && !(flag2 && net_type.equals(net_type2))) {
			if (tac != null && !tac.equals("")) {
				lac_tac = tac;
			}
			if (pci != null && !pci.equals("")) {
				cid_pci = pci;
			}
			if (tac != null && !tac.equals("") && lac != null && !lac.equals("")) {
				lac_tac = lac + "|" + tac;
			}
			if (pci != null && !pci.equals("") && cid != null && !cid.equals("")) {
				cid_pci = cid + "|" + pci;
			}
		}
		map.put("lac_tac", lac_tac);
		map.put("cid_pci", cid_pci);
		map.put("sinr", sinr);
		if (net_type.equals("WIFI")) {
			map.put("signal_strength", wifi_ss);
		} else {
			map.put("signal_strength", signal_strength);
		}
		map.put("net_type", net_type);

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
		if (model.toLowerCase().contains("iphone")) {
			map.put("android_ios", "ios");
		} else {
			map.put("android_ios", "android");
		}
		sql = appendSql(map, numType, file_index, testtime);
		return sql;
	}

	/**
	 * 根据map拼装插入的sql
	 * 
	 * @param map
	 * @param numType
	 * @param fileIndex
	 * @return 拼装完全的sql
	 */
	private StringBuffer appendSql(Map map, String numType, String fileIndex, String testtime) {
		StringBuffer sql = new StringBuffer("");
		// String database = "testdataanalyse";
		StringBuffer columnStr = new StringBuffer("(");
		StringBuffer valueStr = new StringBuffer(" values (");
		StringBuffer updateStr = new StringBuffer("");
		String table = "";
		String datatype = "";
		// 判断要插入数据的表名
		if (numType.equals("01001")) {
			table = "pc_speed_test_" + testtime;
			datatype = "pc_speed_test";
		} else if (numType.equals("02001")) {
			table = "pc_web_browsing_" + testtime;
			datatype = "pc_web_browsing";
		} else if (numType.equals("03001")) {
			table = "pc_http_test_" + testtime;
			datatype = "pc_http_test";
		} else if (numType.equals("04002")) {
			table = "pc_ping_" + testtime;
			datatype = "pc_ping";
		} else if (numType.equals("04003")) {
			table = "pc_dns_" + testtime;
			datatype = "pc_dns";
		} else if (numType.equals("04004")) {
			table = "pc_traceroute_" + testtime;
			datatype = "pc_traceroute";
		} else if (numType.equals("04005")) {
			table = "pc_mtu_" + testtime;
			datatype = "pc_mtu";
		} else if (numType.equals("04006")) {
			table = "hand_over_" + testtime;
			datatype = "hand_over";
		} else if (numType.equals("05001")) {
			table = "call_test_" + testtime;
			datatype = "call_test";
		} else if (numType.equals("05002")) {
			table = "sms_" + testtime;
			datatype = "sms";
		} else {
			return new StringBuffer("");
		}

		/**
		 * 创建新表
		 */
		boolean flage = queryTabExist(table);
		if (!flage) {
			System.out.println(table + "    tablename");
			createTable(table);
			// ping_201507 ping_new_201507
			String newtable = table.replace(testtime, "new_" + testtime);
			System.out.println(testtime);
			/*
			 * boolean fl = queryTabExist(newtable); if(!fl){
			 * createNewTable(newtable); }
			 */
			boolean opFla = queryTabExist("date_tablename");
			if (!opFla) {
				createDateTab("date_tablename");
			}
			boolean queryF = queryDataExist(table, newtable);
			if (!queryF) {
				String tabsql = "insert into date_tablename (confdate,tablename,newtablename,datatype)" + " values('" + testtime + "','" + table + "','" + newtable + "','" + datatype + "')";
				String url = dsturl.substring(0, dsturl.lastIndexOf("/") + 1) + SpecialPcTestData.basename;
				try {
					start(url, dstuser, dstpassword);
					insert(tabsql);
				} catch (Exception e) {
					e.printStackTrace();
				} finally {
					try {
						close();
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		}

		// 根据查询结果判断插入还是更新
		// boolean flag = this.queryExist(database, table, fileIndex);//
		// 有数据则为true
		boolean flag = false;
		// 执行更新
		if (flag) {
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
					updateStr = new StringBuffer(updateStr.toString().substring(0, updateStr.toString().lastIndexOf(",")));
				}
				sql.append(updateStr);
				sql.append("where file_index = '" + fileIndex + "'");
			} catch (Exception e) {
				e.printStackTrace();
			}
		} else {
			sql.append("insert into " + table + " ");
			try {
				// map.put("haschange", "0");
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
					columnStr = new StringBuffer(columnStr.toString().substring(0, columnStr.toString().lastIndexOf(",")));
				}
				while (valueStr.toString().trim().endsWith(",")) {
					valueStr = new StringBuffer(valueStr.toString().substring(0, valueStr.toString().lastIndexOf(",")));
				}
				columnStr.append(" )");
				valueStr.append(" )");
				sql.append(columnStr);
				sql.append(valueStr);
			} catch (Exception e) {
				System.out.println("拼装语句错误：" + sql);
				sql = new StringBuffer("");
				e.printStackTrace();
			} finally {
				try {
					close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		return sql;
	}

	/**
	 * 根据fileIndex 进行数据存在的查询 ，确定是否进行插入还是更新的操作
	 * 
	 * @param database
	 * @param table
	 * @param fileIndex
	 * @return
	 * @return boolean
	 */
	public boolean queryExist(String database, String table, String fileIndex) {

		String sql = "select * from " + database + "." + table + " where file_index = '" + fileIndex + "'";
		// start();
		try {
			ResultSet rs = (ResultSet) statement.executeQuery(sql);
			if (rs.next()) {
				return true;
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
		return false;
	}

	/**
	 * GPS坐标转换
	 * 
	 * @param meta
	 * @return
	 */
	private static String[] transGpsPoint(String meta) {

		String[] result = new String[2];
		String[] info = null;
		if (meta != null && !"".equals(meta)) {
			info = meta.split(" ");
		} else {
			return null;
		}

		String latitude = "";
		String longitude = "";

		String gpsPoint = "";
		try {
			for (int i = 0; i < info.length && i < 2; i++) {
				if (info[i].contains("°")) {
					String degrees = info[i].substring(0, info[i].lastIndexOf("°"));
					String minutes = info[i].substring(info[i].lastIndexOf("°") + 1, info[i].lastIndexOf("′"));
					String seconds = info[i].substring(info[i].lastIndexOf("′") + 1, info[i].lastIndexOf("″"));

					// Long gpsLong =
					// Long.parseLong(degrees)+Long.parseLong(minutes)/60+Long.parseLong(seconds)/3600;
					float gpsLong = Float.parseFloat(degrees) + Float.parseFloat(minutes) / 60 + Float.parseFloat(seconds) / 3600;

					DecimalFormat decimalFormat = new DecimalFormat(".0000000");
					gpsPoint = decimalFormat.format(gpsLong);
					if (info[i].contains("E")) {
						longitude = gpsPoint;
					} else if (info[i].contains("N")) {
						latitude = gpsPoint;
					} else if (gpsLong > 80.0) {
						longitude = gpsPoint;
					} else {
						latitude = gpsPoint;
					}
				} else {
					if (info[i].contains("E")) {
						gpsPoint = info[i].substring(0, info[i].lastIndexOf("E"));
						float gpsLong = Float.valueOf(gpsPoint);
						DecimalFormat decimalFormat = new DecimalFormat(".0000000");
						gpsPoint = decimalFormat.format(gpsLong);
						longitude = gpsPoint;
					} else if (info[i].contains("N")) {
						gpsPoint = info[i].substring(0, info[i].lastIndexOf("N"));
						float gpsLong = Float.valueOf(gpsPoint);
						DecimalFormat decimalFormat = new DecimalFormat(".0000000");
						gpsPoint = decimalFormat.format(gpsLong);
						latitude = gpsPoint;
					} else {
						gpsPoint = info[i];
						float gpsLong = Float.valueOf(gpsPoint);
						DecimalFormat decimalFormat = new DecimalFormat(".0000000");
						gpsPoint = decimalFormat.format(gpsLong);
						if (gpsLong > 80.0) {
							longitude = gpsPoint;
						} else {
							latitude = gpsPoint;
						}
					}
				}
			}
			result[0] = longitude;
			result[1] = latitude;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}

	/**
	 * 根据code查询数据库名
	 * 
	 * @param code
	 * @return
	 * @return String
	 */
	public String getBaseName(String code) {

		String sql = "select basename from code_basename where confcode='" + code + "'";
		System.out.println(sql);
		ResultSet rs = null;
		String basename = "";
		try {
			start(dsturl, dstuser, dstpassword);

			rs = statement.executeQuery(sql);
			while (rs.next()) {
				basename = rs.getString("basename");
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
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
	 * 连接数据库，若不存在则创建该数据库
	 * 
	 * @param code
	 * @param srcBaseName
	 * @return void
	 */
	public void startOrCreateDB(String baseName) {

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
			} finally {
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
		String pro1sql  = "CREATE  PROCEDURE `pro_get_scandata_bytablename`(IN `param1` varchar(128),IN `param2` varchar(128),OUT `result` varchar(1024))BEGIN DECLARE i int DEFAULT 0; DECLARE num int; DECLARE temp_count int DEFAULT 0; DECLARE temp_typeno VARCHAR (20) DEFAULT '';" +
				"DECLARE temp_name VARCHAR(100); DECLARE strsSuffix CHAR(1) DEFAULT ','; DECLARE strs VARCHAR(2048) DEFAULT ''; DECLARE _Table_Cur CURSOR FOR SELECT tablename,confdate from date_tablename WHERE tablename  LIKE 'pc_%' AND  confdate=param2 ;" +
				"SELECT count(*) INTO num from date_tablename WHERE tablename  LIKE 'pc_%' AND  confdate=param2  ; OPEN _Table_Cur;while i < num DO FETCH _Table_Cur into temp_name,temp_typeno; SET @temp_count_insql = 0; IF temp_typeno = '05001' THEN " +
				"SET @SQLSTR = CONCAT(\"SELECT id into @temp_count_insql from \",temp_name,\" WHERE device_org = ? AND csfb = 0 LIMIT 1\"); ELSEIF temp_typeno = '05001.C' THEN SET @SQLSTR = CONCAT(\"SELECT id into @temp_count_insql from \",temp_name,\" WHERE device_org = ? AND csfb = 1 LIMIT 1\");" +
				"ELSE  SET @SQLSTR = CONCAT(\"SELECT id into @temp_count_insql from \",temp_name,\" WHERE device_org = ? LIMIT 1\");END if; IF temp_typeno = '05001.C' THEN set temp_name = \"call_test_new.C\"; END if; SET @p = param1; PREPARE tempSTMT FROM @SQLSTR;" +
				"EXECUTE tempSTMT USING @p; set temp_count = @temp_count_insql; IF temp_count = '' THEN set temp_count = 0; END if; DEALLOCATE PREPARE tempSTMT; IF i = (num - 1) THEN SET strsSuffix = ''; END IF;" +
				"SET strs = CONCAT(strs,CONCAT('\"',SUBSTRING_INDEX(temp_name,CONCAT('_',param2),1),'\":',temp_count),strsSuffix); SET strs = CONCAT(strs,CONCAT('\"',temp_name,'\":',temp_count),strsSuffix);SET i = i+1; END WHILE; SET result = strs; CLOSE _Table_Cur; END;" ;
		
		String pro2sql = "CREATE  PROCEDURE `pro_get_scandata_orgs`(IN `p1` varchar(128),IN `p2` varchar(128),IN `p3` varchar(128),OUT `result` varchar(4096))" +
				"BEGIN DECLARE str VARCHAR(1024); DECLARE i INT DEFAULT 0; DECLARE strs VARCHAR(4096) DEFAULT '['; DECLARE obj VARCHAR(1024) DEFAULT '';" +
				"DECLARE objSuffix CHAR(1) DEFAULT ','; DECLARE count INT DEFAULT LENGTH(p1) - LENGTH(REPLACE(p1,p2,'')); DECLARE lastChar CHAR(1) DEFAULT SUBSTR(p1 FROM LENGTH(p1) FOR 1);" +
				"IF lastChar = p2 THEN set count = count - 1; END IF; WHILE i <= count DO set @pos = LOCATE(p2,p1); set str = substring_index(p1,p2,1);" +
				"set p1 = SUBSTR(p1 FROM (@pos + 1) FOR (LENGTH(p1) - (@pos -  1))); CALL pro_get_scandata_bytablename(str,p3,@hasdata);" +
				"IF i = count THEN SET objSuffix = ''; END IF; SET obj = CONCAT('{\"',str,'\":{',@hasdata,'}}',objSuffix); SET strs = CONCAT(strs,obj);set i = i + 1; END WHILE; SET result = CONCAT(strs,']'); SELECT result; END;" ;
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
				e.printStackTrace();
			}
		}
		
		
	}


	
	
	/**
	 * 查询表是否存在
	 * 
	 * @param table
	 * @return
	 * @return boolean
	 */
	public boolean queryTabExist(String table) {
		String url = dsturl.substring(0, dsturl.lastIndexOf("/") + 1) + SpecialPcTestData.basename;
		try {
			start(url, dstuser, dstpassword);
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
	 * 静态表的创建（表是code与库名的对应关系表）
	 * 
	 * @param newTableName
	 * @return void
	 */
	public void createDateTab(String newTableName) {

		String sql = "CREATE TABLE " + newTableName + " ( id int(10) NOT NULL AUTO_INCREMENT,"
				+ "`confdate`  varchar(11) not null,`tablename`  varchar(50) not null,`newtablename`  varchar(50) not null,`datatype`  varchar(50) not null,"
				+ " PRIMARY KEY (id) ) CHARSET=utf8 ROW_FORMAT=COMPACT;";

		String url = dsturl.substring(0, dsturl.lastIndexOf("/") + 1) + SpecialPcTestData.basename;
		try {
			start(url, dstuser, dstpassword);
			statement.execute(sql);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * 根据code查询数据库名
	 * 
	 * @param code
	 * @return
	 * @return String
	 */
	public boolean queryDataExist(String table, String newtable) {

		String sql = "select count(*) count from date_tablename where tablename='" + table + "' and newtablename= '" + newtable + "'";
		System.out.println(sql);
		ResultSet rs = null;
		int count = 0;
		boolean fal = false;
		String url = dsturl.substring(0, dsturl.lastIndexOf("/") + 1) + SpecialPcTestData.basename;
		try {
			start(url, dstuser, dstpassword);

			rs = statement.executeQuery(sql);
			while (rs.next()) {
				count = rs.getInt("count");
				if (count > 0) {
					fal = true;
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
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
	 * 创建新表
	 * 
	 * @param newTableName
	 * @return void
	 */
	public void createTable(String newTableName) {

		String sql = "";

		if (newTableName.startsWith("pc_ping")) {
			sql = "CREATE TABLE "
					+ newTableName  
					+ " ( id int(10) NOT NULL AUTO_INCREMENT, "
					+ "`start_time`  varchar(200)  DEFAULT NULL ,`end_time`  varchar(200)  DEFAULT NULL,"
					+ "`device_org`  varchar(200)  DEFAULT NULL ,`times`  varchar(200)  DEFAULT NULL ,`avg_delay`  varchar(20)  DEFAULT NULL ,"
					+ "`file_index`  varchar(500)  DEFAULT NULL ,`file_path`  varchar(500)  DEFAULT NULL ,`test_location`  varchar(200)  DEFAULT NULL ,`operator`  varchar(200)  DEFAULT NULL ,"
					+ "`contracted_bandwidth`  varchar(200)  DEFAULT NULL ,`user_id`  varchar(200)  DEFAULT NULL ,`service_id`  varchar(200)  DEFAULT NULL ,"
					+ "`computer_name`  varchar(200)  DEFAULT NULL ,`os_name`  varchar(200)  DEFAULT NULL ,`cpu`  varchar(200)  DEFAULT NULL ,`system_type`  varchar(200)  DEFAULT NULL ,`further_request`  varchar(200)  DEFAULT NULL ,`echo_request`  varchar(200)  DEFAULT NULL ,`package_size`  varchar(200)  DEFAULT NULL ,`section`  varchar(200)  DEFAULT NULL ,`ttl`  varchar(200)  DEFAULT NULL ,`resolutionhost_name`  varchar(200)  DEFAULT NULL ,`routing_number`  varchar(200)  DEFAULT NULL ,`hops_timestamp`  varchar(200)  DEFAULT NULL ,"
					+ "`overtime`  varchar(200)  DEFAULT NULL ,`ip_type`  varchar(200)  DEFAULT NULL ,`loose_source_route`  varchar(200)  DEFAULT NULL ,`strict_source_route`  varchar(200)  DEFAULT NULL ,`test_reverse_address`  varchar(200)  DEFAULT NULL ,`use_source_address`  varchar(200)  DEFAULT NULL ,`replays`  varchar(200)  DEFAULT NULL ,`type_of_service`  varchar(200)  DEFAULT NULL ,`sleep_time`  varchar(200)  DEFAULT NULL ,`loss_rate`  varchar(200)  DEFAULT NULL ,"
					+ "`mac1`  varchar(100)  DEFAULT NULL ,`mac2`  varchar(100)  DEFAULT NULL ,`mac3`  varchar(100)  DEFAULT NULL ,`ip1`  varchar(100)  DEFAULT NULL ,`ip2`  varchar(100)  DEFAULT NULL ,`ip3`  varchar(100)  DEFAULT NULL ,`app_id`  varchar(100)  DEFAULT NULL ," 
					+ "PRIMARY KEY (id),INDEX `file_index` (`file_index`) USING BTREE ) CHARSET=utf8 ROW_FORMAT=COMPACT;";
		} else if (newTableName.startsWith("pc_web_browsing")) {
			sql = "CREATE TABLE "
					+ newTableName
					+ " ( id int(10) NOT NULL AUTO_INCREMENT, "
					+ "`start_time`  varchar(200)  DEFAULT NULL ,`end_time`  varchar(200)  DEFAULT NULL ,"
					+ "`file_index`  varchar(500)  DEFAULT NULL ,`device_org`  varchar(200)  DEFAULT NULL ,"
					+ "`file_path`  varchar(500)  DEFAULT NULL ,`test_location`  varchar(200)  DEFAULT NULL ,`operator`  varchar(200)  DEFAULT NULL ,`contracted_bandwidth`  varchar(200)  DEFAULT NULL ,`user_id`  varchar(200)  DEFAULT NULL ,`service_id`  varchar(200)  DEFAULT NULL ,"
					+ "`computer_name`  varchar(200)  DEFAULT NULL ,`os_name`  varchar(200)  DEFAULT NULL ,`cpu`  varchar(200)  DEFAULT NULL ,`system_type`  varchar(200)  DEFAULT NULL ,`times`  varchar(200)  DEFAULT NULL ,`avg_delay`  varchar(200)  DEFAULT NULL ,"
					+ "`mac1`  varchar(100)  DEFAULT NULL ,`mac2`  varchar(100)  DEFAULT NULL ,`mac3`  varchar(100)  DEFAULT NULL ,`ip1`  varchar(100)  DEFAULT NULL ,`ip2`  varchar(100)  DEFAULT NULL ,`ip3`  varchar(100)  DEFAULT NULL ,`app_id`  varchar(100)  DEFAULT NULL ,`avg_success_rate`  varchar(100)  DEFAULT NULL ," 
					+ " PRIMARY KEY (id) ) CHARSET=utf8 ROW_FORMAT=COMPACT;";
		}

		String url = dsturl.substring(0, dsturl.lastIndexOf("/") + 1) + SpecialPcTestData.basename;
		System.out.println(sql);
		try {
			start(url, dstuser, dstpassword);
			statement.execute(sql);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * 根据拼装好的sql查询appid
	 * 
	 * @param sql
	 * @return
	 */
	public List getappIds(String sql) {
		List list = new ArrayList();
		Connection conn1 = null;
		Statement statement1 = null;
		try {
			String driver = "com.mysql.jdbc.Driver";
			//String url = dsturl.substring(0, dsturl.lastIndexOf("/") + 1) + "static_param";
			String url = dsturl;
			System.out.println("******************" + url);
			String user = dstuser;
			// String password = "cmrictpdata";
			String password = dstpassword;
			Class.forName(driver);
			conn1 = DriverManager.getConnection(url, user, password);
			statement1 = conn1.createStatement();
			System.out.println(sql);
			ResultSet rs = statement1.executeQuery(sql);
			while (rs.next()) {
				String appid = rs.getString("app_id");
				if (appid == null || appid.isEmpty()) {

				} else {
					list.add(appid);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (statement1 != null) {
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
}
