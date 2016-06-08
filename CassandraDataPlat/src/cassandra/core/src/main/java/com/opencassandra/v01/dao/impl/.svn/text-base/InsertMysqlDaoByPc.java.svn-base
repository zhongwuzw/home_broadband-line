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

import com.opencassandra.descfile.ConfParser;

public class InsertMysqlDaoByPc {
	Statement statement;
	Connection conn;
	StringBuffer sql = new StringBuffer();
	private static String ak = ConfParser.ak;
	private String[] dateStr = new String[] { "yyyy", "MM", "dd", "HH", "mm", "ss", "SSS" };

	public static void main(String[] args) {
		System.out.println("NULL:" + isNum(""));
	}

	public void start() {
		String driver = "com.mysql.jdbc.Driver";
		String url = ConfParser.url;
		// String url = "jdbc:mysql://218.206.179.109:3306/testdataanalyse";
		String user = ConfParser.user;
		String password = ConfParser.password;
		// String password = "Bi123456";
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
		if (statement != null) {
			statement.close();
		}
		if (conn != null) {
			conn.close();
		}
	}

	/**
	 * 执行sql
	 * 
	 * @param sql
	 * @return
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
	public boolean insertMysqlDB(Map dataMap, String numType, String org, File file) {
		StringBuffer sql = new StringBuffer("");
		boolean flag = false;
		if (!numType.isEmpty()) {
			if (numType.equals("01001")) {
				// 生成要插入数据的sql
				sql = insertSpeedTest(dataMap, numType, org);
			} else if (numType.equals("02001")) {
				sql = insertWebBrowser(dataMap, numType, org);
			} else if (numType.equals("03001")) {
				sql = insertHTTP(dataMap, numType, org);
			} else if (numType.equals("04002")) {
				sql = insertPING(dataMap, numType, org);
			} else if (numType.equals("04003")) {// DNS
				sql = insertDNS(dataMap, numType, org);
			} else if (numType.equals("04004")) {// Traceroute
				sql = insertTraceroute(dataMap, numType, org);
			} else if (numType.equals("04005")) {// MTU
				sql = insertMTU(dataMap, numType, org);
			} else if (numType.equals("04006")) {
				sql = insertResideTest(dataMap, numType, org);
			} else if (numType.equals("05001")) {
				sql = insertCall(dataMap, numType, org);
			} else if (numType.equals("05002")) {
				sql = insertSMS(dataMap, numType, org);
			} else {
				return false;
			}
			if (sql.toString().isEmpty()) {
				flag = false;
			} else {
				start();
				try {
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
	 * 
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
	 */
	public StringBuffer insertSpeedTest(Map dataMap, String numType, String org) {
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
		
		String os = "";//系統名稱
		String toolversion = "";
		

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
			}else if ("系統名称".equals(key)) {
				os = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("os", os);
			}else if ("当前版本".equals(key)) {
				toolversion = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("toolversion", toolversion);
			}
		}
		sql = appendSql(map, numType, file_index);
		return sql;
	}

	/**
	 * 插入WebBrowser数据
	 */
	public StringBuffer insertWebBrowser(Map dataMap, String numType, String org) {
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
		String web_site = "";// 目标地址
		String load_size = "";// 加载总大小
		String object_num = "";// 页面资源数
		String load_success_rate = "";// 加载成功率
		String load_delay = "";// 加载时延
		String load_men_delay = "";// 加载门限时延
		String out_num = "";// 超限制资源数
		String out_Alldelay = "";// 超限制资源总加载时延
		String out_time = "";// 访问超时时间
		String user_agent = "";// USERAgent

		String test_description = "";
		String operator = "";
		String bandwidth = "";
		
		String consumerid = "";// 用户id
		String businessid = "";// 业务id
		String mac = "";// MAC地址
		
		String os = "";//系統名稱
		String toolversion = ""; //当前版本
		String eighty_loading = "";
		
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
				web_site = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("web_site", web_site);
			} else if ("加载总大小(KB)".equals(key)) {
				load_size = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("site_size", load_size);
			} else if ("页面资源总数".equals(key)) {
				object_num = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("total_resource_number", object_num);
			} else if ("请求成功率(%)".equals(key)) {
				load_success_rate = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("success_rate", load_success_rate);
			} else if ("加载时延(ms)".equals(key) || "100%加载时延(ms)".equals(key)) {
				load_delay = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("loading_delay", load_delay);
			} else if ("加载门限时延(ms)".equals(key)) {
				load_men_delay = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("threshold_delay", load_men_delay);
			} else if ("超限值资源数".equals(key)) {
				out_num = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("link_of_overcritical_resource", out_num);
			} else if ("超限值资源总加载时延(ms)".equals(key)) {
				out_Alldelay = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("overcritical_resource_loading_delay", out_Alldelay);
			} else if ("访问超时时间(ms)".equals(key)) {
				out_time = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("timeout", out_time);
			} else if ("UserAgent".equals(key)) {
				user_agent = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("useragent", user_agent);
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
			}else if ("系统名称".equals(key)) {
				os = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("os", os);
			}else if ("当前版本".equals(key)) {
				toolversion = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("toolversion", toolversion);
			}else if ("80%加载时延(ms)".equals(key) || "80%加载时延(ms)".equals(key)) {
				eighty_loading = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("eighty_loading", eighty_loading);
			} 
		}
		sql = appendSql(map, numType, file_index);
		return sql;
	}

	/**
	 * 插入HTTP数据
	 */
	public StringBuffer insertHTTP(Map dataMap, String numType, String org) {
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
		
		String os = "";//系統名稱
		String toolversion = ""; //当前版本
		

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
			}else if ("系统名称".equals(key)) {
				os = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("os", os);
			}else if ("当前版本".equals(key)) {
				toolversion = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("toolversion", toolversion);
			}
		}
		sql = appendSql(map, numType, file_index);
		return sql;
	}

	/**
	 * 插入PING数据
	 */
	public StringBuffer insertPING(Map dataMap, String numType, String org) {
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
		String domain_address = "";// 目标地址
		String ip_address = "";// ip地址
		String location = "";// 归属地
		String times = "";// 测试次数
		String avg_delay = "";// 平均时延
		String max_delay = "";// 最大时延
		String min_delay = "";// 最小时延
		String success_rate = "";// 成功率
		String time_interval = "";// 时间间隔
		String buffer_size = "";// 缓冲区大小

		String test_description = "";
		String operator = "";
		String bandwidth = "";

		String consumerid = "";// 用户id
		String businessid = "";// 业务id
		String mac = "";// MAC地址
		
		String os = "";//系統名稱
		String toolversion = ""; //当前版本
		String loss_rate = "";//丢包率(%)
		

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
				domain_address = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("domain_address", domain_address);
			} else if ("IP地址".equals(key)) {
				ip_address = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("ip_address", ip_address);
			} else if ("归属地".equals(key)) {
				location = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("location", location);
			} else if ("测试次数".equals(key)) {
				times = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("times", times);
			} else if ("平均时延(ms)".equals(key)) {
				avg_delay = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				if (avg_delay.equals("--") || avg_delay.equals("N/A") || avg_delay.isEmpty()) {
					avg_delay = "";
				}
				map.put("avg_delay", avg_delay);
			} else if ("最大时延(ms)".equals(key)) {
				max_delay = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				if (max_delay.equals("--") || max_delay.equals("N/A") || max_delay.isEmpty()) {
					max_delay = "";
				}
				map.put("max_delay", max_delay);
			} else if ("最小时延(ms)".equals(key)) {
				min_delay = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				if (min_delay.equals("--") || min_delay.equals("N/A") || min_delay.isEmpty()) {
					min_delay = "";
				}
				map.put("min_delay", min_delay);
			} else if ("成功率(%)".equals(key)) {
				success_rate = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("success_rate", success_rate);
			} else if ("时间间隔(ms)".equals(key)) {
				time_interval = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("time_interval", time_interval);
			} else if ("缓冲区(byte)".equals(key)) {
				buffer_size = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("buffer_size", buffer_size);
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
			}else if ("系统名称".equals(key)) {
				os = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("os", os);
			}else if ("当前版本".equals(key)) {
				toolversion = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("toolversion", toolversion);
			}else if ("丢包率(%)".equals(key)) {
				loss_rate = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("loss_rate", loss_rate);
			}
		}
		sql = appendSql(map, numType, file_index);
		return sql;
	}

	/**
	 * 插入DNS数据
	 */
	public StringBuffer insertDNS(Map dataMap, String numType, String org) {
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
		
		String os = "";//系統名稱
		String toolversion = ""; //当前版本
		
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
			}else if ("系统名称".equals(key)) {
				os = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("os", os);
			}else if ("当前版本".equals(key)) {
				toolversion = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("toolversion", toolversion);
			}
		}
		sql = appendSql(map, numType, file_index);
		return sql;
	}

	/**
	 * 插入Traceroute数据
	 */
	public StringBuffer insertTraceroute(Map dataMap, String numType, String org) {
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
		
		String os = "";//系統名稱
		String toolversion = ""; //当前版本
		
		

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
			}else if ("系统名称".equals(key)) {
				os = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("os", os);
			}else if ("当前版本".equals(key)) {
				toolversion = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("toolversion", toolversion);
			}
			
		}
		sql = appendSql(map, numType, file_index);
		return sql;
	}

	/**
	 * 插入MTU数据
	 */
	public StringBuffer insertMTU(Map dataMap, String numType, String org) {
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
		
		String os = "";//系統名稱
		String toolversion = ""; //当前版本
		

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
			}else if ("系统名称".equals(key)) {
				os = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("os", os);
			}else if ("当前版本".equals(key)) {
				toolversion = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("toolversion", toolversion);
			}
		}
		sql = appendSql(map, numType, file_index);
		return sql;
	}

	/**
	 * 插入call数据
	 */
	public StringBuffer insertCall(Map dataMap, String numType, String org) {
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
		
		String os = "";//系統名稱
		String toolversion = ""; //当前版本
		

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
			}	else if ("系统名称".equals(key)) {
				os = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("os", os);
			}else if ("当前版本".equals(key)) {
				toolversion = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("toolversion", toolversion);
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
		sql = appendSql(map, numType, file_index);
		return sql;
	}

	/**
	 * 插入ResideTest数据
	 */
	public StringBuffer insertResideTest(Map dataMap, String numType, String org) {
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
		
		String os = "";//系統名稱
		String toolversion = ""; //当前版本
		

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
			}else if ("系统名称".equals(key)) {
				os = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("os", os);
			}else if ("当前版本".equals(key)) {
				toolversion = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("toolversion", toolversion);
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
		sql = appendSql(map, numType, file_index);
		return sql;
	}

	/**
	 * 插入SMS数据
	 */
	public StringBuffer insertSMS(Map dataMap, String numType, String org) {
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
		
		String os = "";//系統名稱
		String toolversion = ""; //当前版本
		
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
			}	else if ("系统名称".equals(key)) {
				os = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("os", os);
			}else if ("当前版本".equals(key)) {
				toolversion = (String) (dataMap.get(key) == null ? "" : dataMap.get(key));
				map.put("toolversion", toolversion);
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
		sql = appendSql(map, numType, file_index);
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
	private StringBuffer appendSql(Map map, String numType, String fileIndex) {
		StringBuffer sql = new StringBuffer("");
		//String database = "testdataanalyse";
		StringBuffer columnStr = new StringBuffer("(");
		StringBuffer valueStr = new StringBuffer(" values (");
		StringBuffer updateStr = new StringBuffer("");
		String table = "";
		// 判断要插入数据的表名
		if (numType.equals("01001")) {
			table = "pc_speed_test";
		} else if (numType.equals("02001")) {
			table = "pc_web_browsing";
		} else if (numType.equals("03001")) {
			table = "pc_http_test";
		} else if (numType.equals("04002")) {
			table = "pc_ping";
		} else if (numType.equals("04003")) {
			table = "pc_dns";
		} else if (numType.equals("04004")) {
			table = "pc_traceroute";
		} else if (numType.equals("04005")) {
			table = "pc_mtu";
		} else if (numType.equals("04006")) {
			table = "hand_over";
		} else if (numType.equals("05001")) {
			table = "call_test";
		} else if (numType.equals("05002")) {
			table = "sms";
		} else {
			return new StringBuffer("");
		}
		// 根据查询结果判断插入还是更新
	//	boolean flag = this.queryExist( table, fileIndex);// 有数据则为true
		boolean flag = false;
																	// 执行更新
		if (flag) {
			sql.append("update "  + table + " set ");
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

	public boolean queryExist( String table, String fileIndex) {

		String sql = "select * from " + table + " where file_index = '" + fileIndex + "'";
		start();
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

	public static String testPost(String x, String y) throws IOException {

		URL url = new URL("http://api.map.baidu.com/geocoder?ak=" + ak + "&coordtype=wgs84ll&location=" + x + "," + y + "&output=json");
		URLConnection connection = url.openConnection();
		/**
		 * 然后把连接设为输出模式。URLConnection通常作为输入来使用，比如下载一个Web页。
		 * 通过把URLConnection设为输出，你可以把数据向你个Web页传送。下面是如何做：
		 */
		System.out.println(url);
		connection.setDoOutput(true);
		OutputStreamWriter out = new OutputStreamWriter(connection.getOutputStream(), "utf-8");
		// remember to clean up
		out.flush();
		out.close();
		// 一旦发送成功，用以下方法就可以得到服务器的回应：
		String res;
		InputStream l_urlStream;
		l_urlStream = connection.getInputStream();
		BufferedReader in = new BufferedReader(new InputStreamReader(l_urlStream, "UTF-8"));
		StringBuilder sb = new StringBuilder("");
		while ((res = in.readLine()) != null) {
			sb.append(res.trim());
		}
		String str = sb.toString();
		System.out.println(str);
		return str;

	}

	public static String subLalo1(String str) {
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

		String resultStr = province + "_" + city + "_" + district + "_" + street + "_" + street_number;
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
					System.out.println(str + "--->请求百度失败");
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

			if (snStart > 0) {
				String snStr = str.substring(snStart);
				if (snStr != null && snStr.length() > 0) {
					int snEnd = snStart + 16;
					String snStr1 = str.substring(snEnd);
					System.out.println(snStr1);
					int snIndex_end = snStr1.indexOf("\"");
					String street_number = snStr1.substring(0, snIndex_end);
					if (StringUtils.isNotBlank(street_number)) {
						ssq += street_number + "_";
					} else {
						ssq += "-_";
					}
				}
			} else {
				ssq += "-_";
			}
			if (ssq.endsWith("_")) {
				ssq = ssq.substring(0, ssq.length() - 1);
			}
			return ssq;
		}
		return null;
	}

	public void execute(String name) {
		start();
		CallableStatement stat = null;
		String sql = "{call " + name + "()}";
		try {
			stat = conn.prepareCall(sql);
			stat.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (stat != null) {
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
