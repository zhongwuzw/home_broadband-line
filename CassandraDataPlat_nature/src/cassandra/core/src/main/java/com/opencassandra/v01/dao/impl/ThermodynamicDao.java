package com.opencassandra.v01.dao.impl;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import com.mysql.jdbc.DatabaseMetaData;
import com.mysql.jdbc.exceptions.MySQLSyntaxErrorException;
import com.opencassandra.descfile.ConfParser;
import com.opencassandra.descfile.GetPostTest;

/**
 * 处理信号强度热力图数据
 * 
 * @author：kxc
 * @date：Oct 29, 2015
 */
public class ThermodynamicDao {
	Statement statement;
	Connection conn;

	/**
	 * 
	 * 带时间段的查询
	 * 
	 * select longitude lng,latitude lat ,signal_strength signalstrength from
	 * ping_new where longitude > 1 and signal_strength < 0 and net_type like
	 * '%TD-S%' and time>=UNIX_TIMESTAMP('2015-10-01 00:00:00')*1000 and
	 * time<=UNIX_TIMESTAMP('2015-10-31 23:59')*1000
	 * 
	 * 通过nettype查询出相应数据放入map中
	 * 
	 * @param table
	 * @param nettype
	 * @return
	 * @return Map<String,String>
	 */

	public List<Map<String, String>> getTypeData(String table, String nettype, String code) {

		List<Map<String, String>> listMap = new ArrayList<Map<String, String>>();
		String sql = "";
		// String[] code = ConfParser.code;
		// 根据code查询出的device_org 拼装查询条件
		String device_orgs = this.getDevice(code) == "" ? "" : "and (" + this.getDevice(code) + ")";
		try {
			this.close();
			try {
				this.start();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			if (!nettype.isEmpty() && ("2G").equals(nettype)) {
				sql = "select longitude lng,latitude lat ,signal_strength signalstrength from " + table
						+ " where longitude > 1 and signal_strength < 0 and (net_type like '%EDGE%' or net_type like '%GPRS%' or net_type like '%GSM%') " + device_orgs + "";
			} else if (!nettype.isEmpty() && ("3G").equals(nettype)) {
				sql = "select longitude lng,latitude lat ,signal_strength signalstrength from " + table + " where longitude > 1 and signal_strength < 0 and net_type like '%TD-S%' " + device_orgs + "";
			} else if (!nettype.isEmpty() && ("4G").equals(nettype)) {
				sql = "select longitude lng,latitude lat ,signal_strength signalstrength from " + table + " where longitude > 1 and signal_strength < 0 and net_type like '%LTE%' " + device_orgs + "";
			}
			System.out.println(sql + "  :::查询总量sql");
			ResultSet rs = statement.executeQuery(sql);

			while (rs.next()) {
				Map<String, String> map = new HashMap<String, String>();
				String lng = rs.getString("lng");
				String lat = rs.getString("lat");
				String signalstrength = rs.getString("signalstrength");
				map.put("lng", lng);
				map.put("lat", lat);
				map.put("signalstrength", signalstrength);
				listMap.add(map);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return listMap;
	}
	public List<Map<String, String>> getTypeTest() {
		
		List<Map<String, String>> listMap = new ArrayList<Map<String, String>>();
		String sql = "";
		try {
			this.close();
			try {
				this.start();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
				sql = "select longitude lng,latitude lat ,id from ping_new_20151102 where longitude>0 and latitude>0";
			System.out.println(sql + "  :::查询总量sql");
			ResultSet rs = statement.executeQuery(sql);
			
			while (rs.next()) {
				Map<String, String> map = new HashMap<String, String>();
				String lng = rs.getString("lng");
				String lat = rs.getString("lat");
				String id = rs.getString("id");
				map.put("lng", lng);
				map.put("lat", lat);
				map.put("id", id);
				listMap.add(map);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return listMap;
	}

	/**
	 * 建立数据库连接
	 * 
	 * @return void
	 */
	public void start() throws Exception {
		String driver = "com.mysql.jdbc.Driver";
		String url = ConfParser.url;
		System.out.println(url + "::::::::::::");
		String user = ConfParser.user;
		String password = ConfParser.password;
		Class.forName(driver);
		conn = DriverManager.getConnection(url, user, password);
		statement = conn.createStatement();
	}

	/**
	 * 建立数据库连接
	 * 
	 * @return void
	 */
	public void datastart() throws Exception {
		String driver = "com.mysql.jdbc.Driver";
		String url = ConfParser.dataurl;
		String user = ConfParser.user;
		String password = ConfParser.password;
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
		if (statement != null) {
			statement.close();
		}
		if (conn != null) {
			conn.close();
		}
	}

	/**
	 * 在配置文件中同时配置两个数据库ip时使用的一个
	 * 
	 * @param database
	 * @return void
	 */
	public void createDB(String database) {
		String oldUrl = ConfParser.dataurl;
		try {
			ConfParser.dataurl = ConfParser.dataurl.substring(0, ConfParser.dataurl.lastIndexOf("/"));
			datastart();
			String sql = "create DATABASE " + database;
			statement.execute(sql);
			close();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				close();
				ConfParser.dataurl = oldUrl;
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * 根据配置文件中的map等级进行数据库查询对应的lng、lat的值
	 * 
	 * @param mapLevel
	 * @return
	 * @return List
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public List getLNGLATByLevel(String mapLevel) {
		List list = new ArrayList();
		String sql = "SELECT map.loninterval lng,map.latinterval lat FROM map_level map where level = " + mapLevel;
		double lng = 0;
		double lat = 0;
		Connection conn1 = null;
		Statement statement1 = null;
		try {
			String driver = "com.mysql.jdbc.Driver";
			String url = ConfParser.dataurl.substring(0, ConfParser.dataurl.lastIndexOf("/") + 1) + "static_param";
			System.out.println("******************" + url);
			String user = ConfParser.user;
			String password = ConfParser.password;
			Class.forName(driver);
			conn1 = DriverManager.getConnection(url, user, password);
			statement1 = conn1.createStatement();
			System.out.println(sql);
			ResultSet rs = statement1.executeQuery(sql);
			if (rs.next()) {
				lng = rs.getDouble("lng");
				lat = rs.getDouble("lat");
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
		list.add(lng);
		list.add(lat);
		return list;
	}

	/**
	 * 创建相应的数据库表
	 * 
	 * @param tablename
	 * @return
	 * @return boolean
	 */
	public boolean createTherTab(String tablename) {

		boolean flag = true;
		String sql = "CREATE TABLE " + tablename + " ( id int(10) NOT NULL AUTO_INCREMENT," + "lng decimal(10,6) DEFAULT NULL," + "lat decimal(10,6) DEFAULT NULL,"
				+ "test_times varchar(20) DEFAULT NULL," + "avg_signalstrength decimal(10,3) DEFAULT NULL," + "PRIMARY KEY (id)) CHARSET=utf8 ROW_FORMAT=COMPACT;";
		try {
			datastart();
			statement.execute(sql);
		} catch (Exception e) {
			flag = false;
			e.printStackTrace();
		} finally {
			try {
				close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return flag;

	}

	/**
	 * 查询ter表中是否存在
	 * 
	 * @param table
	 * @param lng
	 * @param lat
	 * @return
	 * @return boolean
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public Map queryTher(String table, String lng, String lat) {

		String sql = "select * from " + table + " where lng = '" + lng + "' and lat = " + lat;
		Map map = new HashMap();
		System.out.println(sql + "   :::::::查询数据存在sql");
		boolean flage = false;
		try {
			try {
				datastart();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			ResultSet rs = (ResultSet) statement.executeQuery(sql);
			if (rs.next()) {
				flage = true;
				map.put("id", rs.getString("id"));
				map.put("test_times", rs.getInt("test_times"));
				map.put("avg_signalstrength", rs.getDouble("avg_signalstrength"));
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
		map.put("flag", flage);
		return map;
	}

	/**
	 * ]执行sql插入
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
				try {
					this.datastart();
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				flag = statement.execute(sql);
			} catch (SQLException e) {
				e.printStackTrace();
				return false;
			} finally {
				try {
					close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		return flag;
	}

	/**
	 * 静态sql的批量插入
	 * 
	 * @param sqlList
	 * @return
	 */
	public boolean insertList(List<String> sqllist) {

		// 获取数据库连接
		int count = 0;
		boolean flag = false;
		try {
			try {
				/**
				 * 在进行ping_new_20151102数据更新的时候有过修改，需改成this.datastart();
				 */
				this.datastart();
			} catch (Exception e) {
				e.printStackTrace();
			}
			for (Iterator iterator = sqllist.iterator(); iterator.hasNext();) {
				String sql = (String) iterator.next();
				// count++;
				conn.setAutoCommit(false);
				statement.addBatch((String) sql);
				// 1000条记录插入一次
				count++;
				//System.out.println(count);
				if (count % 1000 == 0) {
					statement.executeBatch();
					conn.commit();
				}
			}
			// 不足1000的最后进行插入
			statement.executeBatch();
			conn.commit();
			flag = true;
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return flag;
	}

	/**
	 * 复制上一个月表的数据到最新的月份
	 * 
	 * @param table
	 * @param table_domain
	 */
	public void copyTable(String table, String table_domain) {
		String copySql = "insert into " + table + " select * from " + table_domain;
		try {
			start();
			this.insert(copySql);
			System.out.println("复制表" + table_domain + "到" + table + "成功");
		} catch (Exception e) {
			System.out.println("复制表" + table_domain + "到" + table + "失败");
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
	 * 查询数据库中表是否存在
	 * 
	 * @param tableName
	 * @return
	 * @return boolean
	 */
	public boolean queryTableExist(String tableName) {
		try {
			datastart();
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		boolean flag = false;
		DatabaseMetaData databaseMetaData;
		try {
			databaseMetaData = (DatabaseMetaData) conn.getMetaData();
			ResultSet resultSet = databaseMetaData.getTables(null, null, tableName, null);
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
			try {
				close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return flag;
	}

	/**
	 * 把配置文件中的code拼装成字符串，用于拼接查询sql的限制条件
	 * 
	 * @param code
	 * @return
	 * @return String
	 */
	public String getDevice(String code) {
		List<String> keyList = new ArrayList<String>();
		String codeUrl = ConfParser.getCodePath;
		System.out.println(codeUrl);
		// String codeStr = "";
		StringBuffer codebBuffer = new StringBuffer();
		if (code != null && !"".equals(code)) {
			code = "code=" + code;
			String sr = GetPostTest.sendGet(codeUrl, code);
			System.out.println(sr);
			JSONObject json = JSONObject.fromObject(sr);
			try {
				JSONArray jsonArray = (JSONArray) json.get("detail");
				for (int i = 0; i < jsonArray.size(); ++i) {
					JSONObject keys = (JSONObject) jsonArray.get(i);
					codebBuffer.append(" device_org = '" + keys.getString("key") + "' or ");
					keyList.add(keys.getString("key"));
				}
				JSONObject thisOrgInfo = json.getJSONObject("thisOrgInfo");
				String keySelf = thisOrgInfo.getString("key");
				codebBuffer.append(" device_org = '" + keySelf + "' or ");

				keyList.add(keySelf);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		String codes = codebBuffer.toString();
		codes = codes.substring(0, codes.lastIndexOf("or"));
		System.out.println(codes);
		return codes;
	}

}
