package com.opencassandra.v01.dao.impl;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.text.Format;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.mysql.jdbc.DatabaseMetaData;
import com.opencassandra.descfile.ConfParser;

/**
 * 进行终端数据的插入（中间表、以及map等级分类数据表）
 * 
 * @author：kxc
 * @date：Oct 21, 2015
 */
public class InsertMysqlByDataToZD {
	// 数据库连接
	Statement statement;
	Connection conn;
	private static String[] mapLevel = ConfParser.mapLevel;
	Format fm = new DecimalFormat("#.######");

	/**
	 * 连接数据库
	 * 
	 * @return void
	 */
	public void start() {
		String driver = "com.mysql.jdbc.Driver";
		String url = ConfParser.url;
		String user = ConfParser.user;
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

	/**
	 * 关闭数据库
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
	 * 执行sql
	 * 
	 * @param sql
	 * @return
	 * @return boolean
	 */
	public boolean insert(String sql) {

		System.out.println("sql:" + sql);
		boolean flag = false;
		start();
		if (sql.isEmpty()) {
			flag = false;
		} else {
			try {
				flag = statement.execute(sql);
			} catch (SQLException e) {
				e.printStackTrace();
				return false;
			} finally {
				try {
					close();
				} catch (SQLException e) {
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
	public boolean insertSqlMap(Map sqlMap) {

		// 获取数据库连接
		this.start();
		int count = 0;
		boolean flag = false;
		try {
			if (sqlMap != null && sqlMap.size() > 0) {

				Iterator iter = sqlMap.entrySet().iterator();
				while (iter.hasNext()) {
					Map.Entry entry = (Map.Entry) iter.next();
					Object sqlkey = entry.getKey();
					Object val = entry.getValue();

					sqlkey = sqlkey.toString().replace("ternumber", val + "");

					System.out.println(sqlkey + "  ：修改过后的sql");
					conn.setAutoCommit(false);
					statement.addBatch((String) sqlkey);
					// 1000条记录插入一次
					System.out.println(count);
					if (count++ % 1000 == 0) {
						statement.executeBatch();
						conn.commit();
					}
				}
				// 不足1000的最后进行插入
				statement.executeBatch();
				conn.commit();
				flag = true;
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
		return flag;
	}

	/**
	 * 插入insertZDdata数据
	 * 
	 * @param dataMap
	 *            拼装过后的csv问价数据
	 * @param keyspace
	 *            CMCC_GUANGXI_PINZHIBU23G
	 * @param numType
	 *            05001 确定数据要插入的表
	 * @param detailreport
	 * @param dataOrg
	 *            当前时间
	 * @return void
	 */
	@SuppressWarnings("unchecked")
	public void insertZDdata(Map dataMap, String testtime, String imei) {
		System.out.println("insertZDdata-----------------" + new Date().toLocaleString());
		Map map = new HashMap();
		Format fm = new DecimalFormat("#.######");
		String longitude = "";
		String latitude = "";
		String gpsStr = "";
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
		if (gpsStr.equals("--")) {
			return;
		}
		if (gpsStr.contains(" ") && !gpsStr.equals("(null) (null)")) {
			String[] gps = transGpsPoint(gpsStr);
			System.out.println(gps[0] + " ::::::GPS位置lng");
			System.out.println(gps[1] + " ::::::GPS位置lat");
			if (gps != null && gps[0] != null && gps[1] != null) {
				longitude = gps[0];
				latitude = gps[1];
			} else {
				System.out.println("经纬度错误 不插入此数据");
				return;
			}
		}
		if (longitude.isEmpty() || latitude.isEmpty()) {
			System.out.println("经纬度错误 不插入此数据");
			return;
		}
		map.put("lng", fm.format(Double.parseDouble(longitude)));
		map.put("lat", fm.format(Double.parseDouble(latitude)));
		map.put("imei", imei);
		map.put("testtime", testtime);
		appAndecuSql(map, testtime, imei, ConfParser.temptable);
	}

	/**
	 * 根据解析文件所得的数据插入相应的表中
	 * 
	 * @param map
	 * @param testtime
	 * @param imei
	 * @param table
	 * @return void
	 */
	@SuppressWarnings({ "rawtypes" })
	private void appAndecuSql(Map map, String testtime, String imei, String table) {
		System.out.println("appendSQL------------------start :" + new Date().toLocaleString());
		StringBuffer sql = new StringBuffer("");
		StringBuffer columnStr = new StringBuffer("(");
		StringBuffer valueStr = new StringBuffer(" values (");
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
				columnStr = new StringBuffer(columnStr.toString().substring(0, columnStr.toString().lastIndexOf(",")));
			}
			while (valueStr.toString().trim().endsWith(",")) {
				valueStr = new StringBuffer(valueStr.toString().substring(0, valueStr.toString().lastIndexOf(",")));
			}
			columnStr.append(" )");
			valueStr.append(" )");
			sql.append(columnStr);
			sql.append(valueStr);
			// 获取数据库连接
			start();
			// 插入数据（临时表）
			insert(sql.toString());
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

		return;
	}

	/**
	 * 根据配置文件中的map等级进行数据库查询对应的lng、lat的值
	 * 
	 * @param mapLevel
	 * @return
	 * @return List
	 */
	@SuppressWarnings("unchecked")
	public List getLNGLATByLevel(String mapLevel) {
		List list = new ArrayList();
		String sql = "SELECT map.loninterval lng,map.latinterval lat FROM map_level map where level = " + mapLevel;
		double lng = 0;
		double lat = 0;
		Connection conn1 = null;
		Statement statement1 = null;
		try {
			String driver = "com.mysql.jdbc.Driver";
			String url = ConfParser.url.substring(0, ConfParser.url.lastIndexOf("/") + 1) + "static_param";
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
	 * 查询对应的表是否存在
	 * 
	 * @param tableName
	 * @return
	 * @return boolean
	 */
	public boolean queryTableExist(String tableName) {
		System.out.println(ConfParser.url);
		start();
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
	 * 根据imei、testtime查询相应的数据是否存在，若存在则跳过，不存在继续解析文件进行数据的插入
	 * 
	 * @param table
	 * @param testtime
	 * @param imei
	 * @return
	 * @return boolean
	 */
	public boolean queryExist(String table, String testtime, String imei) {

		String sql = "select * from " + table + " where imei = '" + imei + "' and testtime = " + testtime;

		System.out.println(sql + "   :::::::查询数据存在sql");
		boolean flage = false;
		start();
		try {
			ResultSet rs = (ResultSet) statement.executeQuery(sql);
			if (rs.next()) {
				flage = true;
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
		return flage;
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
	public Map queryTer(String table, String lng, String lat) {

		String sql = "select * from " + table + " where lng = '" + lng + "' and lat = " + lat;
		Map map = new HashMap();
		System.out.println(sql + "   :::::::查询数据存在sql");
		boolean flage = false;
		start();
		try {
			ResultSet rs = (ResultSet) statement.executeQuery(sql);
			if (rs.next()) {
				flage = true;
				map.put("id", rs.getString("id"));
				map.put("test_times", rs.getInt("test_times"));
				;
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

					float gpsLong = Float.parseFloat(degrees) + Float.parseFloat(minutes) / 60 + Float.parseFloat(seconds) / 3600;

					DecimalFormat decimalFormat = new DecimalFormat(".000000");
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
						DecimalFormat decimalFormat = new DecimalFormat(".000000");
						gpsPoint = decimalFormat.format(gpsLong);
						longitude = gpsPoint;
					} else if (info[i].contains("N")) {
						gpsPoint = info[i].substring(0, info[i].lastIndexOf("N"));
						float gpsLong = Float.valueOf(gpsPoint);
						DecimalFormat decimalFormat = new DecimalFormat(".000000");
						gpsPoint = decimalFormat.format(gpsLong);
						latitude = gpsPoint;
					} else {
						gpsPoint = info[i];
						float gpsLong = Float.valueOf(gpsPoint);
						DecimalFormat decimalFormat = new DecimalFormat(".000000");
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
	 * 创建中间表
	 * 
	 * @param tablename
	 * @return
	 * @return boolean
	 */
	public boolean createTab(String tablename) {
		boolean flag = true;
		String sql = "CREATE TABLE "
				+ tablename
				+ " ( id int(10) NOT NULL AUTO_INCREMENT,imei varchar(100),lng decimal(10,6) DEFAULT NULL,lat decimal(10,6) DEFAULT NULL,testtime varchar(20),PRIMARY KEY (id)) CHARSET=utf8 ROW_FORMAT=COMPACT;";
		try {
			start();
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
	 * 把临时表中的数据更新到正是表中（Terminal_201509_1 Terminal_年月_maplevel）
	 * 
	 * @return
	 * @return boolean
	 */
	public boolean queryAndCreat() {
		// 存放临时表中查询出来的所有的指定月份的数据
		List<Map> lnglatList = new ArrayList<Map>();
		List<String> terSql = new ArrayList<String>();
		String sql = "";
		double lng = 0;
		double lat = 0;
		boolean fag = false;
		Map sqlMap = new HashMap();
		String database = ConfParser.url.substring(ConfParser.url.lastIndexOf("/") + 1, ConfParser.url.length());
		System.out.println(ConfParser.url);
		// 根据配置文件创建相应的数据库
		try {
			this.start();
		} catch (Exception e) {
			this.createDB(database);
			try {
				this.start();
			} catch (Exception e1) {
				e1.printStackTrace();
			}
		}
		try {
			this.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}

		String[] testdate = ConfParser.testdate;
		// 遍历获取到的配置指定时间
		for (int i = 0; i < testdate.length; i++) {
			// 配置的指定时间，按该时间进行建表以及数据的查询
			String date = testdate[i];
			// 根据指定时间查询所有临时表中的
			lnglatList = this.getlnglat(ConfParser.temptable, date);
			//根据时间进行一个数据的查询，如果没有数据的存在则直接跳过
			boolean dateFlage = this.queryIsEx(date);
			if (dateFlage) {
				for (int j = 0; j < mapLevel.length; j++) {
					if (!date.isEmpty()) {
						String terTable = "terminal" + "_" + date + "_" + mapLevel[j];

						try {
							boolean fal = this.queryTableExist(terTable);
							if (!fal) {
								// 创建对应的表
								this.createTerTab(terTable);
							}
						} catch (Exception e) {
							e.printStackTrace();
						}

						for (Iterator iterator = lnglatList.iterator(); iterator.hasNext();) {
							Map map = (Map) iterator.next();
							lng = (Double) map.get("lng");
							lat = (Double) map.get("lat");
							// 处理经纬度数据
							double[] em = Earth2Mars.transform(lng, lat);
							double[] bf = BdFix.bd_encrypt(em[0], em[1]);
							double longitudeNum = bf[0];
							double latitudeNum = bf[1];
							List list = this.getLNGLATByLevel(mapLevel[j]);
							double LNGinterval = (Double) list.get(0);
							double LATinterval = (Double) list.get(1);
							lng = ((int) (longitudeNum / LNGinterval)) * LNGinterval + LNGinterval / 2;
							lat = ((int) (latitudeNum / LATinterval)) * LATinterval + LATinterval / 2;
							Map nummap = this.queryTer(terTable, fm.format(lng), fm.format(lat));
							boolean flag = (Boolean) nummap.get("flag");

							System.out.println(flag + " ::::::flage + id :::: " + nummap.get("id"));
							if (!flag) {
								sql = "insert into " + terTable + " (lng,lat,test_times) values('" + fm.format(lng) + "','" + fm.format(lat) + "',ternumber)";
								terSql.add(sql);
							} else {
								int terminalnum = (Integer) nummap.get("test_times");
								sql = "update " + terTable + " set test_times='" + (terminalnum + 1) + "' where id=" + nummap.get("id");
								// 更新操作直接进行
								fag = insert(sql);
							}
							// fag = insert(sql);
							// 执行数据的批量插入
							if (terSql.size() % 1000 == 0) {
								fag = this.listMap(terSql);
							}
						}
					}
				}
				fag = this.listMap(terSql);
			}
		}
		return fag;
	}

	/**
	 * 根据配置文件创建相应的数据库
	 * 
	 * @param database
	 * @return void
	 */
	public void createDB(String database) {
		String oldUrl = ConfParser.url;
		try {
			ConfParser.url = ConfParser.url.substring(0, ConfParser.url.lastIndexOf("/"));
			start();
			String sql = "create DATABASE " + database;
			statement.execute(sql);
			close();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				close();
				ConfParser.url = oldUrl;
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * 按月份获取到临时表中的终端数据，并把数据放入到listmap中
	 * 
	 * @param table
	 * @param testdate
	 * @return
	 * @return List<Map>
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public List<Map> getlnglat(String table, String testdate) {
		List<Map> list = new ArrayList<Map>();
		String sql = "select grid.lng lng,grid.lat lat FROM " + table + " grid where testtime ='" + testdate + "'";
		double lng = 0;
		double lat = 0;
		try {
			this.start();
			System.out.println(sql);
			ResultSet rs = statement.executeQuery(sql);
			while (rs.next()) {
				Map map = new HashMap();
				lng = rs.getDouble("lng");
				lat = rs.getDouble("lat");
				map.put("lng", lng);
				map.put("lat", lat);
				list.add(map);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				this.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return list;
	}

	/**
	 * 创建正式表
	 * 
	 * @param tablename
	 * @return
	 * @return boolean
	 */
	public boolean createTerTab(String tablename) {
		boolean flag = true;
		String sql = "CREATE TABLE " + tablename
				+ " ( id int(10) NOT NULL AUTO_INCREMENT,lng decimal(10,6) DEFAULT NULL,lat decimal(10,6) DEFAULT NULL,test_times varchar(20),PRIMARY KEY (id)) CHARSET=utf8 ROW_FORMAT=COMPACT;";
		try {
			start();
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
	 * 处理放入list中的sql，查询重复的插入语句，并记录数目放入map中
	 * 
	 * @param terSql
	 * @return
	 * @return boolean
	 */
	public boolean listMap(List terSql) {
		Map sqlMap = new HashMap();
		boolean fag = false;
		Iterator<String> it = terSql.iterator();
		while (it.hasNext()) {
			String strSql = it.next();
			System.out.println(strSql + ": " + Collections.frequency(terSql, strSql));
			sqlMap.put(strSql, Collections.frequency(terSql, strSql));
		}
		// 执行批量插入
		fag = insertSqlMap(sqlMap);
		terSql.clear();
		sqlMap.clear();
		return fag;

	}

	/**
	 * 根据测试时间进行数据的查询，看着一个月份的数据是否存在
	 * 
	 * @param date
	 * @return
	 * @return boolean
	 */
	public boolean queryIsEx(String date) {
		String sql = "select count(id) cou FROM gridtable grid where grid.testtime ='" + date + "'";
		boolean flage = false;
		int count = 0;
		try {
			this.start();
			ResultSet rs = statement.executeQuery(sql);
			while (rs.next()) {
				count = rs.getInt("cou");
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				this.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		
		if(count>0){
			flage = true;
		}
		System.out.println(flage+"   ::::::按月份数据查询的结果");
		return flage;
	}
	
	
	public static void main(String[] args) {
		
		InsertMysqlByDataToZD insertMysqlByDataToZD = new InsertMysqlByDataToZD();
				
		insertMysqlByDataToZD.queryAndCreat();
	}
}
