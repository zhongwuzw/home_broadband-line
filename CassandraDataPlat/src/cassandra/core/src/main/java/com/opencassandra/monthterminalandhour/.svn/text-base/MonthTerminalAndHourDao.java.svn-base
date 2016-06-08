package com.opencassandra.monthterminalandhour;

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
import java.util.Set;

import com.mysql.jdbc.DatabaseMetaData;
import com.opencassandra.descfile.ConfParser;

/**
 * 按月份 ，时段 ，终端 统计分布
 * 
 * @author：kxc
 * @date：Dec 14, 2015
 */
public class MonthTerminalAndHourDao {

	Statement statement;
	Connection conn;
	// 数据源数据库的地址
	private static String srcurl = ConfParser.srcurl;
	private static String srcuser = ConfParser.srcuser;
	private static String srcpassword = ConfParser.srcpassword;

	// 数据入库的地址信息
	private static String dsturl = ConfParser.dsturl;
	private static String dstuser = ConfParser.dstuser;
	private static String dstpassword = ConfParser.dstpassword;

	// -----------------------------------------------------数据库连接相关操作----------------------------------------------------------
	/**
	 * 建立数据库连接
	 * 
	 * @return void
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
		if (statement != null) {
			statement.close();
		}
		if (conn != null) {
			conn.close();
		}
	}

	/**
	 * 创建数据库
	 * 
	 * @param database
	 * @return void
	 */
	public void createDB(String database) {
		try {
			// ConfParser.dsturl = ConfParser.dsturl.substring(0,
			// ConfParser.dsturl.lastIndexOf("/"));
			start(dsturl, dstuser, dstpassword);
			String sql = "create DATABASE " + database;
			statement.execute(sql);
			close();
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
	 * 根据code从code_basename对应表中获取到对应的数据库名称
	 * 
	 * @param code
	 * @return
	 * @return String
	 */
	public String getDatabaseName(String code) {

		String sql = "select * from code_basename where confcode = '" + code + "'";
		System.out.println(sql + " :::根据code查询数据库名");
		String basename = "";
		ResultSet rs = null;
		try {
			try {
				start(srcurl, srcuser, srcpassword);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			rs = (ResultSet) statement.executeQuery(sql);
			if (rs.next()) {
				basename = rs.getString("basename");
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (rs != null) {
				try {
					rs.close();
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
		return basename;
	}

	/**
	 * 查询对应表名
	 * 
	 * @param tabletype
	 * @param tdate
	 * @param srcBaseName
	 * @return
	 * @return String
	 */
	public String getDataTableName(String tabletype, String tdate, String srcBaseName) {

		String sql = "select newtablename from date_tablename where confdate = '" + tdate + "' and datatype like '" + tabletype + "%'";
		System.out.println(sql + " :::根据code查询数据库名");
		String tablename = "";
		ResultSet rs = null;
		String url = dsturl.substring(0, dsturl.lastIndexOf("/") + 1) + srcBaseName;
		try {
			start(url, dstuser, dstpassword);

			rs = statement.executeQuery(sql);
			while (rs.next()) {
				tablename = rs.getString("newtablename");
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
		return tablename;
	}

	/**
	 * 根据类型获取数据
	 * 
	 * @param srctableName
	 * @param month
	 * @param pointer
	 * @param uparea
	 * @param area
	 * @param mhinterval
	 * @return
	 * @return List<Map>
	 */
	public List<Map> getData(String srctableName, String tdate, String pointer, String uparea, String area, String mhinterval, String srcBaseName) {

		List<Map> listMap = new ArrayList<Map>();
		String sql = "";
		String wherequery = "where " + area + " !='' and " + area + "!='-' ";
		if (uparea.isEmpty()&&area.equals("province")) {
			uparea = "'全国'";
		}else if(uparea.isEmpty()&&area.equals("nationwide")){
			uparea = "'全国'";
			area = "'全国'";
			wherequery = "where province !='' and  province!='-' ";
		}

		String sumavg = "";
		if (srctableName.contains("ping")) {
			sumavg = "count(id) as testtimes ,count(id) / SUM(1 / " + pointer + ") value";
		} else {
			sumavg = "count(id) as testtimes ,sum(" + pointer + " )/count(id) value";
		}

		if (mhinterval.contains("month")) {
			sql = "select " + uparea + " as uparea , " + area + " as area ,'" + tdate + "' as yearmonth ,"
					+ "CASE when net_type LIKE'%WIFI%' then 'WIFI' WHEN net_type LIKE '%TD-S%' THEN '3G' "
					+ "when net_type LIKE '%LTE%' THEN '4G' when net_type LIKE '%EDGE%' OR  net_type LIKE '%GPRS%' OR net_type  LIKE '%GSM%' THEN '2G' else 'unknow' end nettype ,model as terminal, " + sumavg + "" + " from " + srctableName
					+ " "+wherequery+"  and " + pointer + "  > 0  " +
					" group by uparea ,area,terminal ,nettype order by value";

		} else {
			sql = "select " + uparea + " as uparea , " + area + " as area ,'" + tdate + "' as yearmonth,FROM_UNIXTIME(time / 1000, '%H') AS HOUR ,"
					+ "CASE when net_type LIKE'%WIFI%' then 'WIFI' WHEN net_type LIKE '%TD-S%' THEN '3G' "
					+ "when net_type LIKE '%LTE%' THEN '4G' when net_type LIKE '%EDGE%' OR net_type LIKE  '%GPRS%' OR net_type LIKE  '%GSM%' THEN '2G' else 'unknow' end nettype , " + sumavg + "" + " from " + srctableName +
					" "+wherequery+" and " + pointer + " >0 " 
					+ "   group by uparea ,area,nettype,hour";
		}

		ResultSet rs = null;
		String url = dsturl.substring(0, dsturl.lastIndexOf("/") + 1) + srcBaseName;

		System.out.println(sql);
		try {
			start(url, dstuser, dstpassword);

			rs = statement.executeQuery(sql);
			while (rs.next()) {
				Map map = new HashMap();

				String upareas = rs.getString("uparea");
				String areas = rs.getString("area");
				if (mhinterval.contains("month")) {
					String terminal = rs.getString("terminal");
					map.put("terminal", terminal);
				} else {
					String hour = rs.getString("hour");
					map.put("hour", hour);
				}
				String nettype = rs.getString("nettype");
				String testtimes = rs.getString("testtimes");
				String value = rs.getString("value");
				map.put("uparea", upareas);
				map.put("area", areas);
				map.put("yearmonth", tdate);
				map.put("nettype", nettype);
				map.put("testtimes", testtimes);
				map.put("value", value);

				listMap.add(map);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return listMap;
	}

	/**
	 * 查看数据入库表是否存在
	 * 
	 * @param dsttableName
	 * @param dstBaseName
	 * @return
	 * @return boolean
	 */
	public boolean queryTableExist(String dsttableName, String dstBaseName) {

		String url = dsturl.substring(0, dsturl.lastIndexOf("/") + 1) + dstBaseName;
		try {
			this.close();
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
			resultSet = databaseMetaData.getTables(null, null, dsttableName, null);
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
		}
		return flag;
	}

	/**
	 * 创建新表
	 * 
	 * @param dsttableName
	 * @param dstBaseName
	 * @param mhinterval
	 * @param pointer
	 * @return void
	 */
	public void createTable(String dsttableName, String dstBaseName, String mhinterval) {

		String parameter = "";
		if (mhinterval.contains("month")) {
			parameter = "terminal varchar(100) DEFAULT NULL ";
		} else {
			parameter = "hour varchar(10) DEFAULT NULL ";
		}

		String sql = "CREATE TABLE " + dsttableName + " ( id int(10) NOT NULL AUTO_INCREMENT,"
				+ "uparea varchar(100) DEFAULT NULL ,area varchar(100) DEFAULT NULL , INDEX  uparea_area ( uparea, area ) ,yearmonth varchar(10) DEFAULT NULL ," 
				+ "nettype varchar(100) DEFAULT NULL ,"+ parameter + " ,testtimes varchar(50) DEFAULT NULL,value varchar(30) DEFAULT NULL,"
				+ "PRIMARY KEY (id)) CHARSET=utf8 ROW_FORMAT=COMPACT;";

		String url = dsturl.substring(0, dsturl.lastIndexOf("/") + 1) + dstBaseName;
		System.out.println(sql + "::::::建表sql");
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
	 * 查询该月份的数据是否已存在
	 * 
	 * @param dsttableName
	 * @param dstBaseName
	 * @param month
	 * @return
	 * @return boolean
	 */
	public boolean queryDataExist(String dsttableName, String dstBaseName, String tdate) {

		String sql = "select count(id) count from " + dsttableName + " where yearmonth = '" + tdate + "'";
		int count = 0;

		String url = dsturl.substring(0, dsturl.lastIndexOf("/") + 1) + dstBaseName;
		System.out.println(sql + "::::::查询月份数据是否已存在sql");
		ResultSet rs = null;
		boolean flage = false;
		try {
			start(url, dstuser, dstpassword);
			rs = statement.executeQuery(sql);
			while (rs.next()) {
				count = rs.getInt("count");
				if (count > 0) {
					flage = true;
				}
			}
		} catch (Exception e) {
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
	 * 删除已存在的月份数据
	 * 
	 * @param dsttableName
	 * @param dstBaseName
	 * @param month
	 * @return void
	 */
	public void delMonthData(String dsttableName, String dstBaseName, String tdate) {

		String sql = "delete from " + dsttableName + " where yearmonth= '" + tdate + "' ";
		String url = dsturl.substring(0, dsturl.lastIndexOf("/") + 1) + dstBaseName;
		System.out.println(sql + "::::::查询月份数据是否已存在sql");
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
	 * 查询时段分布表中该数据是否存在
	 * 
	 * @param dsttableName
	 * @param dstBaseName
	 * @param map
	 * @return
	 * @return String
	 */
	public String queryHourDataExist(String dsttableName, String dstBaseName, Map map) {

		String uparea = (String) map.get("uparea");
		String area = (String) map.get("area");
		String nettype = (String) map.get("net_type");
		String hour = (String) map.get("hour");
		String id = "";
		String sql = "select id from " + dsttableName + " where uparea= '" + uparea + "' and area='" + area + "' and net_type='" + nettype + "' and hour='" + hour + "'";
		String url = dsturl.substring(0, dsturl.lastIndexOf("/") + 1) + dstBaseName;

		ResultSet rs = null;
		boolean flage = false;
		System.out.println(sql + "::::::查询月份数据是否已存在sql");
		try {
			start(url, dstuser, dstpassword);
			rs = statement.executeQuery(sql);
			while (rs.next()) {
				id = rs.getString("id");
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return id;
	}

	/**
	 * 拼接插入sql
	 * 
	 * @param dsttableName
	 * @param map
	 * @return
	 * @return String
	 */
	public String appendInsertSql(String dsttableName, Map map) {
		StringBuffer sql = new StringBuffer("");
		StringBuffer columnStr = new StringBuffer("(");
		StringBuffer valueStr = new StringBuffer(" values (");

		sql.append("insert into " + dsttableName + " ");
		try {
			Set set = map.keySet();
			Iterator iter = set.iterator();
			while (iter.hasNext()) {
				String name = (String) iter.next();
				String value = map.get(name) == null ? "" : map.get(name).toString();
				if (value.isEmpty()) {
					// continue;
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
		}
	//	System.out.println(sql.toString() + "  :::::拼装insertsql");
		return sql.toString();

	}

	/**
	 * 拼接更新sql
	 * 
	 * @param dsttableName
	 * @param map
	 * @param id
	 * @return
	 * @return String
	 */
	public String appendUpdateSql(String dsttableName, Map map, String id) {
		StringBuffer sql = new StringBuffer("");
		StringBuffer columnStr = new StringBuffer("(");
		StringBuffer valueStr = new StringBuffer(" values (");
		StringBuffer updateStr = new StringBuffer("");

		sql.append("update " + dsttableName + " set ");
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
			sql.append("where id = '" + id + "'");
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println(sql.toString());
		return sql.toString();

	}

	/**
	 * 静态sql的批量插入
	 * 
	 * @param sqlList
	 * @return
	 */
	public boolean dealSqlList(List<String> sqllist, String dstBaseName) {

		// 获取数据库连接
		int count = 0;
		boolean flag = false;
		try {
			String url = dsturl.substring(0, dsturl.lastIndexOf("/") + 1) + dstBaseName;
			try {
				close();
				start(url, dstuser, dstpassword);
			} catch (Exception e) {
				e.printStackTrace();
			}
			conn.setAutoCommit(false);
			for (Iterator iterator = sqllist.iterator(); iterator.hasNext();) {
				String sql = (String) iterator.next();
				// count++;
				System.out.println(sql + "  :::::插入sql");
				statement.addBatch((String) sql);
				// 1000条记录插入一次
				count++;
				// System.out.println(count);
				if (count % 20000 == 0) {
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

}
