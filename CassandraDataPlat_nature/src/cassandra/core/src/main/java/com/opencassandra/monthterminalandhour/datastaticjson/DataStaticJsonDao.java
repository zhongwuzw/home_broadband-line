package com.opencassandra.monthterminalandhour.datastaticjson;

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

import com.mysql.jdbc.DatabaseMetaData;
import com.mysql.jdbc.exceptions.MySQLSyntaxErrorException;
import com.opencassandra.descfile.ConfParser;

/**
 * 数据汇总 从中间表中把数据整理转入json表
 * 
 * @author：kxc
 * @date：Dec 15, 2015
 */
@SuppressWarnings({ "unchecked", "rawtypes" })
public class DataStaticJsonDao {

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
	static boolean fal = false;
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
	 * 
	 * 获取数据源数据
	 * @param tablename
	 * @param basename
	 * @param interval
	 * @param pointer
	 * @return
	 * @return List<Map>
	 */
	public List<Map> getdata(String tablename, String basename, String interval,int start,int end) {

		List<Map> listMap = new ArrayList<Map>();
		String sql = "";

		String sumavg = "";
		if (tablename.contains("ping")) {
			sumavg = "SUM(testtimes) / SUM(testtimes / value) value";
		} else {
			sumavg = "sum(testtimes*value)/sum(testtimes) value";
		}

		if (interval.contains("month") || interval.contains("terminal")) {
			sql = "select uparea,area,nettype,terminal,yearmonth,sum(testtimes) testtimes," + sumavg + " from " + tablename + " " +
					"where nettype!='' and nettype is not null and value!='' and value is not null AND (uparea != area or area = '全国') " +
					"group by uparea,area,nettype," + interval + " order by "
					+ interval + " limit "+start+" ,"+end+"";// "+interval+"

		} else {
			sql = "select * from " + tablename + " where nettype!='' and nettype is not null and value!='' and value is not null AND (uparea != area or area = '全国')  order by " + interval + "  limit "+start+" ,"+end+"";
		}


		System.out.println(sql + " :::数据查询sql");
		ResultSet rs = null;
		String url = dsturl.substring(0, dsturl.lastIndexOf("/") + 1) + basename;
		fal = false;
		try {
			start(url, dstuser, dstpassword);

			rs = statement.executeQuery(sql);
			while (rs.next()) {
				fal= true;
				Map map = new HashMap();
				String uparea = rs.getString("uparea");
				String area = rs.getString("area");
				String net_type = rs.getString("nettype");
				String testtimes = rs.getString("testtimes");
				String value = rs.getString("value");
				/**
				 * 
				 * 2015年12月16日 10:28:14 可以处理所有的类型（按month，terminal，hour 分组出来的数据）
				 * 
				 */
				String parameter = rs.getString(interval);
				if (net_type != null && !net_type.isEmpty()) {
					map.put("uparea", uparea);
					map.put("area", area);
					map.put("net_type", net_type);
					map.put("testtimes", testtimes);
					map.put("value", value);
					map.put("parameter", parameter);
					listMap.add(map);
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
		return listMap;
	}

	/**
	 * 
	 * 根据上下级的地区位置查询是否已有该地区数据存在
	 * @param uparea
	 * @param area
	 * @param tablename
	 * @param basename
	 * @return
	 * @return boolean
	 */
	public boolean queryDataExist(String uparea, String area, String tablename, String basename) {
		String sql = "select count(id) count from " + tablename + " where uparea='" + uparea + "' and area = '" + area + "'";
		ResultSet rs = null;
		String url = dsturl.substring(0, dsturl.lastIndexOf("/") + 1) + basename;
		int count = 0;
		boolean flage = false;
		System.out.println(sql+  "     ::::存在查询");
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
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return flage;
	}

	/**
	 * 静态sql的批量插入
	 * 
	 * @param sqlList
	 * @return
	 */
	public boolean insertList(List<String> sqllist, String dataBaseName) {

		// 获取数据库连接
		int count = 0;
		boolean flag = false;
		try {
			String url = dsturl.substring(0, dsturl.lastIndexOf("/") + 1) + dataBaseName;
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

	/**
	 * 查询数据入库的表是否存在
	 * @param dsttablename
	 * @param basename
	 * @return
	 * @return boolean
	 */
	public boolean queryDstTableExist(String dsttablename, String basename) {

		String url = dsturl.substring(0, dsturl.lastIndexOf("/") + 1) + basename;
		boolean flag = false;
		ResultSet resultSet = null;
		DatabaseMetaData databaseMetaData;
		try {
			start(url, dstuser, dstpassword);
			databaseMetaData = (DatabaseMetaData) conn.getMetaData();
			resultSet = databaseMetaData.getTables(null, null, dsttablename, null);
			if (resultSet.next()) {
				flag = true;
			} else {
				flag = false;
			}
			resultSet.close();
		} catch (SQLException e) {
			flag = false;
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
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
	 * 删除并创建新表
	 * @param dsttablename
	 * @param basename
	 * @return void
	 */
	public void dropAndCreateDstTable(String dsttablename, String basename) {

		DataStaticJsonDao dao = new DataStaticJsonDao();
		String url = dsturl.substring(0, dsturl.lastIndexOf("/") + 1) + basename;
		ResultSet resultSet = null;
		try {
			start(url, dstuser, dstpassword);
			String dropsql = "drop table " + dsttablename + "";
			statement.execute(dropsql);
			dao.createDstTable(dsttablename, basename);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (resultSet != null) {
				try {
					resultSet.close();
					close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}

	/**
	 * 创建新表（该表为数据入库表）
	 * @param dsttablename
	 * @param basename
	 * @return void
	 */
	public void createDstTable(String dsttablename, String basename) {
		String createsql = "CREATE TABLE " + dsttablename + " ( id int(10) NOT NULL AUTO_INCREMENT,"
				+ "`uparea`  varchar(100)  DEFAULT NULL ,`area`  varchar(100)  DEFAULT NULL ,`month_testtimes`  varchar(2550)  DEFAULT NULL ,"
				+ "`month_value`  varchar(2550)  DEFAULT NULL ,`terminal_testtimes`  varchar(2550)  DEFAULT NULL ,`terminal_value`  varchar(2550)  DEFAULT NULL ,"
				+ "`hour_testtimes`  varchar(2550)  DEFAULT NULL ,`hour_value`  varchar(2550)  DEFAULT NULL , `testtimes` int(11)  DEFAULT NULL ,`value`  decimal(20,5)  DEFAULT NULL ,`2gtvalue`  int(11)  DEFAULT NULL ,`3gtvalue`  int(11)  DEFAULT NULL ,`4gtvalue`  int(11)  DEFAULT NULL ,`wifitvalue`  int(11)  DEFAULT NULL ," +
				"`2gvaluet`  decimal(20,5)  DEFAULT NULL ,`3gvaluet` decimal(20,5)  DEFAULT NULL ,`4gvaluet`  decimal(20,5)  DEFAULT NULL ,`wifivaluet`  decimal(20,5)  DEFAULT NULL ,"
				+ "PRIMARY KEY (id),INDEX latlng(uparea,area)) CHARSET=utf8 ROW_FORMAT=COMPACT;";

		String url = dsturl.substring(0, dsturl.lastIndexOf("/") + 1) + basename;
		ResultSet resultSet = null;
		try {
			start(url, dstuser, dstpassword);
			statement.execute(createsql);

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (resultSet != null) {
				try {
					resultSet.close();
					close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}

	/**
	 * 查询最大的测试日期
	 * @param tablename
	 * @param basename
	 * @return
	 * @return String
	 */
	public String getMaxMonth(String tablename, String basename) {
		String sql = "select max(yearmonth) max  from " + tablename + "";
		System.out.println(sql + " :::数据查询sql");// where year='"+year+"'
		ResultSet rs = null;
		String url = dsturl.substring(0, dsturl.lastIndexOf("/") + 1) + basename;
		String maxmonth = "";
		try {
			start(url, dstuser, dstpassword);

			rs = statement.executeQuery(sql);
			while (rs.next()) {
				maxmonth = rs.getString("max");

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
		return maxmonth;
	}

	/**
	 * 根据类型及上下级的地区位置查询测试次数前9的终端或按大小排序的日期或测试时段
	 * @param tablename
	 * @param basename
	 * @param uparea
	 * @param area
	 * @param inteval
	 * @return
	 * @return Map
	 */
	public Map getTerMap(String tablename, String basename, String uparea, String area, String inteval) {
		String sql = "";
		if (inteval.equals("terminal")) {
			sql = "select terminal test  from " + tablename + " where uparea = '" + uparea + "' and area = '" + area + "' GROUP BY terminal ORDER BY SUM(testtimes) desc  limit 0,9";
		} else {
			sql = "select " + inteval + " test  from " + tablename + " where uparea = '" + uparea + "' and area='" + area + "' group by " + inteval + " order by " + inteval + "";
		}
		 System.out.println(sql + " :::数据查询sql");// where year='"+year+"'
		ResultSet rs = null;
		// Map<String, String> map = new HashMap<String, String>();
		String url = dsturl.substring(0, dsturl.lastIndexOf("/") + 1) + basename;
		String ters = "";
		try {
			try {
				start(url, dstuser, dstpassword);
			} catch (MySQLSyntaxErrorException e) {
				
				start(url, dstuser, dstpassword);
			}catch (Exception e) {
				// TODO: handle exception
				start(url, dstuser, dstpassword);
			}

			rs = statement.executeQuery(sql);
			while (rs.next()) {
				String terminal = rs.getString("test");
				ters += "\"" + terminal + "\"&";
				
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
		DataStaticJsonData.uparmap.put(uparea + "," + area, ters);
		return DataStaticJsonData.uparmap;
	}
}
