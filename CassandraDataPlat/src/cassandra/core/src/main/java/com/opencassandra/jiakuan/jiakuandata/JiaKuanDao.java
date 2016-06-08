package com.opencassandra.jiakuan.jiakuandata;

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

public class JiaKuanDao {

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
	 * 获取overview_kpi表 数据
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
	public List<Map> getDataKpi(String dataSourceTable, String dataSourcebasename, String tarTableName, String month) {

		List<Map> listMap = new ArrayList<Map>();
		String sql = " SELECT case probetype when 'ios' then 'iOS' when 'android' then 'Android' else 'PC' end probetype , month,groupname province,ifnull(sum(terminal_num),0) terminal_num,ifnull(SUM(new_sample_num),0) new_sample_num ,ifnull(sum(new_user_num),0) new_user_num,ifnull(sum(newly_increase_num),0) newly_increase_num,count(DISTINCT org_id,broadband_type) org_num,b.groupid groupid,a.accumulativ_num accumulativ_num,a.regusername_num regusername_num "
				+ ",a.customers_num customers_num FROM "
				+ dataSourceTable
				+ " b,(SELECT	groupid ,ifnull(sum(new_user_num),0) accumulativ_num,ifnull(sum(new_regusername_num),0) regusername_num,ifnull(sum(new_customers_num),0) customers_num "
				+ "  FROM "
				+ dataSourceTable + "  WHERE MONTH <= '" + month + "' GROUP BY groupid ) a WHERE MONTH = '" + month + "' and a.groupid=b.groupid GROUP BY groupid";

		ResultSet rs = null;
		String url = srcurl + dataSourcebasename;

		System.out.println(sql);
		try {
			start(url, srcuser, srcpassword);

			rs = statement.executeQuery(sql);
			while (rs.next()) {
				Map map = new HashMap();

				String groupid = rs.getString("groupid");
				String province = rs.getString("province");
				String new_user_num = rs.getString("new_user_num");

				String accumulativ_num = rs.getString("accumulativ_num");
				String org_num = rs.getString("org_num");
				String terminal_num = rs.getString("terminal_num");
				String regusername_num = rs.getString("regusername_num");
				String customers_num = rs.getString("customers_num");
				String newly_increase_num = rs.getString("newly_increase_num");
				String new_sample_num = rs.getString("new_sample_num");
				String probetype = rs.getString("probetype");
				
				
				map.put("org_num", org_num);
				map.put("probetype", probetype);
				map.put("accumulativ_num", accumulativ_num);
				map.put("terminal_num", terminal_num);
				map.put("regusername_num", regusername_num);
				map.put("customers_num", customers_num);
				map.put("newly_increase_num", newly_increase_num);
				map.put("new_sample_num", new_sample_num);
				map.put("groupid", groupid);
				map.put("province", province);
				map.put("new_user_num", new_user_num);

				map.put("month", month);
				listMap.add(map);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return listMap;
	}
	/**
	 * 获取network_operator表 数据
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
	public List<Map> getDataOperator(String dataSourceTable, String dataSourcebasename, String tarTableName, String month) {
		
		List<Map> listMap = new ArrayList<Map>();
		String sql = " SELECT case probetype when 'ios' then 'iOS' when 'android' then 'Android' else 'PC' end probetype , month,groupname province,ifnull(sum(new_user_num),0) new_user_num,ifnull(sum(newly_increase_num),0) newly_increase_num,b.groupid groupid,a.accumulativ_num accumulativ_num,a.operator operator "
				+ " FROM "
				+ dataSourceTable
				+ " b,(SELECT	groupid ,ifnull(sum(new_user_num),0) accumulativ_num ,operator "
				+ "  FROM "
				+ dataSourceTable + "  WHERE MONTH <= '" + month + "' GROUP BY groupid,operator ) a WHERE MONTH = '" + month + "' and a.groupid=b.groupid and a.operator = b.operator  GROUP BY groupid,operator";
		
		ResultSet rs = null;
		String url = srcurl + dataSourcebasename;
		
		System.out.println(sql);
		try {
			start(url, srcuser, srcpassword);
			
			rs = statement.executeQuery(sql);
			while (rs.next()) {
				Map map = new HashMap();
				
				String groupid = rs.getString("groupid");
				String province = rs.getString("province");
				String new_user_num = rs.getString("new_user_num");
				
				String accumulativ_num = rs.getString("accumulativ_num");
				String newly_increase_num = rs.getString("newly_increase_num");
				String operator = rs.getString("operator");
				String probetype = rs.getString("probetype");
				map.put("probetype", probetype);
				map.put("accumulativ_num", accumulativ_num);
				map.put("groupid", groupid);
				map.put("province", province);
				map.put("new_user_num", new_user_num);
				map.put("newly_increase_num", newly_increase_num);
				map.put("operator", operator);
				
				map.put("month", month);
				listMap.add(map);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return listMap;
	}
	/**
	 * 获取network_type表 数据
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
	public List<Map> getDataNetType(String dataSourceTable, String dataSourcebasename, String tarTableName, String month) {
		
		List<Map> listMap = new ArrayList<Map>();
		String sql = " SELECT case probetype when 'ios' then 'iOS' when 'android' then 'Android' else 'PC' end probetype , month,groupname province,ifnull(sum(new_user_num),0) new_user_num,ifnull(sum(newly_increase_num),0) newly_increase_num,b.groupid groupid,a.accumulativ_num accumulativ_num,b.net_type "
				+ " FROM "
				+ dataSourceTable
				+ " b,(SELECT	groupid ,ifnull(sum(new_user_num),0) accumulativ_num ,net_type "
				+ "  FROM "
				+ dataSourceTable + "  WHERE MONTH <= '" + month + "' GROUP BY groupid,net_type ) a WHERE MONTH = '" + month + "' and a.groupid=b.groupid and a.net_type= b.net_type GROUP BY groupid ,net_type";
		
		ResultSet rs = null;
		String url = srcurl + dataSourcebasename;
		
		System.out.println(sql);
		try {
			start(url, srcuser, srcpassword);
			
			rs = statement.executeQuery(sql);
			while (rs.next()) {
				Map map = new HashMap();
				
				String groupid = rs.getString("groupid");
				String province = rs.getString("province");
				String new_user_num = rs.getString("new_user_num");
				String net_type = rs.getString("net_type");
				
				String accumulativ_num = rs.getString("accumulativ_num");
				String newly_increase_num = rs.getString("newly_increase_num");
				String probetype = rs.getString("probetype");
				map.put("probetype", probetype);
				map.put("accumulativ_num", accumulativ_num);
				map.put("groupid", groupid);
				map.put("province", province);
				map.put("new_user_num", new_user_num);
				map.put("net_type", net_type);
				map.put("newly_increase_num", newly_increase_num);
				
				map.put("month", month);
				listMap.add(map);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return listMap;
	}
	/**
	 * 获取terminal_os表 数据
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
	public List<Map> getDataTerminalOs(String dataSourceTable, String dataSourcebasename, String tarTableName, String month) {
		
		List<Map> listMap = new ArrayList<Map>();
		String sql = " SELECT case probetype when 'ios' then 'iOS' when 'android' then 'Android' else 'PC' end probetype , month,groupname province,ifnull(sum(new_user_num),0) new_user_num,ifnull(sum(newly_increase_num),0) testtimes,ifnull(sum(terminal_num),0) new_terminal_num , b.groupid groupid,a.accumulativ_num accumulativ_num ,a.accumulativ_terminal_num accumulativ_terminal_num ,case b.platform when 'ios' then 'iOS' when 'android' then 'Android' else 'PC' end platform"
				+ " FROM "
				+ dataSourceTable
				+ " b,(SELECT	groupid ,ifnull(sum(new_user_num),0) accumulativ_num ,ifnull(sum(terminal_num),0) accumulativ_terminal_num,platform"
				+ "  FROM "
				+ dataSourceTable + "  WHERE MONTH <= '" + month + "' GROUP BY groupid,platform ) a WHERE MONTH = '" + month + "' and a.groupid=b.groupid and a.platform= b.platform GROUP BY groupid ,platform";
		
		ResultSet rs = null;
		String url = srcurl + dataSourcebasename;
		
		System.out.println(sql);
		try {
			start(url, srcuser, srcpassword);
			
			rs = statement.executeQuery(sql);
			while (rs.next()) {
				Map map = new HashMap();
				
				String groupid = rs.getString("groupid");
				String province = rs.getString("province");
				String new_user_num = rs.getString("new_user_num");
				String accumulativ_terminal_num = rs.getString("accumulativ_terminal_num");
				String new_terminal_num = rs.getString("new_terminal_num");
				
				String accumulativ_num = rs.getString("accumulativ_num");
				String testtimes = rs.getString("testtimes");
				String platform = rs.getString("platform");
				map.put("accumulativ_num", accumulativ_num);
				String probetype = rs.getString("probetype");
				map.put("probetype", probetype);
				map.put("groupid", groupid);
				map.put("province", province);
				map.put("new_user_num", new_user_num);
				map.put("accumulativ_terminal_num", accumulativ_terminal_num);
				map.put("platform", platform);
				map.put("testtimes", testtimes);
				map.put("new_terminal_num", new_terminal_num);
				
				map.put("month", month);
				listMap.add(map);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return listMap;
	}
	/**
	 * 获取terminal_model表 数据
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
	public List<Map> getDataTerminalModel(String dataSourceTable, String dataSourcebasename, String tarTableName, String month) {
		
		List<Map> listMap = new ArrayList<Map>();
		String sql = " SELECT case probetype when 'ios' then 'iOS' when 'android' then 'Android' else 'PC' end probetype , month,groupname province, groupid , ifnull(sum(newly_increase_num),0) testtimes ,terminal_model,terminal_type "
				+ " FROM "
				+ dataSourceTable
				+ " WHERE MONTH = '" + month + "'  GROUP BY groupid ,terminal_model";
		
		ResultSet rs = null;
		String url = srcurl + dataSourcebasename;
		
		System.out.println(sql);
		try {
			start(url, srcuser, srcpassword);
			
			rs = statement.executeQuery(sql);
			while (rs.next()) {
				Map map = new HashMap();
				
				String groupid = rs.getString("groupid");
				String province = rs.getString("province");
				String testtimes = rs.getString("testtimes");
				
				String terminal_model = rs.getString("terminal_model");
				String terminal_type = rs.getString("terminal_type");
				map.put("terminal_type", terminal_type);
				String probetype = rs.getString("probetype");
				map.put("probetype", probetype);
				map.put("groupid", groupid);
				map.put("province", province);
				map.put("terminal_model", terminal_model);
				map.put("testtimes", testtimes);
				
				map.put("month", month);
				listMap.add(map);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return listMap;
	}

	/**
	 * 获取overview_province表 数据
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
	public List<Map> getDataProvince(String dataSourceTable, String dataSourcebasename, String tarTableName, String month) {

		List<Map> listMap = new ArrayList<Map>();
		String sql = " SELECT case probetype when 'ios' then 'iOS' when 'android' then 'Android' else 'PC' end probetype , month,b.groupid groupid,groupname province,a.accumulativ_num accumulativ_num,ifnull(sum(new_user_num),0) new_user_num ,ifnull(sum(newly_increase_num),0) testtimes ,b.broadband_type,sum(b.terminal_num) terminal_num ,sum(b.new_sample_num) new_sample_num FROM "
				+ dataSourceTable + " b , " +
						"(SELECT	groupid ,ifnull(sum(new_user_num),0) accumulativ_num ,broadband_type"
				+ "  FROM "
				+ dataSourceTable + "  WHERE MONTH <= '" + month + "' GROUP BY groupid,broadband_type ) a WHERE MONTH = '" + month + "' and a.groupid=b.groupid and a.broadband_type= b.broadband_type GROUP BY groupid ,broadband_type" ;
		ResultSet rs = null;
		String url = srcurl + dataSourcebasename;

		System.out.println(sql);
		try {
			start(url, srcuser, srcpassword);

			rs = statement.executeQuery(sql);
			while (rs.next()) {
				Map map = new HashMap();

				String groupid = rs.getString("groupid");
				String province = rs.getString("province");
				String new_user_num = rs.getString("new_user_num");
				String accumulativ_num = rs.getString("accumulativ_num");
				String new_sample_num = rs.getString("new_sample_num");
				String terminal_num = rs.getString("terminal_num");
				String probetype = rs.getString("probetype");

				map.put("groupid", groupid);
				map.put("province", province);
				map.put("new_user_num", new_user_num);
				map.put("probetype", probetype);

				map.put("month", month);
				map.put("accumulativ_num", accumulativ_num);
				map.put("new_sample_num", new_sample_num);
				map.put("terminal_num", terminal_num);

				String broadband_type = rs.getString("broadband_type");
				String testtimes = rs.getString("testtimes");
				map.put("broadband_type", broadband_type);
				map.put("testtimes", testtimes);
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

		String url = dsturl + dstBaseName;
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
	public void createTable(String dsttableName, String dstBaseName) {

		String sql = "";
		if (dsttableName.endsWith("terminal")) {

			sql = "CREATE TABLE "
					+ dsttableName
					+ ""
					+ "(`id`  int(11) NOT NULL AUTO_INCREMENT ,`month`  varchar(100)  DEFAULT NULL ,`org_id`  varchar(200)  DEFAULT NULL ,`groupid`  varchar(200)  DEFAULT NULL ,`groupname`  varchar(200)  DEFAULT NULL ,"
					+ "`new_user_num`  int(11)  DEFAULT NULL ,`terminal_num`  int(11)  DEFAULT NULL ,`newly_increase_num`  int(11)  DEFAULT NULL ,"
					+ "`platform`  varchar(200)  DEFAULT NULL ,`terminal_model` VARCHAR (255)  DEFAULT NULL ,`terminal_type`  varchar(100)  DEFAULT NULL ,"
					+ "PRIMARY KEY (id)) CHARSET=utf8 ROW_FORMAT=COMPACT;";

		} else if (dsttableName.endsWith("bandwidth")) {
			sql = "CREATE TABLE " + dsttableName + ""
					+ "(`id`  int(11) NOT NULL AUTO_INCREMENT ,`org_id`  varchar(200)  DEFAULT NULL ,`groupid`  varchar(200)  DEFAULT NULL ,`groupname`  varchar(200)  DEFAULT NULL ,"
					+ "`new_user_num`  int(11)  DEFAULT NULL  ,`newly_increase_num`  int(11)  DEFAULT NULL ," + "`broadband_type`  varchar(200)  DEFAULT NULL ,`month`  varchar(100)  DEFAULT NULL ,"
					+ "PRIMARY KEY (id)) CHARSET=utf8 ROW_FORMAT=COMPACT;";
		} else if (dsttableName.endsWith("operator")) {

			sql = "CREATE TABLE "
					+ dsttableName
					+ ""
					+ "(`id`  int(11) NOT NULL AUTO_INCREMENT ,`month`  varchar(100)  DEFAULT NULL ,`org_id`  varchar(200)  DEFAULT NULL ,`groupid`  varchar(200)  DEFAULT NULL ,`groupname`  varchar(200)  DEFAULT NULL ,"
					+ "`new_user_num`  int(11)  DEFAULT NULL ,`newly_increase_num`  int(11)  DEFAULT NULL ,`operator`  varchar(200)  DEFAULT NULL ,`net_type`  varchar(255)  DEFAULT NULL ,"
					+ "PRIMARY KEY (id)) CHARSET=utf8 ROW_FORMAT=COMPACT;";
		}

		String url = dsturl + dstBaseName;
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
	public boolean queryDataExist(String dsttableName, String dstBaseName,String argument, String month) {

		String sql = "select count(id) count from " + dsttableName + " where month = '" + month + "' and probetype='"+argument+"'";
		int count = 0;

		String url = dsturl + dstBaseName;
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
	public void delMonthData(String dsttableName, String dstBaseName, String argument,String month) {

		String sql = "delete from " + dsttableName + " where month= '" + month + "' and probetype = '"+argument+"' ";
		String url = dsturl + dstBaseName;
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
		// System.out.println(sql.toString() + "  :::::拼装insertsql");
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
			String url = dsturl + dstBaseName;
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

	/**
	 * 查询上月份的数据在本月是否还存在
	 * 
	 * @param dsttableName
	 * @param dstBaseName
	 * @param month
	 * @return
	 * @return boolean
	 */
	public boolean queryLastMonthDataExist(String dsttableName, String dstBaseName,String latmonth, String month) {

		String sql = "INSERT into "+dsttableName+" (accumulativ_num,groupid,province,probetype ,month) (SELECT accumulativ_num,groupid,province, probetype,'"+month+"' FROM "+dsttableName+" WHERE `month` = '"+latmonth+"' AND groupid NOT IN (SELECT groupid FROM "+dsttableName+" WHERE `month` = '"+month+"'))";

		String url = dsturl + dstBaseName;
		System.out.println(sql + "::::::查询月份数据是否已存在sql");
		boolean flage = false;
		try {
			start(url, dstuser, dstpassword);
			statement.execute(sql);
			/*while (rs.next()) {
				count = rs.getInt("count");
				if (count > 0) {
					flage = true;
				}
			}*/
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

	
}
