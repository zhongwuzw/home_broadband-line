package com.opencassandra.jiakuan;

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
import com.opencassandra.service.utils.UtilDate;

public class JiaKuanMiddleDao {

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

	// JiaKuanMiddleDao jiaKuanMiddleDao = new JiaKuanMiddleDao();

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

	public void createTempTable(String dataSourceBaseName, String targetBaseName, String constantBaseName, String[] sourceTables) {

		String pjsql = "";
		String file_type = "";//AND cast(avg_rate AS signed) > 0
		String qualification = "";
		String bufferstring = "";
		for (int i = 0; i < sourceTables.length; i++) {
			String source = sourceTables[i];
			if(source.contains("http")){
				file_type = " and file_type ='download' ";
				qualification = " AND cast(avg_rate AS signed) > 0 ";
				bufferstring = "";
			}else if(source.contains("web")){
				qualification = " AND cast(ninety_loading AS signed) > 0 AND cast(success_rate AS signed) >= 0 AND cast(success_rate AS signed) <= 100 ";
				bufferstring = "";
			}else if(source.contains("video")){
				qualification= " AND cast(avg_delay AS signed) > 0 ";
				bufferstring = "  and buffer_proportion != '' ";
			}
			String groupidsql = "(SELECT t.org_key, m.groupid,	m.NAME	FROM " + constantBaseName + ".t_org t, ( SELECT j.org_id,j.groupid,i.name from " + constantBaseName
					+ ".t_group_org_mapping i , (SELECT org_id, groupid" + " 	FROM " + constantBaseName + ".t_group_org_mapping a, " + targetBaseName
					+ ".group_id b WHERE a.group_id = b.groupid ) j where j.groupid =i.parent_org_id  ) m WHERE t.id = m.org_id) n";

			String sql = "SELECT n.name,n.groupid,time, model, net_type, imei, android_ios, device_org, test_description,  bandwidth, operator_manual operator "
					+ "FROM " + source + " v , " + groupidsql + "  WHERE n.org_key = v.device_org "+file_type+" "+qualification+" "+bufferstring+" union ALL ";

			pjsql += sql;
		}

		System.out.println(pjsql);
		pjsql = "create table jiakuan_temp  " + pjsql.substring(0, pjsql.lastIndexOf("union ALL"));

		String url = srcurl + dataSourceBaseName;

		System.out.println(pjsql);
		try {
			start(url, srcuser, srcpassword);

			boolean flage = queryTableExist("jiakuan_temp", dataSourceBaseName);
			if (flage) {
				String dropsql = "DROP TABLE jiakuan_temp;";
				statement.execute(dropsql);
			}

			statement.execute(pjsql);
		} catch (Exception e) {
			e.printStackTrace();
		}

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
	public List<Map> getData(String dataSourceTable, String dataSourcebasename, String targetBaseName, String tarTableName, String argument, String month) {

		List<Map> listMap = new ArrayList<Map>();
		String sql = "";
		String nextMonth = UtilDate.getNextMonth(month);

		String sampleSql = "( SELECT count(imei) new_sample_num, p.device_org, p.bandwidth FROM " + dataSourceTable + " p WHERE time >= UNIX_TIMESTAMP('" + month
				+ "01') * 1000 AND time < UNIX_TIMESTAMP('" + nextMonth + "01') * 1000 " + "and p.android_ios ='" + argument
				+ "' GROUP BY p.device_org, p.bandwidth ) q";
		if (tarTableName.contains("terminal")) {
			sql = "select groupid groupid ,name groupname, '"
					+ month
					+ "' month,device_org org_id,COUNT(DISTINCT imei,model) AS new_user_num,COUNT(DISTINCT imei,model) AS new_regusername_num ,COUNT(DISTINCT imei,model) AS new_customers_num,COUNT(DISTINCT imei ,model) terminal_num,COUNT(imei) newly_increase_num,'"
					+ argument + "' platform,model terminal_model,'" + argument + "'  android_ios , '" + argument + "' termianl_type " + " from " + dataSourceTable
					+ " p  where p.bandwidth != ''and  imei != '' AND model != ''  AND operator != ''  AND time >= UNIX_TIMESTAMP('" + month + "01') * 1000 AND time < UNIX_TIMESTAMP('" + nextMonth
					+ "01') * 1000  AND UPPER(net_type) = 'WIFI'  and android_ios='" + argument + "'   GROUP BY device_org,model";

		} else if (tarTableName.contains("bandwidth")) {

			sql = "SELECT p.groupid groupid, p. NAME groupname, q.new_sample_num new_sample_num, '" + month
					+ "' MONTH, p.device_org org_id, COUNT(DISTINCT imei, p.bandwidth) AS new_user_num, COUNT(DISTINCT imei, p.bandwidth) AS new_regusername_num, '" + argument + "'  android_ios ,"
					+ "	COUNT(DISTINCT imei, p.bandwidth) AS new_customers_num, COUNT(DISTINCT imei) terminal_num, COUNT(imei) newly_increase_num, p.bandwidth, p.test_description, '" + argument
					+ "'android_ios" + " FROM " + dataSourceTable + " p, " + sampleSql + " "
					+ "WHERE p.bandwidth != '' AND imei != ''  AND model != ''  AND operator != ''  and UPPER(net_type) = 'WIFI' AND time >= UNIX_TIMESTAMP('" + month + "01') * 1000 AND time < UNIX_TIMESTAMP('" + nextMonth
					+ "01') * 1000  AND p.android_ios ='" + argument
					+ "' AND p.device_org = q.device_org AND p.bandwidth = q.bandwidth GROUP BY p.device_org, p.bandwidth";

		} else if (tarTableName.contains("operator")) {
			sql = "select groupid groupid ,name groupname, '"
					+ month
					+ "' month,device_org org_id,COUNT(DISTINCT imei,operator) AS new_user_num,COUNT(DISTINCT imei,operator) AS new_regusername_num ,COUNT(DISTINCT imei,operator) AS new_customers_num,COUNT(DISTINCT imei) terminal_num,COUNT(imei) newly_increase_num, '"
					+ argument + "'  android_ios ,operator  from " + dataSourceTable + " p   where 	imei != '' and  p.bandwidth != '' AND operator != '' and model !='' AND time >= UNIX_TIMESTAMP('" + month
					+ "01') * 1000 AND time < UNIX_TIMESTAMP('" + nextMonth + "01') * 1000  AND UPPER(net_type) = 'WIFI'  and android_ios='" + argument
					+ "' GROUP BY device_org,operator";
		}

		ResultSet rs = null;
		String url = srcurl + dataSourcebasename;

		System.out.println(sql);
		try {
			start(url, dstuser, dstpassword);

			rs = statement.executeQuery(sql);
			while (rs.next()) {
				Map map = new HashMap();

				if (tarTableName.contains("terminal")) {
					String platform = rs.getString("platform");
					String termianl_type = rs.getString("termianl_type");
					String terminal_model = rs.getString("terminal_model");
					map.put("platform", platform);
					map.put("terminal_type", termianl_type);
					map.put("terminal_model", terminal_model);
				} else if (tarTableName.contains("bandwidth")) {
					String bandwidth = rs.getString("bandwidth");
					// String mac = rs.getString("mac");
					String new_sample_num = rs.getString("new_sample_num");
					map.put("broadband_type", bandwidth);
					map.put("new_sample_num", new_sample_num);
					// map.put("mac", mac);
				} else if (tarTableName.contains("operator")) {
					String operator = rs.getString("operator");
					// String net_type = rs.getString("net_type");
					map.put("operator", operator);
					// map.put("net_type", net_type);
				}

				String groupid = rs.getString("groupid");
				String groupname = rs.getString("groupname");
				String org_id = rs.getString("org_id");
				String new_user_num = rs.getString("new_user_num");
				String new_regusername_num = rs.getString("new_regusername_num");
				String new_customers_num = rs.getString("new_customers_num");
				String newly_increase_num = rs.getString("newly_increase_num");
				String terminal_num = rs.getString("terminal_num");
				String android_ios = rs.getString("android_ios");

				map.put("terminal_num", terminal_num);
				map.put("groupid", groupid);
				map.put("groupname", groupname);
				map.put("org_id", org_id);
				map.put("new_user_num", new_user_num);
				map.put("new_regusername_num", new_regusername_num);
				map.put("new_customers_num", new_customers_num);

				map.put("newly_increase_num", newly_increase_num);
				map.put("probetype", android_ios);

				map.put("month", month);

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

		if(dsttableName.contains("temp")){
			String url = srcurl + dstBaseName;
			try {
				this.close();
				start(url, srcuser, srcpassword);
			} catch (Exception e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}

		}else{
			String url = dsturl + dstBaseName;
			try {
				this.close();
				start(url, dstuser, dstpassword);
			} catch (Exception e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			
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
		if (dsttableName.contains("terminal")) {

			sql = "CREATE TABLE "
					+ dsttableName
					+ ""
					+ "(`id`  int(11) NOT NULL AUTO_INCREMENT ,`month`  varchar(100)  DEFAULT NULL ,`org_id`  varchar(200)  DEFAULT NULL ,`groupid`  varchar(200)  DEFAULT NULL ,`groupname`  varchar(200)  DEFAULT NULL ,"
					+ "`new_user_num`  int(11)  DEFAULT NULL ,`terminal_num`  int(11)  DEFAULT NULL ,`newly_increase_num`  int(11)  DEFAULT NULL ,"
					+ "`platform`  varchar(200)  DEFAULT NULL ,`terminal_model` VARCHAR (255)  DEFAULT NULL ,`terminal_type`  varchar(100)  DEFAULT NULL ,`new_regusername_num`  int(11)  DEFAULT NULL ,`new_customers_num`  int(11)  DEFAULT NULL , `probetype`  varchar(100)  DEFAULT NULL ,"
					+ "PRIMARY KEY (id)) CHARSET=utf8 ROW_FORMAT=COMPACT;";

		} else if (dsttableName.contains("bandwidth")) {
			sql = "CREATE TABLE "
					+ dsttableName
					+ ""
					+ "(`id`  int(11) NOT NULL AUTO_INCREMENT ,`org_id`  varchar(200)  DEFAULT NULL ,`groupid`  varchar(200)  DEFAULT NULL ,`groupname`  varchar(200)  DEFAULT NULL ,"
					+ "`new_user_num`  int(11)  DEFAULT NULL  ,`newly_increase_num`  int(11)  DEFAULT NULL ,"
					+ "`broadband_type`  varchar(200)  DEFAULT NULL ,`month`  varchar(100)  DEFAULT NULL ,`new_regusername_num`  int(11)  DEFAULT NULL ,`new_customers_num`  int(11)  DEFAULT NULL ,`terminal_num`  int(11)  DEFAULT NULL ,`new_sample_num`  int(11)  DEFAULT NULL ,`case_num`  int(11)  DEFAULT NULL , `probetype`  varchar(100)  DEFAULT NULL ,"
					+ "PRIMARY KEY (id)) CHARSET=utf8 ROW_FORMAT=COMPACT;";
		} else if (dsttableName.contains("operator")) {

			sql = "CREATE TABLE "
					+ dsttableName
					+ ""
					+ "(`id`  int(11) NOT NULL AUTO_INCREMENT ,`month`  varchar(100)  DEFAULT NULL ,`org_id`  varchar(200)  DEFAULT NULL ,`groupid`  varchar(200)  DEFAULT NULL ,`groupname`  varchar(200)  DEFAULT NULL ,"
					+ "`new_user_num`  int(11)  DEFAULT NULL ,`newly_increase_num`  int(11)  DEFAULT NULL ,`operator`  varchar(200)  DEFAULT NULL ,`net_type`  varchar(255)  DEFAULT NULL ,`new_regusername_num`  int(11)  DEFAULT NULL ,`new_customers_num`  int(11)  DEFAULT NULL ,`terminal_num`  int(11)  DEFAULT NULL , `probetype`  varchar(100)  DEFAULT NULL ,"
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
	public boolean queryDataExist(String dsttableName, String dstBaseName, String month) {

		String sql = "select count(id) count from " + dsttableName + " where month = '" + month + "'";
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
	public void delMonthData(String dsttableName, String dstBaseName, String month) {

		String sql = "delete from " + dsttableName + " where month= '" + month + "' ";
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

}
