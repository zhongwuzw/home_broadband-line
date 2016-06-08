package com.opencassandra.jiakuan.jiakuan_servicequality.dao;


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

public class ServicequalityGroupidHttpdownloadDao {

	Statement statement;
	Connection conn;
	// 数据源数据库的地�?
	private static String srcurl = ConfParser.srcurl;
	private static String srcuser = ConfParser.srcuser;
	private static String srcpassword = ConfParser.srcpassword;

	// 数据入库的地�?���?
	private static String dsturl = ConfParser.dsturl;
	private static String dstuser = ConfParser.dstuser;
	private static String dstpassword = ConfParser.dstpassword;
	
	public static List<Map> groupList = new ArrayList<Map>();

	// -----------------------------------------------------数据库连接相关操�?---------------------------------------------------------
	/**
	 * 建立数据库连�?
	 * 
	 * @return void
	 */
	public void start(String url, String user, String password) throws Exception {
		System.out.println(url + " :数据库连�? " + user + " :: " + password);
		this.close();
		String driver = "com.mysql.jdbc.Driver";
		Class.forName(driver);
		conn = DriverManager.getConnection(url, user, password);
		statement = conn.createStatement();
	}

	/**
	 * 关闭数据库连�?
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
	 * 创建数据�?
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
	public List<Map> getData(String dataSourceTable, String dataSourcebasename, String tarTableName,String month,String targetBaseName) {

		List<Map> listMap = new ArrayList<Map>();
		
		String sql = "";
		//String wherequery = "where " + area + " !='' and " + area + "!='-' ";
		
		String nextMonth = UtilDate.getNextMonth(month);
		String groupidsql = "(SELECT t.org_key, m.groupid,	m. NAME	FROM t_org t, ( SELECT j.org_id,j.groupid,i.name from t_group_org_mapping i , (SELECT org_id, groupid" +
				" 	FROM t_group_org_mapping a, "+targetBaseName+".group_id b WHERE a.group_id = b.groupid ) j where j.groupid =i.parent_org_id  ) m WHERE t.id = m.org_id) n";
		
		sql = "select n.groupid groupid ,n.name groupname, '"+month+"' month,p.device_org org_id,COUNT(id) AS ping_test_times,round(avg(avg_delay),2)  ping_avg_delay,round(avg(loss_rate),2) ping_loss_rate,min(avg_delay) best_ping_delay ,round(min(loss_rate),2) best_ping_loss_rate,p.bandwidth broadband_type" +
				" from "+dataSourceTable+" p , "+groupidsql+"   where 	consumerid != '' AND businessid != '' AND p.bandwidth != '' AND mac != '' AND start_time >= UNIX_TIMESTAMP('"+month+"01') * 1000 AND start_time < UNIX_TIMESTAMP('"+nextMonth+"01') * 1000 and cast(avg_delay AS signed) >0 and cast(loss_rate AS signed) >0  and p.device_org = n.org_key  GROUP BY n.groupid,p.bandwidth"; 
		

		ResultSet rs = null;
		String url = srcurl + dataSourcebasename;

		System.out.println(sql);
		try {
			start(url, dstuser, dstpassword);

			rs = statement.executeQuery(sql);
			while (rs.next()) {
				Map map = new HashMap();
				Map groupmap = new HashMap();

				
				String groupid = rs.getString("groupid");
				String groupname = rs.getString("groupname");
				String ping_test_times = rs.getString("ping_test_times");
				String ping_avg_delay = rs.getString("ping_avg_delay");
				String ping_loss_rate = rs.getString("ping_loss_rate");
				String best_ping_delay = rs.getString("best_ping_delay");
				String best_ping_loss_rate = rs.getString("best_ping_loss_rate");
				String broadband_type = rs.getString("broadband_type");
				
				map.put("groupid", groupid);
				map.put("groupname", groupname);
				map.put("ping_test_times", ping_test_times);
				map.put("ping_avg_delay", ping_avg_delay);
				map.put("ping_loss_rate", ping_loss_rate);
				map.put("best_ping_delay", best_ping_delay);
				
				map.put("best_ping_loss_rate", best_ping_loss_rate);
				map.put("month", month);
				map.put("broadband_type", broadband_type);
				
				
				groupmap.put("groupid", groupid);
				groupmap.put("broadband_type", broadband_type);
				groupmap.put("percentnum", ping_test_times);

				listMap.add(map);
				
				groupList.add(groupmap);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return listMap;
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
	public String getPercentData(String dataSourceTable, String dataSourcebasename,String month,String groupid,String bandwidth,int percentnum,String sourceField,String tarField,String targetBaseName) {
		
		List<Map> listMap = new ArrayList<Map>();
		
		String sql = "";
		//String wherequery = "where " + area + " !='' and " + area + "!='-' ";
		
		String nextMonth = UtilDate.getNextMonth(month);
		String groupidsql = "(SELECT t.org_key, m.groupid,	m. NAME	FROM t_org t, ( SELECT j.org_id,j.groupid,i.name from t_group_org_mapping i , (SELECT org_id, groupid" +
				" 	FROM t_group_org_mapping a, "+targetBaseName+".group_id b WHERE a.group_id = b.groupid ) j where j.groupid =i.parent_org_id  ) m WHERE t.id = m.org_id) n";
		
		sql = "select round("+sourceField+",2)  "+tarField+"" +
				" from "+dataSourceTable+" p , "+groupidsql+"   where 	consumerid != '' AND businessid != '' AND p.bandwidth != '' AND mac != '' AND start_time >= UNIX_TIMESTAMP('"+month+"01') * 1000 AND start_time < UNIX_TIMESTAMP('"+nextMonth+"01') * 1000 and cast(avg_delay AS signed) >0 and cast(loss_rate AS signed) >0  and p.device_org = n.org_key and n.groupid='"+groupid+"' and p.bandwidth = '"+bandwidth+"'  ORDER BY ABS("+sourceField+")  limit "+percentnum+" ,1 "; 
		
		
		/*sql = "select avg_delay top95_ping_delay from "+dataSourceTable+"  where 	consumerid != '' AND businessid != '' AND p.bandwidth != '' AND mac != '' AND start_time > UNIX_TIMESTAMP('"+month+"01') * 1000 AND end_time < UNIX_TIMESTAMP('"+nextMonth+"01') * 1000 and avg_delay>0  and groupid = "+groupid+" and bandwidth = "+bandwidth+" ORDER BY avg_delay limit "+percentnum+",1 ";
		*/
		ResultSet rs = null;
		String url = srcurl + dataSourcebasename;
		
		String data = "";
		
		System.out.println(sql);
		try {
			start(url, dstuser, dstpassword);
			
			rs = statement.executeQuery(sql);
			while (rs.next()) {
				data = rs.getString(tarField);
				
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return data;
	}

	/**
	 * 查看数据入库表是否存�?
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
		if(dsttableName.endsWith("terminal")){
			
			sql = "CREATE TABLE " + dsttableName + "" +
					"(`id`  int(11) NOT NULL AUTO_INCREMENT ,`month`  varchar(100)  DEFAULT NULL ,`org_id`  varchar(200)  DEFAULT NULL ,`groupid`  varchar(200)  DEFAULT NULL ,`groupname`  varchar(200)  DEFAULT NULL ," +
					"`new_user_num`  int(11)  DEFAULT NULL ,`terminal_num`  int(11)  DEFAULT NULL ,`newly_increase_num`  int(11)  DEFAULT NULL ," +
					"`platform`  varchar(200)  DEFAULT NULL ,`terminal_model` VARCHAR (255)  DEFAULT NULL ,`terminal_type`  varchar(100)  DEFAULT NULL ,`new_regusername_num`  int(11)  DEFAULT NULL ,`new_customers_num`  int(11)  DEFAULT NULL ," +
					"PRIMARY KEY (id)) CHARSET=utf8 ROW_FORMAT=COMPACT;";
			
		}else if(dsttableName.endsWith("bandwidth")){
			 sql = "CREATE TABLE " + dsttableName + "" +
					"(`id`  int(11) NOT NULL AUTO_INCREMENT ,`org_id`  varchar(200)  DEFAULT NULL ,`groupid`  varchar(200)  DEFAULT NULL ,`groupname`  varchar(200)  DEFAULT NULL ," +
					"`new_user_num`  int(11)  DEFAULT NULL  ,`newly_increase_num`  int(11)  DEFAULT NULL ," +
					"`broadband_type`  varchar(200)  DEFAULT NULL ,`month`  varchar(100)  DEFAULT NULL ,`new_regusername_num`  int(11)  DEFAULT NULL ,`new_customers_num`  int(11)  DEFAULT NULL ,`terminal_num`  int(11)  DEFAULT NULL ,`new_sample_num`  int(11)  DEFAULT NULL ," +
					"PRIMARY KEY (id)) CHARSET=utf8 ROW_FORMAT=COMPACT;";
		}else if (dsttableName.endsWith("operator")){
			
			sql = "CREATE TABLE " + dsttableName + "" +
					"(`id`  int(11) NOT NULL AUTO_INCREMENT ,`month`  varchar(100)  DEFAULT NULL ,`org_id`  varchar(200)  DEFAULT NULL ,`groupid`  varchar(200)  DEFAULT NULL ,`groupname`  varchar(200)  DEFAULT NULL ," +
					"`new_user_num`  int(11)  DEFAULT NULL ,`newly_increase_num`  int(11)  DEFAULT NULL ,`operator`  varchar(200)  DEFAULT NULL ,`net_type`  varchar(255)  DEFAULT NULL ,`new_regusername_num`  int(11)  DEFAULT NULL ,`new_customers_num`  int(11)  DEFAULT NULL ,`terminal_num`  int(11)  DEFAULT NULL ," +
					"PRIMARY KEY (id)) CHARSET=utf8 ROW_FORMAT=COMPACT;";
		}
		
		String url = dsturl+ dstBaseName;
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
	 * 查询该月份的数据是否已存�?
	 * 
	 * @param dsttableName
	 * @param dstBaseName
	 * @param month
	 * @return
	 * @return boolean
	 */
	public boolean queryDataExist(String dsttableName, String dstBaseName, String month,String probetype) {
		String consql = "";
		if("pc".equals(probetype)){
			consql = "and probetype='"+probetype+"'";
		}else if ("app".equals(probetype)){
			consql = "and (probetype='ios' or probetype='android')";
		}

		String sql = "select count(id) count from " + dsttableName + " where month = '" + month + "' "+consql+"";
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
	public void delMonthData(String dsttableName, String dstBaseName, String month,String probetype) {

		String consql = "";
		if("pc".equals(probetype)){
			consql = "and probetype='"+probetype+"'";
		}else if ("app".equals(probetype)){
			consql = "and (probetype='ios' or probetype='android')";
		}
		
		String sql = "delete from " + dsttableName + " where month= '" + month + "' "+consql+"";
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
	 * 根据探针类型对数据进行分类处理
	 * @param dataSourceTable
	 * @param dataSourcebasename
	 * @param tarTableName
	 * @param month
	 * @param targetBaseName
	 * @param probetype
	 * @return
	 */
	public List<Map> getDataByprobetype(String dataSourceTable, String dataSourcebasename, String tarTableName,String month,String targetBaseName,String probetype,String authBaseName) {
		List<Map> listMap = new ArrayList<Map>();
		
		if("pc".equals(probetype)){
			listMap = getDataPc(dataSourceTable, dataSourcebasename, tarTableName, month, targetBaseName,probetype,authBaseName);
		}else if("app".equals(probetype)){
			listMap = getDataApp(dataSourceTable, dataSourcebasename, tarTableName, month, targetBaseName,probetype,authBaseName);
		}else if("".equals(probetype)){
			
		}
		return listMap;
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
	public List<Map> getDataPc(String dataSourceTable, String dataSourcebasename, String tarTableName,String month,String targetBaseName,String probetype,String authBaseName) {

		List<Map> listMap = new ArrayList<Map>();
		
		String sql = "";
		//String wherequery = "where " + area + " !='' and " + area + "!='-' ";
		
		String nextMonth = UtilDate.getNextMonth(month);
		String groupidsql = "(SELECT t.org_key, m.groupid,	m. NAME	FROM "+authBaseName+".t_org t, ( SELECT j.org_id,j.groupid,i.name from "+authBaseName+".t_group_org_mapping i , (SELECT org_id, groupid" +
				" 	FROM "+authBaseName+".t_group_org_mapping a, "+targetBaseName+".group_id b WHERE a.group_id = b.groupid ) j where j.groupid =i.parent_org_id  ) m WHERE t.id = m.org_id) n";
		
		sql = "select n.groupid groupid ,n.name groupname, '"+month+"' month,p.device_org org_id,COUNT(id) AS download_test_times,round(avg(CONVERT(overall_speed,decimal(10,2))),2) avg_download_rate,max(CONVERT(overall_speed,decimal(10,2))) best_download_rate ,p.bandwidth broadband_type" +
				" from "+dataSourceTable+" p , "+groupidsql+"   where 	consumerid != '' AND businessid != '' AND p.bandwidth != '' AND mac != '' AND start_time >= UNIX_TIMESTAMP('"+month+"01') * 1000 AND start_time < UNIX_TIMESTAMP('"+nextMonth+"01') * 1000 and cast(overall_speed AS signed)>0  and p.device_org = n.org_key  and service_type='下载' GROUP BY n.groupid,p.bandwidth"; 
		
		
		ResultSet rs = null;
		String url = srcurl + dataSourcebasename;

		System.out.println(sql);
		try {
			start(url, dstuser, dstpassword);

			rs = statement.executeQuery(sql);
			while (rs.next()) {
				Map map = new HashMap();
				Map groupmap = new HashMap();

				
				String groupid = rs.getString("groupid");
				String groupname = rs.getString("groupname");
				String download_test_times = rs.getString("download_test_times");
				String broadband_type = rs.getString("broadband_type");
				String avg_download_rate = rs.getString("avg_download_rate");
				String best_download_rate = rs.getString("best_download_rate");
				
				map.put("groupid", groupid);
				map.put("best_download_rate", best_download_rate);
				map.put("avg_download_rate", avg_download_rate);
				map.put("groupname", groupname);
				map.put("download_test_times", download_test_times);
				map.put("month", month);
				map.put("broadband_type", broadband_type);
				map.put("probetype", probetype.toUpperCase());
				
				
				groupmap.put("groupid", groupid);
				groupmap.put("broadband_type", broadband_type);
				groupmap.put("percentnum", download_test_times);

				listMap.add(map);
				
				groupList.add(groupmap);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return listMap;
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
	public List<Map> getDataApp(String dataSourceTable, String dataSourcebasename, String tarTableName,String month,String targetBaseName,String probetype,String authBaseName) {
		
		List<Map> listMap = new ArrayList<Map>();
		
		String sql = "";
		String nextMonth = UtilDate.getNextMonth(month);
		
		String groupidsql = "(SELECT t.org_key, m.groupid,	m. NAME	FROM "+authBaseName+".t_org t, ( SELECT j.org_id,j.groupid,i.name from "+authBaseName+".t_group_org_mapping i , (SELECT org_id, groupid" +
				" 	FROM "+authBaseName+".t_group_org_mapping a, "+targetBaseName+".group_id b WHERE a.group_id = b.groupid ) j where j.groupid =i.parent_org_id  ) m WHERE t.id = m.org_id) n";
		
		sql = "select case p.android_ios when 'ios' then 'iOS' when 'android' then 'Android' end android_ios, COUNT(id)  as download_test_times,'"+month+"' month,n.groupid groupid ,n.name groupname,substring_index(test_description, '/', - 1) broadband_type,round(avg(CONVERT(avg_rate,decimal(10,2))),2) AS avg_download_rate ,max(CONVERT(avg_rate,decimal(10,2)))  as best_download_rate " +
				" from "+dataSourcebasename+"."+dataSourceTable+" p , "+groupidsql+"  where 	p.imei!='' AND substring_index(test_description, '/', - 1) != '' and test_description LIKE '%/%'  AND time >= UNIX_TIMESTAMP('"+month+"01') * 1000 AND time < UNIX_TIMESTAMP('"+nextMonth+"01') * 1000 and  cast(avg_rate AS signed)>0  and p.device_org = n.org_key and file_type='download'  GROUP BY n.groupid,substring_index(test_description, '/', - 1),p.android_ios"; 
		
		ResultSet rs = null;
		String url = srcurl + dataSourcebasename;
		
		System.out.println(sql);
		try {
			start(url, dstuser, dstpassword);
			
			rs = statement.executeQuery(sql);
			while (rs.next()) {
				Map map = new HashMap();
				Map groupmap = new HashMap();
				
				String groupid = rs.getString("groupid");
				String groupname = rs.getString("groupname");
				String download_test_times = rs.getString("download_test_times");
				String broadband_type = rs.getString("broadband_type");
				String avg_download_rate = rs.getString("avg_download_rate");
				String best_download_rate = rs.getString("best_download_rate");
				String android_ios = rs.getString("android_ios");
				
				map.put("groupid", groupid);
				map.put("best_download_rate", best_download_rate);
				map.put("avg_download_rate", avg_download_rate);
				map.put("groupname", groupname);
				map.put("download_test_times", download_test_times);
				map.put("month", month);
				map.put("broadband_type", broadband_type);
				map.put("probetype", android_ios);
				
				
				groupmap.put("groupid", groupid);
				groupmap.put("broadband_type", broadband_type);
				groupmap.put("percentnum", download_test_times);
				listMap.add(map);
				
				groupList.add(groupmap);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return listMap;
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
			System.out.println("拼装语句错误" + sql);
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
	 * 静�?sql的批量插�?
	 * 
	 * @param sqlList
	 * @return
	 */
	public boolean dealSqlList(List<String> sqllist, String dstBaseName) {

		// 获取数据库连�?
		int count = 0;
		boolean flag = false;
		try {
			String url = dsturl+ dstBaseName;
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
				// 1000条记录插入一�?
				count++;
				// System.out.println(count);
				if (count % 20000 == 0) {
					statement.executeBatch();
					conn.commit();
				}
			}
			// 不足1000的最后进行插�?
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
