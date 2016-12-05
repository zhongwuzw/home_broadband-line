package com.opencassandra.jiakuan.base;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.mysql.jdbc.DatabaseMetaData;
import com.opencassandra.descfile.ConfParser;

public class BaseDaoImp implements BaseDao{

	public static Statement statement;
	public static Connection conn;
	// 数据源数据库的地址
	public static String srcurl = ConfParser.srcurl;
	public static String srcuser = ConfParser.srcuser;
	public static String srcpassword = ConfParser.srcpassword;

	// 数据入库的地址
	public static String dsturl = ConfParser.dsturl;
	public static String dstuser = ConfParser.dstuser;
	public static String dstpassword = ConfParser.dstpassword;

	// -----------------------------------------------------数据库连接相关操作---------------------------------------------------------
	/**
	 * 建立数据库连�?
	 * 
	 * @return void
	 */
	public void start(String url, String user, String password) {
		System.out.println(url + " :数据库连�? " + user + " :: " + password);
		this.close();
		String driver = "com.mysql.jdbc.Driver";
		try {
			Class.forName(driver);
		} catch (ClassNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		try {
			conn = DriverManager.getConnection(url, user, password);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			statement = conn.createStatement();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * 关闭数据库连�?
	 * 
	 * @return void
	 */
	public void close() {
		if (statement != null) {
			try {
				statement.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		if (conn != null) {
			try {
				conn.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
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
			start(dsturl, dstuser, dstpassword);
			String sql = "create DATABASE " + database;
			statement.execute(sql);
			close();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			close();
		}
	}

	/**
	 * 获取查询结果集
	 * 
	 * @param dataSourceTable
	 * @param dataSourcebasename
	 * @param sql
	 * @return
	 */
	public ResultSet getResultSet(String dataSourceTable, String dataSourcebasename, String sql) {

		ResultSet rs = null;
		String url = srcurl + dataSourcebasename;
		try {
			start(url, srcuser, srcpassword);
			rs = statement.executeQuery(sql);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return rs;
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
		return sql.toString();

	}

	/**
	 * 静态批量插入sql
	 * 
	 * @param sqlList
	 * @return
	 */

	public boolean dealSqlList(List<String> sqllist, String dstBaseName) {

		// 获取数据库连�?
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
			close();
		}
		return flag;
	}
}
