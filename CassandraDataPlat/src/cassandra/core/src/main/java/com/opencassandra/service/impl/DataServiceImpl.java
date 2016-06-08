package com.opencassandra.service.impl;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.List;

import com.opencassandra.descfile.ConfParser;
import com.opencassandra.service.DataService;

public class DataServiceImpl implements DataService {

	public static Connection conn;
	public static Statement statement;

	// 数据入库的地址
	private static String dsturl = ConfParser.dsturl;
	private static String dstuser = ConfParser.dstuser;
	private static String dstpassword = ConfParser.dstpassword;

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
	@Override
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
	@Override
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
	 * 创建数据入库的数据库
	 * 
	 * @param database
	 * @return void
	 */
	@Override
	public void createDB(String database) {
		try {
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
	 * 执行单条sql
	 * 
	 * @param sql
	 * @return
	 * @return boolean
	 */
	@Override
	public boolean insert(String sql) {

		boolean flag = false;
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
					this.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		return flag;
	}

	/**
	 * 批量插入sql
	 * 
	 * @param sqllist
	 * @param dsBaseName
	 * @return
	 * @return boolean
	 */
	@Override
	public boolean insertList(List<String> sqllist, String dsBaseName) {

		// 获取数据库连接
		int count = 0;
		boolean flag = false;
		try {
			try {
				String url = dsturl.substring(0, dsturl.lastIndexOf("/") + 1) + dsBaseName;
				start(url, dstuser, dstpassword);
			} catch (Exception e) {
				e.printStackTrace();
			}
			for (Iterator iterator = sqllist.iterator(); iterator.hasNext();) {
				String sql = (String) iterator.next();
				System.out.println(sql + "  ::::::");
				// count++;
				conn.setAutoCommit(false);
				statement.addBatch((String) sql);
				// 10000条记录插入一次
				count++;
				if (count % 50000 == 0) {
					statement.executeBatch();
					conn.commit();
				}
			}
			// 不足10000的最后进行插入
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
	 * 查询创建数据入库数据库
	 * 
	 * @param code
	 * @param srcBaseName
	 * @return void
	 */
	@Override
	public void startOrCreateDB(String code, String srcBaseName) {

		// 应首先查询该数据库是否存在
		String url = dsturl.substring(0, dsturl.lastIndexOf("/") + 1) + srcBaseName;
		try {
			start(url, dstuser, dstpassword);
		} catch (Exception e) {
			// 创建相应的数据库
			try {
				start(dsturl, dstuser, dstpassword);
			} catch (Exception e2) {
				// TODO Auto-generated catch block
				e2.printStackTrace();
			}
			createDB(srcBaseName);
			try {
				start(url, dstuser, dstpassword);
			} catch (Exception e1) {
				e1.printStackTrace();
			}
		} finally {
			try {
				this.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	
	
	public static void main(String[] args) {
		
	}
}
