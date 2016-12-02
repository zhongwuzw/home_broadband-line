package cn.speed.jdbc.utils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.mchange.v2.c3p0.ComboPooledDataSource;

/**
 * @author yangjunwei
 */
public class JDBCUtils {
	// 使用C3P0默认配置
	private static ComboPooledDataSource ds = new ComboPooledDataSource();

	// 定义线程容器，用于存储当前线程使用的连接对象
	public static ThreadLocal<Connection> container = new ThreadLocal<Connection>();

	/**
	 * 从连接池中获取数据库连接——并将连接加入到线程容器中
	 * 
	 * @return 数据库连接对象
	 */
	public static Connection getConnection() {
		Connection conn = null;
		try {
			conn = container.get();
			if (conn == null) {
				conn = ds.getConnection();
				container.set(conn);
			}

		} catch (SQLException e) {
			// log:获取数据库连接失败！
			throw new RuntimeException(e);
		}
		return conn;
	}

	/**
	 * 释放数据库连接对象，SQL运行环境对象，结果集对象占用的资源
	 * 
	 * @param conn_数据库连接对象
	 * @param st_SQL运行环境对象
	 * @param rs_结果集对象
	 */
	public static void release(Connection conn, Statement st, ResultSet rs) {
		// 关闭结果集对象
		if (rs != null) {
			try {
				rs.close();
			} catch (SQLException e) {
				// log:关闭结果集对象失败！
				throw new RuntimeException(e);
			} finally {
				rs = null;
			}
		}

		// 关闭SQL运行环境对象
		if (st != null) {
			try {
				st.close();
			} catch (SQLException e) {
				// log:关闭SQL运行环境对象失败！
				throw new RuntimeException(e);
			} finally {
				st = null;
			}
		}

		// 关闭数据库连接——连接还池
		if (conn != null) {
			try {
				conn.close();
			} catch (SQLException e) {
				// log:关闭数据库连接——连接还池操作失败！
				throw new RuntimeException(e);
			} finally {
				conn = null;
				container.remove();// 将数据库连接从当前线程容器中移除
			}
		}
	}

	/**
	 * 关闭数据库连接——连接还池
	 */
	public static void close() {
		Connection conn = container.get();
		try {
			if (conn == null)
				return;
			conn.close();
		} catch (Exception e) {
			// log:关闭数据库连接——连接还池操作失败！
			throw new RuntimeException(e);
		} finally {
			container.remove();// 将数据库连接从当前线程容器中移除
		}
	}

	/**
	 * 获取数据库连接，开启事务
	 */
	public static void startTransaction() {
		Connection conn = container.get();
		try {
			if (conn == null) {
				conn = getConnection();
			}
			conn.setAutoCommit(false);
		} catch (SQLException e) {
			// log:开启事务失败！
			throw new RuntimeException(e);
		}
	}

	/**
	 * 提交事务
	 */
	public static void commit() {
		Connection conn = container.get();
		try {
			if (conn == null)
				return;
			conn.commit();
		} catch (SQLException e) {
			// log:提交事务失败！
			throw new RuntimeException(e);
		}
	}

	/**
	 * 回滚事务
	 */
	public static void rollback() {
		Connection conn = container.get();
		try {
			if (conn == null)
				return;
			conn.rollback();
		} catch (SQLException e) {
			// log:回滚事务失败！
			throw new RuntimeException(e);
		}
	}

}
