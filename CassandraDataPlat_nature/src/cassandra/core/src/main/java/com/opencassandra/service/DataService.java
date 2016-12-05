package com.opencassandra.service;

import java.sql.SQLException;
import java.util.List;

/**
 * 有关数据库操作
 * 
 * @author：kxc
 * @date：Nov 19, 2015
 */
public interface DataService {

	/**
	 * 建立数据库连接
	 * 
	 * @param url
	 * @param user
	 * @param password
	 * @return void
	 * @throws SQLException
	 */
	void start(String url, String user, String password) throws Exception;

	/**
	 * 关闭数据库连接
	 * 
	 * @return void
	 * @throws SQLException
	 */
	void close() throws SQLException;

	/**
	 * 创建数据库
	 * 
	 * @param database
	 * @return void
	 */
	void createDB(String database);

	/**
	 * 执行单条sql
	 * 
	 * @param sql
	 * @return
	 * @return boolean
	 */
	boolean insert(String sql);

	/**
	 * 批量插入sql
	 * 
	 * @param sqllist
	 * @param dsBaseName
	 * @return
	 * @return boolean
	 */
	boolean insertList(List<String> sqllist, String dsBaseName);

	/**
	 * 查询创建数据库
	 * 
	 * @param code
	 * @param srcBaseName
	 * @return void
	 */
	void startOrCreateDB(String code, String srcBaseName);
}
