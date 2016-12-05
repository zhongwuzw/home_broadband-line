package com.opencassandra.Mesher.database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class DatabaseUtil {

	public static Connection conn;
	public static Statement statement;
	
		static{
			String driver = "com.mysql.jdbc.Driver";
			try {
				Class.forName(driver);
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
		}
		
		public static Connection getconn(String url, String user, String password) throws SQLException{
			return conn = DriverManager.getConnection(url, user, password);
		}
		
}
