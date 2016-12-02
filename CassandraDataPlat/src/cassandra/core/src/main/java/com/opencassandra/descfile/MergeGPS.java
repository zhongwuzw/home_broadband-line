package com.opencassandra.descfile;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

import com.mysql.jdbc.DatabaseMetaData;

public class MergeGPS {
	Statement statement;
	Connection conn;
	
	public static void main(String[] args) {
		MergeGPS mg = new MergeGPS();
		for (int j = 0; j < ConfParser.table.length; j++) {
			String table = ConfParser.table[j];
			if(!mg.queryTableExist(table)){
				continue;
			}
			List<GPSData> list = mg.merge(table);
			GPSData gpsData = null;
			double this_lat = 0.0;
			double this_lng = 0.0;
			double this_rate = 0.0;
			int this_count = 0;
			int num = list.size();
			String mapLevel = ConfParser.mapLevel==null?"1":(ConfParser.mapLevel[0]==null?"1":ConfParser.mapLevel[0]);
			List list1 = mg.getLNGLATByLevel(mapLevel);
			double LNGinterval =0.0001;
			double LATinterval =0.00008;
			if(list1!=null){
				LNGinterval = (Double)list1.get(0);
				LATinterval = (Double)list1.get(1);	
			}
			for (int i = 0; i < list.size(); i++) {
				if(gpsData == null){
					gpsData = list.get(i);
					this_count = gpsData.getTest_times();
					this_lat = gpsData.getLat();
					this_lng = gpsData.getLng();
					this_rate = gpsData.getAvg_rate();
				}
				for (int k = i+1; k < list.size(); k++) {
					GPSData newGpsData = list.get(k);
					double other_lat = newGpsData.getLat();
					double other_lng = newGpsData.getLng();
					double other_rate = newGpsData.getAvg_rate();
					int other_count = newGpsData.getTest_times();
					if (Double.compare(Math.abs((this_lat-other_lat)),LATinterval)==-1 && Double.compare(Math.abs((this_lng-other_lng)),LNGinterval)==-1) {
						list.remove(newGpsData);
						list.remove(i);
						double new_lat  = (this_lat *this_count + other_lat*other_count) / (this_count + other_count);
						double new_lng = (this_lng *this_count + other_lng*other_count) / (this_count + other_count);
						double new_rate = (this_rate*this_count + other_rate*other_count) / (this_count + other_count);
						int new_count = this_count + other_count;
						gpsData.setAvg_rate(new_rate);
						gpsData.setLat(new_lat);
						gpsData.setLng(new_lng);
						gpsData.setTest_times(new_count);
						list.add(gpsData);
						gpsData = null;
						i--;
						break;
					}
					if(k==list.size()-1){
						gpsData = null;
					}
				}
			}
			String mergetTableName = "merge_"+table+"_"+mapLevel;
			if(!mg.queryTableExist(mergetTableName)){
				mg.execute("pro_createtable_merge",mergetTableName);	
			}
			String insertMoreSql = "";
			for (int i = 0; i < list.size(); i++) {
				insertMoreSql += "(";
				GPSData gpsData1 = list.get(i);
				insertMoreSql += gpsData1.getLng()+","; 
				insertMoreSql += gpsData1.getLat()+",";
				insertMoreSql += gpsData1.getTest_times()+",";
				insertMoreSql += gpsData1.getAvg_rate()+"),";
				if(i%1000==0 || i == (list.size()-1)){
					if(insertMoreSql.endsWith(",")){
						insertMoreSql = insertMoreSql.substring(0, insertMoreSql.length()-1);
					}
					try {
						mg.start();
						insertMoreSql = "insert into "+mergetTableName+" (lng,lat,test_times,avg_rate) values " + insertMoreSql;
						mg.insert(insertMoreSql);
						insertMoreSql = "";
					} catch (Exception e) {
						e.printStackTrace();
					}finally{
						try {
							mg.close();
						} catch (SQLException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
			}
			if(list.size()>0){
				
			}else{
				return ;
			}
		}
	}
	
	public List<GPSData> merge(String table){
		String sql = "select * from "+table;
		start();
		ResultSet rs = null;
		List<GPSData> list = new ArrayList<GPSData>();
		try {
			rs = statement.executeQuery(sql);
			if(rs!=null){ 
				while(rs.next()){
					double lng = rs.getDouble("lng");
					double lat = rs.getDouble("lat");
					int test_times = rs.getInt("test_times");
					double avg_rate = rs.getDouble("avg_rate");
					String province = rs.getString("province");
					String city = rs.getString("city");
					String district = rs.getString("district");
					String street_number = rs.getString("street_number");
					String city_code = rs.getString("city_code");
					GPSData gps = new GPSData();
					gps.setAvg_rate(avg_rate);
					gps.setProvince(province);
					gps.setCity(city);
					gps.setDistrict(district);
					gps.setLng(lng);
					gps.setLat(lat);
					gps.setSteet_number(street_number);
					gps.setTest_times(test_times);
					gps.setCity_code(city_code);
					list.add(gps);
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}finally{
			try {
				close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return list;
	}
	public void start() {
		String driver = "com.mysql.jdbc.Driver";
		String url = ConfParser.url;
//		String url = "jdbc:mysql://218.206.179.109:3306/testdataanalyse";
		String user = ConfParser.user;
		String password = ConfParser.password;
//		String password = "Bi123456";
		try {
			Class.forName(driver);
			conn = DriverManager.getConnection(url, user, password);
			statement = conn.createStatement();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void close() throws SQLException {
		if(statement!=null){
			statement.close();
		}
		if (conn != null) {
			conn.close();
		}
	}
	
	public boolean insert(String sql) {
		System.out.println("sql:" + sql);
		boolean flag = false;
		if(sql.isEmpty()){
			flag = false;
		}else{
			try {
				flag = statement.execute(sql);
			} catch (SQLException e) {
				e.printStackTrace();
				return false;
			}	
		}
		return flag;
	}

	/**
	 * 查询表是否存在
	 * @param tableName
	 * @return
	 */
	public boolean queryTableExist(String tableName){
		start();
		boolean flag = false;
		DatabaseMetaData databaseMetaData;
		try {
			databaseMetaData = (DatabaseMetaData) conn.getMetaData();
			ResultSet resultSet = databaseMetaData.getTables(null, null, tableName, null);
			if(resultSet.next()){
				flag = true;
			}else{
				flag = false;
			}
			resultSet.close();
		} catch (SQLException e) {
			flag = false;
			e.printStackTrace();
		}finally{
			try {
				close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return flag;
	}
	
	/**
	 * 一个字符串参数的存储过程调用
	 * @param name
	 * @param value
	 */
	public void execute(String name,String value) {
		start(); 	
        CallableStatement stat = null;
        String sql = "{call "+name+" (?)}";
        try {
            stat = conn.prepareCall(sql);
            stat.setString(1, value);
            stat.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }finally{
        	if(stat!=null){
        		try {
					stat.close();
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
		
	}
	
	public List getLNGLATByLevel(String mapLevel){
		List list = new ArrayList();
		String sql = "SELECT map.loninterval lng,map.latinterval lat FROM map_level map where level = "+mapLevel;
		start();
		double lng = 0;
		double lat = 0;
		try {
			ResultSet rs = statement.executeQuery(sql);
			if(rs.next()){
				lng = rs.getDouble("lng");
				lat = rs.getDouble("lat");
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}finally{
			try {
				close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		list.add(lng);
		list.add(lat);
		return list;
	}
}
