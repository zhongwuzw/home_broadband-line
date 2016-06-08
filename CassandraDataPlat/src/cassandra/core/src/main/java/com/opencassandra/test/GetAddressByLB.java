package com.opencassandra.test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import net.sf.json.JSONObject;

/**
 * testdataanalyse数据库表中数据，省市街道等信息是根据经纬度通过百度接口查询获得，为提高速度现需要与其他数据分开存储
 * 遍历所有表，根据经纬度通过百度接口查询省市街道信息，将这些信息更新到数据库表中，与其他数据合并
 * @author wuxiaofeng
 *
 */
public class GetAddressByLB {
	
	public static void main(String[] args) {
		
		try {
			Date start = new Date();
			System.out.println("Start:"+start.toLocaleString());
			// 要处理的数据库表名集合
//			String[] tableNames = {"call_test", "hand_over", "internet_downloading", "ping", "sms", "speed_test", "web_browsing"};
			String[] tableNames = {"call_test_wu"};
			GetAddressByLB getAddressByLB = new GetAddressByLB();
			// 依次处理每张表
			for (String tableName : tableNames) {
				// 获取表中有经纬度信息的数据列表
				List<LBInfo> lbInfoList = getAddressByLB.findLBFromDB(tableName);
				if (lbInfoList != null && lbInfoList.size() > 0) {
					// 根据经纬度获取省市街道信息，封装结果集集合
					List<LBInfo> resultList = getAddressByLB.getAddrByLB(lbInfoList);
					if (resultList != null && resultList.size() > 0) {
						// 将数据更新到数据库表中
						int count = getAddressByLB.updateDB(resultList, tableName);
						System.out.println("数据库表：" + tableName + ", 更新数据行数：" + count);
					}
				}
			}
			Date end = new Date();
			System.out.println("Start:"+start.toLocaleString()+">>> End:"+end.toLocaleString());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 获取表中有经纬度信息的数据列表
	 * @param tableName 数据库表名
	 * @return
	 */
	public List<LBInfo> findLBFromDB(String tableName) {
		
		Connection conn = getConn();
		
		Integer id = null;
		String latitude = "";    // 维度
		String longitude = "";   // 经度
		try {
			String sql = "SELECT id, longitude, latitude FROM " + tableName;
			PreparedStatement stmt= conn.prepareStatement(sql);
			ResultSet rs = stmt.executeQuery(sql);
			List<LBInfo> lbList = new ArrayList<LBInfo>();
			while (rs.next()) {
				id = rs.getInt("id");
				longitude = rs.getString("longitude");
				latitude = rs.getString("latitude");
				if (id != null && longitude != null && !"".equals(longitude) && latitude != null && !"".equals(latitude)) {
					LBInfo lbInfo = new LBInfo(id, longitude, latitude);
					lbList.add(lbInfo);
				}
			}
			return lbList;
		} catch (SQLException e) {
			System.out.println("出错了！");
			e.printStackTrace();
			return null;
		} finally {
			if (conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	/**
	 * 根据经纬度获取省市街道信息，封装结果集集合
	 * @param lbInfoList
	 * @return
	 */
	public List<LBInfo> getAddrByLB(List<LBInfo> lbInfoList) {
		
		try {
			List<LBInfo> resultList = new ArrayList<LBInfo>();
			for (LBInfo lbInfo : lbInfoList) {
				
				String baiduJsonStr = BaiduAPI.testPost(lbInfo.getLatitude(), lbInfo.getLongitude());
				JSONObject baiduJson = JSONObject.fromObject(baiduJsonStr);
				
				LBInfo lb = new LBInfo(lbInfo.getId(), lbInfo.getLongitude(), lbInfo.getLatitude());
				StringBuffer location = new StringBuffer();
				if (baiduJson.containsKey("result")) {
					baiduJson = JSONObject.fromObject(baiduJson.getString("result"));
					if (baiduJson.containsKey("addressComponent")) {
						baiduJson = JSONObject.fromObject(baiduJson.getString("addressComponent"));
						if (baiduJson.containsKey("province")) {
							lb.setProvince(baiduJson.getString("province"));
							location.append(baiduJson.getString("province"));
						}
						if (baiduJson.containsKey("city")) {
							lb.setCity(baiduJson.getString("city"));
							location.append("_");
							location.append(baiduJson.getString("city"));
						}
						if (baiduJson.containsKey("district")) {
							lb.setDistrict(baiduJson.getString("district"));
							location.append("_");
							location.append(baiduJson.getString("district"));
						}
						if (baiduJson.containsKey("street")) {
							location.append("_");
							location.append(baiduJson.getString("street"));
						}
						if (baiduJson.containsKey("street_number")) {
							location.append("_");
							location.append(baiduJson.getString("street_number"));
						}
						lb.setLocation(location.toString());
						resultList.add(lb);
					}
				}
			}
			return resultList;
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	/**
	 * 将数据更新到数据库表中
	 * @param lbInfoList
	 * @param tableName
	 * @return
	 */
	public int updateDB(List<LBInfo> lbInfoList, String tableName) {
		
		Connection conn = getConn();
		try {
			conn.setAutoCommit(false);
			String sql = "UPDATE " + tableName + " SET province=?, city=?, district=?, location=? WHERE id=?";
			PreparedStatement stmt= conn.prepareStatement(sql);
			for (LBInfo lbInfo : lbInfoList) {
				stmt.setString(1, lbInfo.getProvince());
				stmt.setString(2, lbInfo.getCity());
				stmt.setString(3, lbInfo.getDistrict());
				stmt.setString(4, lbInfo.getLocation());
				stmt.setInt(5, lbInfo.getId());
				stmt.addBatch(); 
			}
			int[] tt = stmt.executeBatch();
	        System.out.println("update : " + tt.length); 
		    conn.commit();
	        return tt.length; 
		} catch (SQLException e) {
			e.printStackTrace();
			return 0;
		} finally {
			if (conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	private Connection getConn() {
		
		String driver = "com.mysql.jdbc.Driver";
		String url = "jdbc:mysql://192.168.85.233:3306/testdataanalyse";
		String user = "root";
		String password = "cmrictpdata";
		try {
			Class.forName(driver);
			Connection conn = DriverManager.getConnection(url, user, password);
			return conn;
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}
}