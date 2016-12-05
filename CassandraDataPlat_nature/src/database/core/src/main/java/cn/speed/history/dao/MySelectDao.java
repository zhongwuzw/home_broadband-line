package cn.speed.history.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cn.speed.jdbc.utils.JDBCUtils;

public class MySelectDao {
	
	/**
	 * 
	 * @return
	 */
	public List<Map<String,Object>> test(){
		Connection conn = null;
		PreparedStatement pst = null;
		ResultSet rs = null;
		try{
			String sql = "select * from speedhistory";
			
			conn = JDBCUtils.getConnection();
			pst = conn.prepareStatement(sql);
			rs = pst.executeQuery();
			
			List<Map<String,Object>> list = new ArrayList<Map<String,Object>>();
			while(rs.next()){
				Map<String,Object> map = new HashMap<String, Object>();
				map.put("id", rs.getObject("id"));
				//Date testDate = rs.getString("testDate");
				//SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				//String testDateStr = sdf.format(testDate);
				map.put("testdate", rs.getString("testDate"));
				//map.put("testtime", rs.getObject("testtime"));
				map.put("IPaddress", rs.getObject("IPaddress"));
				map.put("prottype", rs.getObject("prottype"));
				map.put("upthread", rs.getObject("upthread"));
				map.put("downthread", rs.getObject("downthread"));
				map.put("upspeed", rs.getObject("upspeed"));
				map.put("downspeed", rs.getObject("downspeed"));
				map.put("upspeedavg", rs.getObject("upspeedavg"));
				map.put("downspeedavg", rs.getObject("downspeedavg"));
				//map.put("speedranking", rs.getObject("speedranking"));
			
				
				list.add(map);
			}
			
			return list;
			
		} catch(Exception e){
			throw new RuntimeException(e);
		} finally{
			JDBCUtils.release(null, pst, rs);
		}
	}
	
	public int testUpdate(List<String> list){
		Connection conn = null;
		PreparedStatement pst = null;
		try{
			conn = JDBCUtils.getConnection();
			
			String sql = "insert into speedhistory (" +
			"testdate,"+"IPaddress,"+"prottype,"+"upthread,"+"downthread,"+
			"upspeed,"+ "downspeed," +"upspeedavg,"+"downspeedavg"+
					") values ('"+list.get(0)+"','"+list.get(1)+"','"+list.get(2)+"','"+list.get(3)+"','"+list.get(4)+"','"+
					list.get(5)+"','"+list.get(6)+"','"+list.get(7)+"','"+list.get(8)+
					"')";
			pst = conn.prepareStatement(sql);
			
		return	pst.executeUpdate();
			
		} catch(Exception e){
			throw new RuntimeException(e);
		} finally{
			JDBCUtils.release(null, pst, null);
		}
	}
}
