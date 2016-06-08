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

public class DeviceStatusLogDao {
	
	/**
	 * 
	 * @return
	 */
	public List<Map<String,Object>> findAll(){
		Connection conn = null;
		PreparedStatement pst = null;
		ResultSet rs = null;
		try{
			String sql = "select * from d_device_status_log";
			
			conn = JDBCUtils.getConnection();
			pst = conn.prepareStatement(sql);
			rs = pst.executeQuery();
			
			List<Map<String,Object>> list = new ArrayList<Map<String,Object>>();
			while(rs.next()){
				Map<String,Object> map = new HashMap<String, Object>();
				map.put("id", rs.getObject("id"));
				map.put("devicemodel", rs.getObject("m_model"));
				map.put("devicetype", rs.getObject("m_type"));
				map.put("manulfact", rs.getObject("m_mfr"));
				map.put("belong", rs.getObject("m_belong"));
				map.put("os", rs.getObject("c_os"));
				map.put("imei", rs.getObject("c_imei"));
				map.put("lastut", rs.getString("o_last_update_time"));
				
				list.add(map);
			}
			
			return list;
			
		} catch(Exception e){
			throw new RuntimeException(e);
		} finally{
			JDBCUtils.release(null, pst, rs);
		}
	}
	
	public List<Map<String,Object>> findById(String id){
		Connection conn = null;
		PreparedStatement pst = null;
		ResultSet rs = null;
		try{
			String sql = "select * from d_device_status_log where id =="+id;
			
			conn = JDBCUtils.getConnection();
			pst = conn.prepareStatement(sql);
			rs = pst.executeQuery();
			
			List<Map<String,Object>> list = new ArrayList<Map<String,Object>>();
			while(rs.next()){
				Map<String,Object> map = new HashMap<String, Object>();
				map.put("id", rs.getObject("id"));
				map.put("devicemodel", rs.getObject("m_model"));
				map.put("devicetype", rs.getObject("m_type"));
				map.put("manulfact", rs.getObject("m_mfr"));
				map.put("belong", rs.getObject("m_belong"));
				map.put("os", rs.getObject("c_os"));
				map.put("imei", rs.getObject("c_imei"));
				map.put("lastut", rs.getString("o_last_update_time"));
				
				list.add(map);
			}
			
			return list;
			
		} catch(Exception e){
			throw new RuntimeException(e);
		} finally{
			JDBCUtils.release(null, pst, rs);
		}
	}
	
	public int insert(List<String> list){
		Connection conn = null;
		PreparedStatement pst = null;
		try{
			conn = JDBCUtils.getConnection();
			
			String sql = "insert into d_device_info (" +
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
