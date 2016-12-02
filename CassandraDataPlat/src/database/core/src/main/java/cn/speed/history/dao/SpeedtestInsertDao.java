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

public class SpeedtestInsertDao {
	
	/**
	 * 
	 * @return
	 */
	public List<Map<String,Object>> findAll(){
		Connection conn = null;
		PreparedStatement pst = null;
		ResultSet rs = null;
		try{
			String sql = "select * from c_speedtest_log";
			
			conn = JDBCUtils.getConnection();
			pst = conn.prepareStatement(sql);
			rs = pst.executeQuery();
			
			List<Map<String,Object>> list = new ArrayList<Map<String,Object>>();
			while(rs.next()){
				Map<String,Object> map = new HashMap<String, Object>();
				map.put("id", rs.getObject("id"));
				map.put("device_model", rs.getString("device_model"));
				map.put("device_imei", rs.getString("device_imei"));
				map.put("device_mac", rs.getString("device_mac"));
				map.put("device_code", rs.getString("device_code"));
				map.put("test_mode", rs.getString("test_mode"));
				map.put("test_type", rs.getString("test_type"));
				map.put("store_path", rs.getString("store_path"));
				map.put("apply_time", rs.getObject("apply_time"));
				
				list.add(map);
			}
			
			return list;
			
		} catch(Exception e){
			throw new RuntimeException(e);
		} finally{
			JDBCUtils.release(null, pst, rs);
		}
	}
	
	public int insert(Map<String,Object> list){
		Connection conn = null;
		PreparedStatement pst = null;
		try{
			conn = JDBCUtils.getConnection();
			
			String sql = "insert into c_speedtest_log (" +
			"mfr,"+"imei,"+"sim_state,"+"net_state,"+"operator,"+
			"roaming,"+ "ots_test_position,"+ "inside_ip," +"outside_ip,"+
			"mac,"+ "l_net_type,"+ "l_net_name," +"lac,"+
			"cid,"+ "tm_model,"+ "os_ver," +"ots_ver,"+
			"update_time,"+ "machine_model,"+ "hw_ver," +"speed_test_position,"+
			"d_net_name,"+ "d_net_type,"+ "server_info," +"tm_inside_ip,"+
			"tm_outside_ip,"+ "tm_mac,"+ "protocol_type," +"time_delay,"+
			"down_velocity,"+ "up_velocity,"+  "log_detail,"+"file_id,"+"org_id"+
					") values ('"+list.get("mfr")+"','"+list.get("imei")+"','"+list.get("sim_state")+"','"+list.get("net_state")+"','"+list.get("operator")+"','"+
					list.get("roaming")+"','"+list.get("ots_test_position")+"','"+list.get("inside_ip")+"','"+list.get("outside_ip")+"','"+list.get("mac")+"','"+
					list.get("l_net_type")+"','"+list.get("l_net_name")+"','"+list.get("lac")+"','"+list.get("cid")+"','"+list.get("tm_model")+"','"+
					list.get("os_ver")+"','"+list.get("ots_ver")+"','"+list.get("update_time")+"','"+list.get("machine_model")+"','"+list.get("hw_ver")+"','"+
					list.get("speed_test_position")+"','"+list.get("d_net_name")+"','"+list.get("d_net_type")+"','"+list.get("server_info")+"','"+list.get("tm_inside_ip")+"','"+
					list.get("tm_outside_ip")+"','"+list.get("tm_mac")+"','"+list.get("protocol_type")+"',"+Integer.parseInt(((String)list.get("time_delay")).replace("ms", ""))+","+
					Double.parseDouble(((String)list.get("down_velocity")).replace("Mbps", "").replace("Kbps", ""))+","+Double.parseDouble(((String)list.get("up_velocity")).replace("Mbps", "").replace("Kbps", ""))+",'"+
					list.get("log_detail")+"',"+
					list.get("file_id")+","+
					list.get("org_id")+
					")";
			pst = conn.prepareStatement(sql);
			
		return	pst.executeUpdate();
			
		} catch(Exception e){
			throw new RuntimeException(e);
		} finally{
			JDBCUtils.release(null, pst, null);
		}
	}
}
