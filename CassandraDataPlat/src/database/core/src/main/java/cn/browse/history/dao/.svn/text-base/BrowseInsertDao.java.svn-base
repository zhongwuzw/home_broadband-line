package cn.browse.history.dao;

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

public class BrowseInsertDao {
	
	public int insert(Map<String,Object> list){
		Connection conn = null;
		PreparedStatement pst = null;
		try{
			conn = JDBCUtils.getConnection();
			
			String sql = "insert into c_browse_log (" +
			"mfr,"+"imei,"+"sim_state,"+"net_state,"+"operator,"+
			"roaming,"+ "ots_test_position,"+ "inside_ip," +"outside_ip,"+
			"mac,"+ "l_net_type,"+ "l_net_name," +"lac,"+
			"cid,"+ "tm_model,"+ "os_ver," +"ots_ver,"+
			"update_time,"+ "machine_model,"+ "hw_ver," +"speed_test_position,"+
			"d_net_name,"+ "d_net_type,"+ "server_info," +"tm_inside_ip,"+
			"tm_outside_ip,"+ "tm_mac,"+ "test_times," +"success_rate,"+
			"protocol,"+ "ftp_type,"+ "log_detail,"+"file_id," +"test_addr,"+"org_id"+
					") values ('"+list.get("mfr")+"','"+list.get("imei")+"','"+list.get("sim_state")+"','"+list.get("net_state")+"','"+list.get("operator")+"','"+
					list.get("roaming")+"','"+list.get("ots_test_position")+"','"+list.get("inside_ip")+"','"+list.get("outside_ip")+"','"+list.get("mac")+"','"+
					list.get("l_net_type")+"','"+list.get("l_net_name")+"','"+list.get("lac")+"','"+list.get("cid")+"','"+list.get("tm_model")+"','"+
					list.get("os_ver")+"','"+list.get("ots_ver")+"','"+list.get("update_time")+"','"+list.get("machine_model")+"','"+list.get("hw_ver")+"','"+
					list.get("speed_test_position")+"','"+list.get("d_net_name")+"','"+list.get("d_net_type")+"','"+list.get("server_info")+"','"+list.get("tm_inside_ip")+"','"+
					list.get("tm_outside_ip")+"','"+list.get("tm_mac")+"','"+list.get("test_times")+"','"+list.get("success_rate")+"','"+
					list.get("protocol")+"','"+list.get("ftp_type")+"','"/*+Double.parseDouble(((String)list.get("time_delay")).replace("ms", ""))+","*/+
//					Double.parseDouble(((String)list.get("max_time_delay")).replace("ms", ""))+","+
//					Double.parseDouble(((String)list.get("min_time_delay")).replace("ms", ""))+",'"+
					list.get("log_detail")+"',"+
					list.get("file_id")+",'"+
					list.get("test_addr")+"',"+
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
