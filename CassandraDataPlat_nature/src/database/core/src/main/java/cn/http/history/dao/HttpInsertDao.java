package cn.http.history.dao;

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

public class HttpInsertDao {
	
	public int insert(Map<String,Object> list){
		Connection conn = null;
		PreparedStatement pst = null;
		try{
			conn = JDBCUtils.getConnection();
			
			String sql = "insert into c_http_log (" +
			"mfr,"+"imei,"+"sim_state,"+"net_state,"+"operator,"+
			"roaming,"+ "ots_test_position,"+ "inside_ip," +"outside_ip,"+
			"mac,"+ "l_net_type,"+ "l_net_name," +"lac,"+
			"cid,"+ "tm_model,"+ "os_ver," +"ots_ver,"+
			"update_time,"+ "machine_model,"+ "hw_ver," +"speed_test_position,"+
			"d_net_name,"+ "d_net_type,"+ "server_info," +"tm_inside_ip,"+
			"tm_outside_ip,"+ "tm_mac,"+ "test_times," +"success_rate,"+
			"protocol,"+ "ftp_type,"+ "time_delay,"+ "avg_velocity,"+"log_detail,"+"file_id," +"org_id"+
					") values ('"+list.get("mfr")+"','"+list.get("imei")+"','"+list.get("sim_state")+"','"+list.get("net_state")+"','"+list.get("operator")+"','"+
					list.get("roaming")+"','"+list.get("ots_test_position")+"','"+list.get("inside_ip")+"','"+list.get("outside_ip")+"','"+list.get("mac")+"','"+
					list.get("l_net_type")+"','"+list.get("l_net_name")+"','"+list.get("lac")+"','"+list.get("cid")+"','"+list.get("tm_model")+"','"+
					list.get("os_ver")+"','"+list.get("ots_ver")+"','"+list.get("update_time")+"','"+list.get("machine_model")+"','"+list.get("hw_ver")+"','"+
					list.get("speed_test_position")+"','"+list.get("d_net_name")+"','"+list.get("d_net_type")+"','"+list.get("server_info")+"','"+list.get("tm_inside_ip")+"','"+
					list.get("tm_outside_ip")+"','"+list.get("tm_mac")+"','"+list.get("test_times")+"','"+list.get("success_rate")+"','"+list.get("protocol")+"','"+
					list.get("ftp_type")+"',"+Integer.parseInt(((String)list.get("time_delay")).replace("ms", ""))+","+
					Double.parseDouble(((String)list.get("avg_velocity")).replace("Mbps", "").replace("Kbps", ""))+",'"+
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

//{l_net_type=WIFI, tm_model=GT-I9308, speed_test_position=--, d_net_type=LTE-Test, os_ver=4.0.4, d_net_name=WIFI,
//		 ots_ver=1.1.8, tm_inside_ip=192.168.1.5, machine_model=GT-I9308, 
//		mac=90:18:7C:47:29:57, lac=41009, ftp_type=GET, 
//
//		server_info=218.206.179.190, sim_state=89860091101250393758, net_state=已断开, fil_id=21380, ots_test_position=, 
//		time_delay=525, roaming=无漫游, inside_ip=192.168.1.5, l_net_name=LTE-Test, avg_velocity=, tm_outside_ip=223.104.3.9, 
//		log_detail=2013-09-02 15:21:34:093WIFIHTTPGEThttp://cdn.market.hiapk.com/data/upload/2013/07_19/11/com.carrot.carrotfantasy_114429.apk152.6814.407.5152530100%
//		, protocol=HTTP, imei=355065052393059, cid=29719, operator=中国移动, update_time=2013_09_02_15_19_50,
//		 test_times=2013-09-02 15:21:34:093, mfr=samsung, hw_ver=4.0.4, tm_mac=90:18:7C:47:29:57, success_rate=WIFI, 
//		 outside_ip=223.104.3.6}
