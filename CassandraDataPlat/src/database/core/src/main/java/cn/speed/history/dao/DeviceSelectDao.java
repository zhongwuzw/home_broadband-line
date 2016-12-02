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
import cn.speed.jdbc.utils.SearchValue;

public class DeviceSelectDao {

	/**
	 * 
	 * @return
	 */
	public List<Map<String, Object>> find(SearchValue sv) {
		Connection conn = null;
		PreparedStatement pst = null;
		ResultSet rs = null;
		try {
			String sql = "select a.id id,a.m_model model,a.o_last_update_time last_update,b.s_device_state device_state,b.g_longtitude longtitude,b.g_latitude latitude "
					+ "from d_device_info a,d_device_status_log b "
					+ "where a.id=b.bid ";
					sql += ((sv.getDeviceType().equals(""))?"":("and a.m_type="+"\""+sv.getDeviceType()+"\"")) + " ";
					sql += ((sv.getDeviceMfr().equals(""))?"":("and a.m_mfr="+"\""+sv.getDeviceMfr()+"\"")) + " ";
					sql += ((sv.getDeviceModel().equals(""))?"":("and a.m_model="+"\""+sv.getDeviceModel()+"\"")) + " ";
					sql += ((sv.getDeviceLocation().equals(""))?"":("and a.m_belong="+"\""+sv.getDeviceLocation()+"\"")) + " ";
					sql += ((sv.getDeviceState().equals(""))?"":("and b.s_device_state="+"\""+sv.getDeviceState()+"\"")) + " ";
					sql += ((sv.getDeviceTestState().equals(""))?"":("and b.s_test_state="+"\""+sv.getDeviceTestState()+"\"")) + " ";
					sql += "and b.id=("
						+ "select max(cb.id) from d_device_status_log cb "
						+ "where cb.bid=a.id" 
						+ ");";
			
			System.out.println(sql);
			conn = JDBCUtils.getConnection();
			pst = conn.prepareStatement(sql);
			rs = pst.executeQuery();

			List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
			while (rs.next()) {
				Map<String, Object> map = new HashMap<String, Object>();
				map.put("id", rs.getObject("id"));
				map.put("model", rs.getObject("model"));
				map.put("last_update", rs.getString("last_update"));
				map.put("device_state", rs.getObject("device_state"));
				map.put("longtitude", Double.toString(rs.getDouble("longtitude")));
				map.put("latitude", Double.toString(rs.getDouble("latitude")));

				list.add(map);
			}

			return list;

		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			JDBCUtils.release(null, pst, rs);
		}
	}
}
