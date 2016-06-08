package cn.speed.history.dao;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cn.speed.jdbc.utils.JDBCUtils;

public class ResultUploadDao {

	/**
	 * 
	 * @return
	 */
	public List<Map<String, Object>> findAll() {
		Connection conn = null;
		PreparedStatement pst = null;
		ResultSet rs = null;
		try {
			String sql = "select * from uploadfile_apply";

			conn = JDBCUtils.getConnection();
			pst = conn.prepareStatement(sql);
			rs = pst.executeQuery();

			List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
			while (rs.next()) {
				Map<String, Object> map = new HashMap<String, Object>();
				map.put("id", rs.getObject("id"));
				map.put("device_model", rs.getString("device_model"));
				map.put("device_imei", rs.getString("device_imei"));
				map.put("device_mac", rs.getString("device_mac"));
				map.put("device_code", rs.getString("device_code"));
				map.put("test_mode", rs.getString("test_mode"));
				map.put("test_type", rs.getString("test_type"));
				map.put("store_path", rs.getString("store_path"));
				map.put("apply_time", rs.getObject("apply_time"));
				map.put("planid", rs.getObject("planid"));
				map.put("caseid", rs.getObject("caseid"));
				map.put("excuid", rs.getObject("excuid"));

				list.add(map);
			}

			return list;

		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			JDBCUtils.release(null, pst, rs);
		}
	}

	/**
	 * 
	 * @return
	 */
	public List<Map<String, Object>> findByType(String test_type, int[] isInsert) {
		Connection conn = null;
		PreparedStatement pst = null;
		ResultSet rs = null;
		try {
			String sql = "select * from uploadfile_apply where test_mode='"
					+ test_type + "' and (isInsert=" + isInsert[0]+" or isInsert = "+isInsert[1]+") "+ " limit 0,100";

			conn = JDBCUtils.getConnection();
			pst = conn.prepareStatement(sql);
			rs = pst.executeQuery();

			List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
			while (rs.next()) {
				Map<String, Object> map = new HashMap<String, Object>();
				map.put("id", rs.getObject("id"));
				map.put("test_mode", rs.getString("test_mode"));
				map.put("device_type", rs.getString("device_type"));
				map.put("device_model", rs.getString("device_model"));
				map.put("device_imei", rs.getString("device_imei"));
				map.put("device_mac", rs.getString("device_mac"));
				map.put("device_code", rs.getString("device_code"));
				map.put("store_path", rs.getString("store_path"));
				map.put("isInsert", rs.getObject("isInsert"));
				map.put("apply_time", rs.getObject("apply_time"));
				map.put("planid", rs.getObject("planid"));
				map.put("caseid", rs.getObject("caseid"));
				map.put("excuid", rs.getObject("excuid"));
				map.put("org_id", rs.getObject("org_id"));

				list.add(map);
			}

			return list;

		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			JDBCUtils.release(null, pst, rs);
		}
	}

	/**
	 * 
	 * @return
	 */
	public List<Map<String, Object>> findByStorePath(String test_type, String storePath) {
		Connection conn = null;
		PreparedStatement pst = null;
		ResultSet rs = null;
		try {
			String sql = "select * from uploadfile_apply where test_mode='"
					+ test_type + "' and store_path='" + storePath+ "' limit 0,1";

			conn = JDBCUtils.getConnection();
			pst = conn.prepareStatement(sql);
			rs = pst.executeQuery();

			List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
			while (rs.next()) {
				Map<String, Object> map = new HashMap<String, Object>();
				map.put("id", rs.getObject("id"));
				map.put("test_mode", rs.getString("test_mode"));
				map.put("device_type", rs.getString("device_type"));
				map.put("device_model", rs.getString("device_model"));
				map.put("device_imei", rs.getString("device_imei"));
				map.put("device_mac", rs.getString("device_mac"));
				map.put("device_code", rs.getString("device_code"));
				map.put("store_path", rs.getString("store_path"));
				map.put("isInsert", rs.getObject("isInsert"));
				map.put("apply_time", rs.getObject("apply_time"));
				map.put("planid", rs.getObject("planid"));
				map.put("caseid", rs.getObject("caseid"));
				map.put("excuid", rs.getObject("excuid"));
				map.put("org_id", rs.getObject("org_id"));

				list.add(map);
			}

			return list;

		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			JDBCUtils.release(null, pst, rs);
		}
	}
	
	public int insert(Map<String, Object> list) {
		if (list.get("store_path") != null
				&& ((String) list.get("store_path")).contains(".abstract.csv")) {
			String fileName = (new File((String) list.get("store_path"))).getName();
			String[] fileProp = fileName.split("-");
			String[] caseProp = new String[3];
			if (fileProp[0] != null) {
				caseProp = fileProp[0].split("_");
			} else {
				for (int i = 0; i < 3; i++) {
					caseProp[i] = "";
				}
			}
			String planId = "";
			String caseId = "";
			String excuId = "";
			if (caseProp.length == 3) {
				planId = caseProp[0];
				caseId = caseProp[1];
				excuId = caseProp[2];
			} else if (caseProp.length == 2) {
				planId = caseProp[0];
				caseId = caseProp[1];
			} else if (caseProp.length == 1) {
				planId = caseProp[0];
			}

			Connection conn = null;
			PreparedStatement pst = null;
			try {
				conn = JDBCUtils.getConnection();
				String myPath = "";
				try{
					myPath = (String) list.get("store_path");
					if(myPath!=null && myPath.length()>=2000){
						myPath = myPath.substring(0, 2000);
					}
				}catch (Exception e){
					e.printStackTrace();
				}
				
				String sql = "insert into uploadfile_apply (" + "test_mode,"
						+ "device_type," + "device_model," + "device_imei,"
						+ "device_mac," + "device_code," + "store_path,"
						+ "isInsert," + "apply_time," + "planid," + "caseid,"
						+ "excuid," + "org_id"+") values ('"
						+ list.get("test_mode")
						+ "','"
						+ list.get("device_type")
						+ "','"
						+ list.get("device_model")
						+ "','"
						+ list.get("device_imei")
						+ "','"
						+ list.get("device_mac")
						+ "','"
						+ list.get("device_code")
						+ "','"
						+ myPath.replace("\\", "/")
						+ "',0,'"
						+ list.get("apply_time")
						+ "','"
						+ planId
						+ "','" + caseId + "','" + excuId + "',"+list.get("org_id")+")";
				pst = conn.prepareStatement(sql);

				return pst.executeUpdate();

			} catch (Exception e) {
				System.out.println("store_path=========="+list.get("store_path"));
				throw new RuntimeException(e);
			} finally {
				JDBCUtils.release(null, pst, null);
			}
		} else if (list.get("store_path") != null) {
			String fileName = (new File((String) list.get("store_path"))).getName();
			String[] fileProp = fileName.split("-");
			String[] caseProp = new String[3];
			if (fileProp[0] != null) {
				caseProp = fileProp[0].split("_");
			} else {
				for (int i = 0; i < 3; i++) {
					caseProp[i] = "";
				}
			}
			String planId = "";
			String caseId = "";
			String excuId = "";
			if (caseProp.length == 3) {
				planId = caseProp[0];
				caseId = caseProp[1];
				excuId = caseProp[2];
			} else if (caseProp.length == 2) {
				planId = caseProp[0];
				caseId = caseProp[1];
			} else if (caseProp.length == 1) {
				planId = caseProp[0];
			}

			Connection conn = null;
			PreparedStatement pst = null;
			try {
				conn = JDBCUtils.getConnection();
				String myPath = "";
				try{
					myPath = (String) list.get("store_path");
					if(myPath!=null && myPath.length()>=2000){
						myPath = myPath.substring(0, 2000);
					}
				}catch (Exception e){
					e.printStackTrace();
				}
				String sql = "insert into uploadfile_apply (" + "test_mode,"
						+ "device_type," + "device_model," + "device_imei,"
						+ "device_mac," + "device_code," + "store_path,"
						+ "isInsert," + "apply_time," + "planid," + "caseid,"
						+ "excuid," + "org_id"+") values ('"
						+ list.get("test_mode")
						+ "','"
						+ list.get("device_type")
						+ "','"
						+ list.get("device_model")
						+ "','"
						+ list.get("device_imei")
						+ "','"
						+ list.get("device_mac")
						+ "','"
						+ list.get("device_code")
						+ "','"
						+ myPath.replace("\\", "/")
						+ "',-1,'"
						+ list.get("apply_time")
						+ "','"
						+ planId
						+ "','" + caseId + "','" + excuId + "',"+list.get("org_id")+")";
				pst = conn.prepareStatement(sql);

				return pst.executeUpdate();

			} catch (Exception e) {
				throw new RuntimeException(e);
				
			} finally {
				JDBCUtils.release(null, pst, null);
			}
		}
		return -1;
	}
	/**
	 * 
	 * @return device_imei
	 */
	public List<Map<String,Object>> getStore_path(String device_imei){
		
		Connection conn = null;
		PreparedStatement pst = null;
		ResultSet rs = null;
		try {
			String sql = "SELECT b.store_path,b.org_id from d_device a left join"
					+ " b_result_path b on a.device_org_id = b.org_id where a.device_imei = '"+device_imei+"'";

			conn = JDBCUtils.getConnection();
			pst = conn.prepareStatement(sql);
			rs = pst.executeQuery();

			List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
			while (rs.next()) {
				Map<String, Object> map = new HashMap<String, Object>();
				map.put("store_path", rs.getString("store_path"));
				map.put("org_id", rs.getString("org_id"));
				list.add(map);
			}

			return list;

		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			JDBCUtils.release(null, pst, rs);
		}
		
	}

	/**
	 * 
	 * @return
	 */
	public int updateById(int id,String isInsert) {
		Connection conn = null;
		PreparedStatement pst = null;
		ResultSet rs = null;
		int state = -1;
		try {
			String sql = "UPDATE uploadfile_apply SET isInsert="+isInsert+" where id="
					+ id;

			conn = JDBCUtils.getConnection();
			pst = conn.prepareStatement(sql);
			state = pst.executeUpdate();
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			JDBCUtils.release(null, pst, rs);
		}
		return state;
	}
	public static void main(String[] args){
		String fileName = (new File("C:\\Users\\cmcc\\Desktop\\测试能力\\ping\\0_04002.394_0-2013_07_28_14_52_23_579.deviceInfo.csv")).getName();
		String[] fileProp = fileName.split("-");
		String[] caseProp = new String[3];
		if (fileProp[0] != null) {
			caseProp = fileProp[0].split("_");
		} else {
			for (int i = 0; i < 3; i++) {
				caseProp[i] = "";
			}
		}
		String planId = "";
		String caseId = "";
		String excuId = "";
		if (caseProp.length == 3) {
			planId = caseProp[0];
			caseId = caseProp[1];
			excuId = caseProp[2];
		} else if (caseProp.length == 2) {
			planId = caseProp[0];
			caseId = caseProp[1];
		} else if (caseProp.length == 1) {
			planId = caseProp[0];
		}

		System.out.println(planId);
		System.out.println(caseId);
		System.out.println(excuId);
	}
}
