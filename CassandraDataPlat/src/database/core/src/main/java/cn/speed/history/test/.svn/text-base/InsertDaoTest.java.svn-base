package cn.speed.history.test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import cn.speed.jdbc.utils.JDBCUtils;

public class InsertDaoTest {
	
	/**
	 * 
	 * @return
	 */
	public List<Map<String,Object>> findAll(){
		Connection conn = null;
		PreparedStatement pst = null;
		ResultSet rs = null;
		try{
			String sql = "select * from d_cell_info_lte";
			
			conn = JDBCUtils.getConnection();
			pst = conn.prepareStatement(sql);
			rs = pst.executeQuery();
			
			List<Map<String,Object>> list = new ArrayList<Map<String,Object>>();
			while(rs.next()){
				Map<String,Object> map = new HashMap<String, Object>();
				map.put("id", rs.getObject("id"));
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
	
	public int update(List<String> list, List<String> value){
		Connection conn = null;
		PreparedStatement pst = null;
		try{
			conn = JDBCUtils.getConnection();
			String sql = "";
			
			if(list!=null && value!=null && list.size()==value.size()){
				String firstHalf = "update d_device_info set  (" ;
				String secondHalf = " values ('";
				for(int i=0; i<list.size()-1; i++){
					firstHalf += list.get(i) +",";
					secondHalf += value.get(i) +"','";
				}
				firstHalf += list.get(list.size()-1)+")";
				secondHalf += value.get(list.size()-1) +"')";
				sql = firstHalf + secondHalf;
			}
			
//			sql = "insert into d_device_info (" +
//			"testdate,"+"IPaddress,"+"prottype,"+"upthread,"+"downthread,"+
//			"upspeed,"+ "downspeed," +"upspeedavg,"+"downspeedavg"+
//					") values ('"+list.get(0)+"','"+list.get(1)+"','"+list.get(2)+"','"+list.get(3)+"','"+list.get(4)+"','"+
//					list.get(5)+"','"+list.get(6)+"','"+list.get(7)+"','"+list.get(8)+
//					"')";
			pst = conn.prepareStatement(sql);
			
		return	pst.executeUpdate();
			
		} catch(Exception e){
			throw new RuntimeException(e);
		} finally{
			JDBCUtils.release(null, pst, null);
		}
	}
	
	static public void main(String[]args){
		InsertDaoTest idt = new InsertDaoTest();
		List<Map<String,Object>> values = idt.findAll();
		Connection conn = null;
		PreparedStatement pst = null;
		try{
			conn = JDBCUtils.getConnection();
			for(int i=0; i<values.size(); i++){
				String sql = "update d_cell_info_lte set bs_id = '"+UUID.randomUUID().toString().replaceAll("-", "")+"' where id="+values.get(i).get("id")+";";
				pst = conn.prepareStatement(sql);
				pst.executeUpdate();
			}
		} catch(Exception e){
			throw new RuntimeException(e);
		} finally{
			JDBCUtils.release(null, pst, null);
		}
	}
}
