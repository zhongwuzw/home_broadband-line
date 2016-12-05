package cn.speed.history.service;

import java.util.List;
import java.util.Map;

import cn.speed.history.dao.DeviceSelectDao;
import cn.speed.history.dao.MySelectDao;
import cn.speed.jdbc.utils.JDBCUtils;
import cn.speed.jdbc.utils.SearchValue;

public class DeviceSelectService {
	DeviceSelectDao dao = new DeviceSelectDao();
	
	public List<Map<String, Object>> find(SearchValue sv){
		try{
			JDBCUtils.startTransaction();
			
			List<Map<String,Object>> list = dao.find(sv);
			
			JDBCUtils.commit();
			
			return list;
			
		} catch(Exception e){
			JDBCUtils.rollback();
			throw new RuntimeException(e);
		} finally{
			JDBCUtils.close();
		}
	}
}
