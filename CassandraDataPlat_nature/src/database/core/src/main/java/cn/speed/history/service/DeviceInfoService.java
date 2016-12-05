package cn.speed.history.service;

import java.util.List;
import java.util.Map;

import cn.speed.history.dao.DeviceInfoDao;
import cn.speed.history.dao.MySelectDao;
import cn.speed.jdbc.utils.JDBCUtils;

public class DeviceInfoService {
	DeviceInfoDao dao = new DeviceInfoDao();
	public List<Map<String, Object>> test(){
		try{
			JDBCUtils.startTransaction();
			
			List<Map<String,Object>> list = dao.findAll();
			
			JDBCUtils.commit();
			
			return list;
			
		} catch(Exception e){
			JDBCUtils.rollback();
			throw new RuntimeException(e);
		} finally{
			JDBCUtils.close();
		}
	}
	
	public int insert(List<String> list){
		try{
			JDBCUtils.startTransaction();
			
			int num= dao.insert(list);
			
			JDBCUtils.commit();
			return num;
			
			
		} catch(Exception e){
			JDBCUtils.rollback();
			throw new RuntimeException(e);
		} finally{
			JDBCUtils.close();
		}
	}
}
