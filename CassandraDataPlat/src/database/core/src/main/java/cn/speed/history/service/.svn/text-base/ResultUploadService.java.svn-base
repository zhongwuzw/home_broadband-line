package cn.speed.history.service;

import java.util.List;
import java.util.Map;

import cn.speed.history.dao.DeviceInfoDao;
import cn.speed.history.dao.MySelectDao;
import cn.speed.history.dao.ResultUploadDao;
import cn.speed.jdbc.utils.JDBCUtils;

public class ResultUploadService {
	ResultUploadDao dao = new ResultUploadDao();
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
	
	public int insert(Map<String,Object> list){
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
	public List<Map<String,Object>> getStore_path(String device_imei){
		try{
			JDBCUtils.startTransaction();
			List<Map<String,Object>> list = dao.getStore_path(device_imei);
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
