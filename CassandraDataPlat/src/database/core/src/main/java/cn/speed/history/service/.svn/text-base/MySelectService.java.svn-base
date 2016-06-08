package cn.speed.history.service;

import java.util.List;
import java.util.Map;

import cn.speed.history.dao.MySelectDao;
import cn.speed.jdbc.utils.JDBCUtils;

public class MySelectService {
	MySelectDao dao = new MySelectDao();
	public List<Map<String, Object>> test(){
		try{
			JDBCUtils.startTransaction();
			
			List<Map<String,Object>> list = dao.test();
			
			JDBCUtils.commit();
			
			return list;
			
		} catch(Exception e){
			JDBCUtils.rollback();
			throw new RuntimeException(e);
		} finally{
			JDBCUtils.close();
		}
	}
	
	public int testUpdate(List<String> list){
		try{
			JDBCUtils.startTransaction();
			
			int num= dao.testUpdate(list);
			
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
