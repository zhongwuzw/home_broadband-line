package cn.speed.history.service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.mina.example.httpserver.uploadfactory.UploadLogSet;

import cn.speed.history.service.ResultUploadService;

public class UploadFiletInsert extends Thread{
	Map<String,Object> list2insert = new HashMap<String,Object>();
	public UploadFiletInsert(Map<String,Object> list2insert){
		this.list2insert = list2insert;
	}
	public void run(){		
		try{
			if(list2insert!=null){
				ResultUploadService service = new ResultUploadService();
				service.insert(this.list2insert);
			}
		}catch(Exception e){
			e.printStackTrace();
			UploadLogSet.add(list2insert);
		}
	}
}
