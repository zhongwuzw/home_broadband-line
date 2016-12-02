package cn.http.history.process.impl;

import java.util.List;
import java.util.Map;

import org.apache.mina.example.httpserver.uploadfactory.UploadLogSet;

import cn.speed.history.service.ResultUploadService;

public class HttpFiletInsert extends Thread{
	public HttpFiletInsert(){
		
	}
	public void run(){		
		while(true){
			List<Map<String,Object>> uploadLogSet = UploadLogSet.getUploadCode();
			for(int i=0; i<uploadLogSet.size(); i++){
				if(uploadLogSet.get(i)!=null){
					try{
						ResultUploadService service = new ResultUploadService();
						service.insert(uploadLogSet.get(i));
						uploadLogSet.remove(i);
					}catch(Exception e){
						e.printStackTrace();
					}
				}
			}
			try {
				Thread.sleep(1800000);//1800000
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
