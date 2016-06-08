package org.apache.mina.example.httpserver.uploadfactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import com.chinamobile.httpserver.data.DataParser;

public class UploadCodeSet {
	static private HashMap<String, Map<String,Object>> uploadCodeSet = new HashMap<String, Map<String,Object>>();

	static{
		Map<String,Object> list2insert = new HashMap<String,Object>();
		list2insert.put("store_path","./OTS/");
		uploadCodeSet.put("uploadfile",list2insert);
	}
	
	static public HashMap<String, Map<String,Object>> getUploadCode() {
		return uploadCodeSet;
	}

	static public void setUploadCode(HashMap<String, Map<String,Object>> uploadCode) {
		uploadCodeSet = uploadCode;
	}
	
	static public boolean contains(String uploadCode){
		return uploadCodeSet.containsKey(uploadCode);		
	}
	
	static public void add(String uploadCode,Map<String,Object> uploadValue){
		uploadCodeSet.put(uploadCode,uploadValue);
	}
	
	static public boolean remove(String uploadCode){
		uploadCodeSet.remove(uploadCode);
		return true;
	}
	
	static public Map<String,Object> get(String uploadCode){
		return uploadCodeSet.get(uploadCode);
	}
}
