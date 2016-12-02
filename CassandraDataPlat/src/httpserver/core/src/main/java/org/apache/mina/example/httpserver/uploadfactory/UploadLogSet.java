package org.apache.mina.example.httpserver.uploadfactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import com.chinamobile.httpserver.data.DataParser;

public class UploadLogSet {
	static private List<Map<String,Object>> uploadLogSet = new ArrayList<Map<String,Object>>();
	
	static public List<Map<String,Object>> getUploadCode() {
		return uploadLogSet;
	}

	static public void setUploadCode(List<Map<String,Object>> uploadCode) {
		uploadLogSet = uploadCode;
	}
	
	static public void add(Map<String,Object> uploadValue){
		uploadLogSet.add(uploadValue);
	}
	
	static public boolean remove(String uploadCode){
		uploadLogSet.remove(uploadCode);
		return true;
	}
	
	static public Map<String,Object> get(int index){
		return uploadLogSet.get(index);
	}
}
