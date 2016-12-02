package com.chinamobile.httpserver.data;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.ArrayList;

public class TestMessageBean {
	
	static int default_uid = -1;
	static float default_lng = (float)116.424374;
	static float default_lat = (float)39.914668;
	static String default_contents = "No Content";
	static String default_log_file = "d:/receivelog.txt";
	
	static TestMessageBean instance = null;
	static ArrayList<TestMessageBean> allInstance = null;
	
	int uid;
	float lng;
	float lat;
	String contents;
	
	String log_file;

	public synchronized static TestMessageBean getInstance(int uid){
		if(allInstance != null){
			for(int i=0; i<allInstance.size(); i++){
				TestMessageBean temp = allInstance.get(i);
				if(temp.getUid() == uid){
					return temp;
				}
			}
			TestMessageBean newInstance0 = new TestMessageBean(uid);
			allInstance.add(newInstance0);
			return newInstance0;
		}
		allInstance = new ArrayList<TestMessageBean>();
		TestMessageBean newInstance1 = new TestMessageBean(uid);
		allInstance.add(newInstance1);
		return newInstance1;
	}
	
	public synchronized static ArrayList<TestMessageBean> getAllInstance(){
		return allInstance;
	}
	
	private TestMessageBean(){}
	
	private TestMessageBean(int uid){
		this.uid = uid;
	}
	
	public synchronized void setInstance(int uid){
		setInstance(uid, default_lng, default_lat, default_contents);
	}
	public synchronized void setInstance(int uid, float lng){
		setInstance(uid, lng, default_lat, default_contents);
	}
	public synchronized void setInstance(int uid, float lng, float lat){
		setInstance(uid, lng, lat, default_contents);
	}
	public synchronized void setInstance(int uid, float lng, float lat, String contents){
		this.setUid(uid);
		this.setLng(lng);
		this.setLat(lat);
		this.setContents(contents);
	}
	
	
	public int getUid() {
		return uid;
	}
	public float getLng() {
		return lng;
	}
	public float getLat() {
		return lat;
	}
	public String getContents() {
		return contents;
	}

	private void setUid(int uid) {
		this.uid = uid;
	}

	private void setLng(float lng) {
		this.lng = lng;
	}

	private void setLat(float lat) {
		this.lat = lat;
	}

	private void setContents(String contents) {
		this.contents = contents;
	}
	
	public String toString(){
		return "UID: "+uid+" LNG: "+lng+" LAT: "+lat+" MSG: "+contents;
	}
	
	public synchronized void writetofile(String line){
		if(log_file == null){
			log_file = default_log_file;
		}
		File log = new File(log_file);		
		BufferedWriter writer = null;
		try{
			writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(log,true)));
			writer.append(line + "   received data");
			writer.flush();
			writer.newLine();
			writer.close();
		} catch (FileNotFoundException e){
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
}
