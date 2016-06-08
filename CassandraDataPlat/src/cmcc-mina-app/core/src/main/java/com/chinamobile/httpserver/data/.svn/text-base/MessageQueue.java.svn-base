package com.chinamobile.httpserver.data;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class MessageQueue {

	private static MessageQueue instance = null;
	private static Queue<String> ue_msg=new ConcurrentLinkedQueue<String>();  
	private static Queue<String> car_msg=new ConcurrentLinkedQueue<String>(); 
	
	/**
	 * *_QUEUE_CAPACITY is used to control the size of the MSG_QUEUE.
	 */
	private static int UE_QUEUE_CAPACITY = 10000;
	private static int CAR_QUEUE_CAPACITY = 10000;
	private static int ue_msg_num = 0;
	private static int car_msg_num = 0;

	static String default_log_file = "res/log/receivelog.txt";
	static String default_car_log_file = "res/log/receivecarlog.txt";
	static String default_ue_log_file = "res/log/receiveuelog.txt";
	
	public enum QUEUENUM{}
	
	String log_file;
	String car_log_file;
	String ue_log_file; 

	public static MessageQueue getInstance() {
		if(instance == null){
			return new MessageQueue();
		}
        return instance;
    }
	
	private MessageQueue() {}

	public static int getUe_msg_num() {
		return ue_msg_num;
	}

	public static int getCar_msg_num() {
		return car_msg_num;
	}

	public boolean offer(String message, String element){
		this.toLogFile(message);
		if(element.toLowerCase().trim() == "ue"){
			this.toLogFile(message,element);
			MessageQueue.ue_msg_num++;
			if(ue_msg_num>MessageQueue.UE_QUEUE_CAPACITY){
				this.poll("ue");
			}
			return ue_msg.offer(message);
		}
		if(element.toLowerCase().trim() == "car"){
			this.toLogFile(message,element);
			MessageQueue.car_msg_num++;
			if(car_msg_num>MessageQueue.CAR_QUEUE_CAPACITY){
				this.poll("car");
			}
			return car_msg.offer(message);
		}
		return false;
	}
	
	public String poll(String element){
		if(element.toLowerCase().trim() == "ue"){
			synchronized(ue_msg) {
				if(!ue_msg.isEmpty()) {  
					return ue_msg.poll();
				}  
			}
		}
		if(element.toLowerCase().trim() == "car"){
			synchronized(car_msg) {
				if(!car_msg.isEmpty()) {  
					return car_msg.poll();
				}  
			}
		}
		return "";
	}
	
	public String peek(String element){
		if(element.toLowerCase().trim() == "ue"){
			synchronized(ue_msg) {
				if(!ue_msg.isEmpty()) {  
					return ue_msg.peek();
				}  
			}
		}
		if(element.toLowerCase().trim() == "car"){
			synchronized(car_msg) {
				if(!car_msg.isEmpty()) {  
					return car_msg.peek();
				}  
			}
		}
		return "";
	}
	
	public String get(String element){
		if(element.toLowerCase().trim() == "ue"){
			synchronized(ue_msg) {
				if(!ue_msg.isEmpty()) {  
					return ue_msg.peek();
				}  
			}
		}
		if(element.toLowerCase().trim() == "car"){
			synchronized(car_msg) {
				if(!car_msg.isEmpty()) {  
					return car_msg.peek();
				}  
			}
		}
		return "";
	}
	
	public boolean removeAll(){
		return this.ue_msg.removeAll(this.ue_msg) && this.car_msg.removeAll(this.car_msg);
	}
	
	public String getLog_file() {
		return log_file;
	}

	public void setLog_file(String log_file) {
		this.log_file = log_file;
	}
	
	private void toLogFile(String message){
		toLogFile(message,"");
	}
	
	private void toLogFile(String message, String element){
		String filePath = null;
		if(element.toLowerCase().trim() == "ue"){
			if(ue_log_file == null){
				ue_log_file = default_ue_log_file;
			}
			filePath = ue_log_file;
		} else if(element.toLowerCase().trim() == "car"){
			if(car_log_file == null){
				car_log_file = default_car_log_file;
			}
			filePath = car_log_file;
		} else {
			if(log_file == null){
				log_file = default_log_file;
			}
			filePath = log_file;
		}
		
		File log = new File(filePath);		
		BufferedWriter writer = null;
		try{
			writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(log,true)));
			writer.append(message);
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
