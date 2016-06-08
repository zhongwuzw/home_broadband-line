package com.chinamobile.httpserver.data;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class OttMessageQueue {

	private static OttMessageQueue instance = null;
	private static Queue<byte[]> ue_msg=new ConcurrentLinkedQueue<byte[]>();  
	private static Queue<byte[]> car_msg=new ConcurrentLinkedQueue<byte[]>(); 
	
	/**
	 * *_QUEUE_CAPACITY is used to control the size of the MSG_QUEUE.
	 */
	private static int UE_QUEUE_CAPACITY = 10000;
	private static int CAR_QUEUE_CAPACITY = 10000;
	private static int ue_msg_num = 0;
	private static int car_msg_num = 0;

	static String default_log_file = "res/log/ottreceivelog.txt";
	static String default_car_log_file = "res/log/ottreceivecarlog.txt";
	static String default_ue_log_file = "res/log/ottreceiveuelog.txt";
	
	public enum QUEUENUM{}
	
	String log_file;
	String car_log_file;
	String ue_log_file; 

	public static OttMessageQueue getInstance() {
		if(instance == null){
			return new OttMessageQueue();
		}
        return instance;
    }
	
	private OttMessageQueue() {}

	public static int getUe_msg_num() {
		return ue_msg_num;
	}

	public static int getCar_msg_num() {
		return car_msg_num;
	}

	public boolean offer(byte[] message, String element){
		this.toLogFile(message);
		if(element.toLowerCase().trim() == "gt-9100"){
			this.toLogFile(message,element);
			OttMessageQueue.ue_msg_num++;
			if(ue_msg_num>OttMessageQueue.UE_QUEUE_CAPACITY){
				this.poll("gt-9100");
			}
			return ue_msg.offer(message);
		}
		if(element.toLowerCase().trim() == "gt-9200"){
			this.toLogFile(message,element);
			OttMessageQueue.car_msg_num++;
			if(car_msg_num>OttMessageQueue.CAR_QUEUE_CAPACITY){
				this.poll("gt-9200");
			}
			return car_msg.offer(message);
		}
		return false;
	}
	
	public byte[] poll(String element){
		if(element.toLowerCase().trim() == "gt-9100"){
			synchronized(ue_msg) {
				if(!ue_msg.isEmpty()) {  
					return ue_msg.poll();
				}  
			}
		}
		if(element.toLowerCase().trim() == "gt-9200"){
			synchronized(car_msg) {
				if(!car_msg.isEmpty()) {  
					return car_msg.poll();
				}  
			}
		}
		return null;
	}
	
	public byte[] peek(String element){
		if(element.toLowerCase().trim() == "gt-9100"){
			synchronized(ue_msg) {
				if(!ue_msg.isEmpty()) {  
					return ue_msg.peek();
				}  
			}
		}
		if(element.toLowerCase().trim() == "gt-9200"){
			synchronized(car_msg) {
				if(!car_msg.isEmpty()) {  
					return car_msg.peek();
				}  
			}
		}
		return null;
	}
	
	public byte[] get(String element){
		if(element.toLowerCase().trim() == "gt-9100"){
			synchronized(ue_msg) {
				if(!ue_msg.isEmpty()) {  
					return ue_msg.peek();
				}  
			}
		}
		if(element.toLowerCase().trim() == "gt-9200"){
			synchronized(car_msg) {
				if(!car_msg.isEmpty()) {  
					return car_msg.peek();
				}  
			}
		}
		return null;
	}
	
	public boolean removeAll(){
		return ue_msg.removeAll(ue_msg) && car_msg.removeAll(car_msg);
	}
	
	public String getLog_file() {
		return log_file;
	}

	public void setLog_file(String log_file) {
		this.log_file = log_file;
	}
	
	private void toLogFile(byte[] message){
		toLogFile(message,"");
	}
	
	private void toLogFile(byte[] message, String element){
		String filePath = null;
		if(element.toLowerCase().trim() == "gt-9100"){
			if(ue_log_file == null){
				ue_log_file = default_ue_log_file;
			}
			filePath = ue_log_file;
		} else if(element.toLowerCase().trim() == "gt-9200"){
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
				
		try {
            // 打开一个随机访问文件流，按读写方式
            RandomAccessFile randomFile = new RandomAccessFile(filePath, "rw");
            // 文件长度，字节数
            long fileLength = randomFile.length();
            //将写文件指针移到文件尾。
            randomFile.seek(fileLength);
            randomFile.write(message);
            randomFile.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
	}
	
}
