package redis;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import net.sf.json.JSONArray;

import org.json.JSONObject;


import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class JedisCilent {

	public static JedisPool pool;
	public static Jedis resource;
 static {
	try {
		 pool = JedisPoolManage.getPool();
	} catch (Exception e) {
		e.printStackTrace();
	}
 }
 
  public static String insertObj(String sessionid,int timeout,Serializable  obj){
	  	resource = pool.getResource();
	  	 String setex = resource.setex(sessionid.getBytes(), timeout, serialize(obj));
	  	 pool.returnResource(resource);
	  	 return setex ;
  }
  
  public static Object getObj(String sessionid){
	  if(sessionid==null||"".equals(sessionid)){
		  return null;
	  }
	  resource = pool.getResource();
	  byte[] setex = resource.get(sessionid.getBytes());
	  pool.returnResource(resource);
	  Object  obj = unserialize(setex);
	  return obj ;
  }
  
  public static Long delObj(String sessionid){
	  	 resource = pool.getResource();
	  	 Long setex = resource.del(sessionid.getBytes());
	  	pool.returnResource(resource);
	  	 return setex ;
}
  
  public static Object unserialize(byte[] bytes) {
      ByteArrayInputStream bais = null;
      try {
          // 反序列化
          bais = new ByteArrayInputStream(bytes);
          ObjectInputStream ois = new ObjectInputStream(bais);
          return ois.readObject();
      } catch (Exception e) {

      }
      return null;
  }

  public static byte[] serialize(Object object) {
      ObjectOutputStream oos = null;
      ByteArrayOutputStream baos = null;
      try {
          // 序列化
          baos = new ByteArrayOutputStream();
          oos = new ObjectOutputStream(baos);
          oos.writeObject(object);
          byte[] bytes = baos.toByteArray();
          return bytes;
      } catch (Exception e) {

      }
      return null;
  }
  //处理数据
  public static void main(String[] args){
	  String str=ReadFile("D:/Users/me/Workspaces/xyl/ctp/WebRoot/Distribution_MAP_all_201401_LTE_Report.json");
	  JSONArray json= (JSONArray) JSONArray.fromObject(str);
	  JSONArray arr;
	  for(int i=0;i<json.size();i++){
		   arr=(JSONArray) json.get(i);
		    if(Double.parseDouble(arr.get(0).toString())<114&&Double.parseDouble(arr.get(1).toString())>46){
			   json.remove(i);  
		    }
			if(Double.parseDouble(arr.get(0).toString())>130&&Double.parseDouble(arr.get(1).toString())<43){
				   json.remove(i);  
			}
			if(Double.parseDouble(arr.get(0).toString())>129&&Double.parseDouble(arr.get(1).toString())<38){
				   json.remove(i);  
			}
			if(Double.parseDouble(arr.get(0).toString())>122&&Double.parseDouble(arr.get(1).toString())<36){
				   json.remove(i);  
			}
			if(Double.parseDouble(arr.get(0).toString())<107.4&&Double.parseDouble(arr.get(1).toString())<20){
				   json.remove(i);  
			}
			if(Double.parseDouble(arr.get(0).toString())>121.9&&Double.parseDouble(arr.get(1).toString())<31){
				   json.remove(i);  
			}
	  }
	        System.out.print(json.toString());
  }
  //读流
  public static  String ReadFile(String Path){
		  BufferedReader reader = null;
		  String laststr = "";
		  try{
		  FileInputStream fileInputStream = new FileInputStream(Path);
		  InputStreamReader inputStreamReader = new InputStreamReader(fileInputStream, "UTF-8");
		  reader = new BufferedReader(inputStreamReader);
		  String tempString = null;
		  while((tempString = reader.readLine()) != null){
		  laststr += tempString;
		  }
		  reader.close();
		  }catch(IOException e){
		  e.printStackTrace();
		  }finally{
		  if(reader != null){
		  try {
		  reader.close();
		  } catch (IOException e) {
		  e.printStackTrace();
		  }
		  }
		  }
		  return laststr;
		  }
	  
}
