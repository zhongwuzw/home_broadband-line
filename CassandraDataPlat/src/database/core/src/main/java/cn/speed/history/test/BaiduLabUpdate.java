package cn.speed.history.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.Charset;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.params.ConnRoutePNames;
import org.apache.http.entity.mime.MultipartEntity;
import org.apache.http.entity.mime.content.FileBody;
import org.apache.http.entity.mime.content.StringBody;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.params.HttpParams;

import net.sf.json.JSONObject;

public class BaiduLabUpdate {

	public int counts = 0;
	public String createGeotable(String ak, String name, String geoType, String is_published){
		
		String geotableId = "";
		String status = "";
		String message = "";
		InputStream fis = null;
		BufferedReader br = null;
		try {
			String url = "http://api.map.baidu.com/geodata/v2/geotable/create";
			
			HttpClient client = new DefaultHttpClient();
			
			//设置代理服务器的ip地址和端口  
			HttpHost proxy = new HttpHost("218.206.179.20", 3128);  
			client.getParams().setParameter(ConnRoutePNames.DEFAULT_PROXY, proxy);
			
			//实例化验证   
	        CredentialsProvider credsProvider = new BasicCredentialsProvider();   
	        //设定验证内容   
	        UsernamePasswordCredentials creds = new UsernamePasswordCredentials("liugang", "liugang");   
	        //创建验证   
	        credsProvider.setCredentials(new AuthScope(AuthScope.ANY_HOST, AuthScope.ANY_PORT), creds);   
	        ((DefaultHttpClient)client).setCredentialsProvider(credsProvider);  

			//HttpConnectionParams.setConnectionTimeout(client.getParams(),
			//		1500);
			//HttpConnectionParams.setSoTimeout(client.getParams(), 2000);
			HttpPost httppost = new HttpPost();
			httppost.setURI(new URI(url));
			
			MultipartEntity entity = new MultipartEntity();   
			entity.addPart("ak", new StringBody(ak, Charset.forName("UTF-8")));   
			entity.addPart("name", new StringBody(name, Charset.forName("UTF-8")));   
			//entity.addPart("geotype", new FileBody(new File("C:\\1.txt"))); 
			entity.addPart("geotype", new StringBody(geoType, Charset.forName("UTF-8")));   
			entity.addPart("is_published", new StringBody(is_published, Charset.forName("UTF-8")));   
			
			//httppost.setHeader("Content-Type", "multipart/form-data; boundary=----WebKitFormBoundarywrIcHKEXuuejqcoH");
			
			httppost.setEntity(entity);
			HttpResponse responseFir = client.execute(httppost);

			fis = responseFir.getEntity().getContent();
			br = new BufferedReader(new InputStreamReader(fis, "UTF-8"));
			String line = null;
			String result="";
			while ((line = br.readLine()) != null) {
				result+=line;
			}
			System.out.println("result-------"+result);
			JSONObject jsonObject = JSONObject.fromObject(result);
//			if(!"0".equals(jsonObject.getString("error"))){
//				return null;
//			}
			status = String.valueOf(jsonObject.getInt("status"));
			message = URLDecoder.decode(jsonObject.getString("message"), "utf-8");
			System.out.println(message);
			if(status.equals("0")){
				geotableId=String.valueOf((int)jsonObject.getDouble("id"));
			}else{
				geotableId=null;
			}
			
			
		} catch (Exception e) {
			geotableId=null;
			e.printStackTrace();
		}finally{
			try {
				fis.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return geotableId;
	}
	
	public String uploadGeotable(String ak, String geotable_id, String poi_list){
		
		String geotableId = "";
		String status = "";
		String message = "";
		InputStream fis = null;
		BufferedReader br = null;
		try {
			String url = "http://api.map.baidu.com/geodata/v2/poi/upload";
			
			HttpClient client = new DefaultHttpClient();
			
			//设置代理服务器的ip地址和端口  
			HttpHost proxy = new HttpHost("218.206.179.20", 3128);  
			client.getParams().setParameter(ConnRoutePNames.DEFAULT_PROXY, proxy);
			
			//实例化验证   
	        CredentialsProvider credsProvider = new BasicCredentialsProvider();   
	        //设定验证内容   
	        UsernamePasswordCredentials creds = new UsernamePasswordCredentials("liugang", "liugang");   
	        //创建验证   
	        credsProvider.setCredentials(new AuthScope(AuthScope.ANY_HOST, AuthScope.ANY_PORT), creds);   
	        ((DefaultHttpClient)client).setCredentialsProvider(credsProvider);  

			//HttpConnectionParams.setConnectionTimeout(client.getParams(),
			//		1500);
			//HttpConnectionParams.setSoTimeout(client.getParams(), 2000);
			HttpPost httppost = new HttpPost();
			httppost.setURI(new URI(url));
			
			MultipartEntity entity = new MultipartEntity();   
			entity.addPart("ak", new StringBody(ak, Charset.forName("UTF-8")));   
			entity.addPart("geotable_id", new StringBody(geotable_id, Charset.forName("UTF-8")));   
			entity.addPart("poi_list", new FileBody(new File(poi_list))); 
			
			httppost.setEntity(entity);
			HttpResponse responseFir = client.execute(httppost);

			fis = responseFir.getEntity().getContent();
			br = new BufferedReader(new InputStreamReader(fis, "UTF-8"));
			String line = null;
			String result="";
			while ((line = br.readLine()) != null) {
				result+=line;
			}
			System.out.println("result-------"+result);
			JSONObject jsonObject = JSONObject.fromObject(result);
//			if(!"0".equals(jsonObject.getString("error"))){
//				return null;
//			}
			status = String.valueOf(jsonObject.getInt("status"));
			message = URLDecoder.decode(jsonObject.getString("message"), "utf-8");
			System.out.println(message);
			if(status.equals("0")){
				geotableId=String.valueOf(jsonObject.getDouble("id"));
			}else{
				geotableId=null;
			}
			
			
		} catch (Exception e) {
			geotableId=null;
			e.printStackTrace();
		}finally{
			try {
				fis.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return geotableId;
	}

	public String insertGeotable(String ak, String geotable_id, String title, String address,String latitude, String longitude, String coord_type, String tags){
		
		String geotableId = "";
		String status = "";
		String message = "";
		InputStream fis = null;
		BufferedReader br = null;
		try {
			String url = "http://api.map.baidu.com/geodata/v2/poi/create";
			
			HttpClient client = new DefaultHttpClient();
			
			//设置代理服务器的ip地址和端口  
			HttpHost proxy = new HttpHost("218.206.179.20", 3128);  
			client.getParams().setParameter(ConnRoutePNames.DEFAULT_PROXY, proxy);
			
			//实例化验证   
	        CredentialsProvider credsProvider = new BasicCredentialsProvider();   
	        //设定验证内容   
	        UsernamePasswordCredentials creds = new UsernamePasswordCredentials("liugang", "liugang");   
	        //创建验证   
	        credsProvider.setCredentials(new AuthScope(AuthScope.ANY_HOST, AuthScope.ANY_PORT), creds);   
	        ((DefaultHttpClient)client).setCredentialsProvider(credsProvider);  

			//HttpConnectionParams.setConnectionTimeout(client.getParams(),
			//		1500);
			//HttpConnectionParams.setSoTimeout(client.getParams(), 2000);
			HttpPost httppost = new HttpPost();
			httppost.setURI(new URI(url));
			
			MultipartEntity entity = new MultipartEntity();   
			entity.addPart("ak", new StringBody(ak, Charset.forName("UTF-8")));   
			entity.addPart("geotable_id", new StringBody(geotable_id, Charset.forName("UTF-8")));   
			entity.addPart("title", new StringBody(title, Charset.forName("UTF-8"))); 
			entity.addPart("address", new StringBody(address, Charset.forName("UTF-8"))); 
			entity.addPart("latitude", new StringBody(latitude, Charset.forName("UTF-8"))); 
			entity.addPart("longitude", new StringBody(longitude, Charset.forName("UTF-8"))); 
			entity.addPart("coord_type", new StringBody(coord_type, Charset.forName("UTF-8"))); 
			entity.addPart("tags", new StringBody(tags, Charset.forName("UTF-8"))); 
			
			httppost.setEntity(entity);
			HttpResponse responseFir = client.execute(httppost);

			fis = responseFir.getEntity().getContent();
			br = new BufferedReader(new InputStreamReader(fis, "UTF-8"));
			String line = null;
			String result="";
			while ((line = br.readLine()) != null) {
				result+=line;
			}
			System.out.println("result-------"+result);
			JSONObject jsonObject = JSONObject.fromObject(result);
//			if(!"0".equals(jsonObject.getString("error"))){
//				return null;
//			}
			status = String.valueOf(jsonObject.getInt("status"));
			message = URLDecoder.decode(jsonObject.getString("message"), "utf-8");
			System.out.println(message);
			if(status.equals("0")){
				geotableId=String.valueOf(jsonObject.getDouble("id"));
			}else{
				geotableId=null;
			}
			
			
		} catch (Exception e) {
			geotableId=null;
			e.printStackTrace();
		}finally{
			try {
				fis.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return status;
	}
	
	
	
	public String getJobListStatus(String ak){
		
		String result = "";
		InputStream fis = null;
		BufferedReader br = null;
		try {
			String url = "http://api.map.baidu.com/geodata/v2/job/list?ak=9f6884e6a9e9ebb8c5f921658f2b6355";
			
			HttpClient client = new DefaultHttpClient();
			
			//设置代理服务器的ip地址和端口  
			HttpHost proxy = new HttpHost("218.206.179.20", 3128);  
			client.getParams().setParameter(ConnRoutePNames.DEFAULT_PROXY, proxy);
			
			//实例化验证   
	        CredentialsProvider credsProvider = new BasicCredentialsProvider();   
	        //设定验证内容   
	        UsernamePasswordCredentials creds = new UsernamePasswordCredentials("liugang", "liugang");   
	        //创建验证   
	        credsProvider.setCredentials(new AuthScope(AuthScope.ANY_HOST, AuthScope.ANY_PORT), creds);   
	        ((DefaultHttpClient)client).setCredentialsProvider(credsProvider);  

			//HttpConnectionParams.setConnectionTimeout(client.getParams(),
			//		1500);
			//HttpConnectionParams.setSoTimeout(client.getParams(), 2000);
			HttpGet httpget = new HttpGet();
			httpget.setURI(new URI(url));
			httpget.setParams(client.getParams().setParameter("ak", ak));
			HttpResponse responseFir = client.execute(httpget);

			fis = responseFir.getEntity().getContent();
			br = new BufferedReader(new InputStreamReader(fis, "UTF-8"));
			String line = null;
			while ((line = br.readLine()) != null) {
				result+=line;
			}
			System.out.println("result-------"+result);
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			try {
				fis.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return result;
	}
	
	public String deleteGeotable(String ak, String geotable_id, String id){
		
		String geotableId = "";
		String status = "";
		String message = "";
		InputStream fis = null;
		BufferedReader br = null;
		try {
			String url = "http://api.map.baidu.com/geodata/v2/poi/delete";
			
			HttpClient client = new DefaultHttpClient();
			
			//设置代理服务器的ip地址和端口  
			HttpHost proxy = new HttpHost("proxy.cmcc", 8080);  
			client.getParams().setParameter(ConnRoutePNames.DEFAULT_PROXY, proxy);
			
			/*
			//实例化验证   
	        CredentialsProvider credsProvider = new BasicCredentialsProvider();   
	        //设定验证内容   
	        UsernamePasswordCredentials creds = new UsernamePasswordCredentials("liugang", "liugang");   
	        //创建验证   
	        credsProvider.setCredentials(new AuthScope(AuthScope.ANY_HOST, AuthScope.ANY_PORT), creds);   
	        ((DefaultHttpClient)client).setCredentialsProvider(credsProvider);  
			*/
			
			//HttpConnectionParams.setConnectionTimeout(client.getParams(),
			//		1500);
			//HttpConnectionParams.setSoTimeout(client.getParams(), 2000);
			HttpPost httppost = new HttpPost();
			httppost.setURI(new URI(url));
			
			MultipartEntity entity = new MultipartEntity();   
			entity.addPart("ak", new StringBody(ak, Charset.forName("UTF-8")));   
			entity.addPart("geotable_id", new StringBody(geotable_id, Charset.forName("UTF-8")));   
			entity.addPart("id", new StringBody(id, Charset.forName("UTF-8"))); 
			
			httppost.setEntity(entity);
			HttpResponse responseFir = client.execute(httppost);

			fis = responseFir.getEntity().getContent();
			br = new BufferedReader(new InputStreamReader(fis, "UTF-8"));
			String line = null;
			String result="";
			while ((line = br.readLine()) != null) {
				result+=line;
			}
//			System.out.println("result-------"+result);
			JSONObject jsonObject = JSONObject.fromObject(result);
//			if(!"0".equals(jsonObject.getString("error"))){
//				return null;
//			}
			status = String.valueOf(jsonObject.getInt("status"));
			message = URLDecoder.decode(jsonObject.getString("message"), "utf-8");
//			System.out.println(message);
			if(status.equals("0")){
				counts++;
//				System.out.println("status::   "+status);
			}
			
			
		} catch (Exception e) {
			geotableId=null;
			e.printStackTrace();
		}finally{
			try {
				fis.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return status;
	}
	
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		BaiduLabUpdate blu = new BaiduLabUpdate();
		String ak = "9f6884e6a9e9ebb8c5f921658f2b6355";
//		String geotableId = blu.createGeotable(ak, "GuangZhou", "1", "1");
		String geotableId = "39212";
		
		int id = 18300;
		while(id<30000000){
			id++;
			try{
				blu.deleteGeotable(ak, geotableId, String.valueOf(id));
			} catch (Exception e) {
				System.out.println("Failed id:: "+ id);
				e.printStackTrace();
			}
			
			if(blu.counts%100 == 0){
				System.out.println("blu.counts::  "+ blu.counts);
			}
		}
		
		/*
//		//String geotableId = blu.createGeotable("9f6884e6a9e9ebb8c5f921658f2b6355", "39197", "1", "1");
		if(geotableId!=null && !geotableId.equals("")){
//			System.out.println("geotableId---------"+geotableId);
//			blu.uploadGeotable("9f6884e6a9e9ebb8c5f921658f2b6355", geotableId, "C:\\Users\\cmcc\\Desktop\\a-1.csv");
			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			String startTime = dateFormat.format(new Date());
			int successCount = 0;
			successCount = blu.analyzeFile(ak, geotableId, "C:\\d_cell_info_lte.txt");
			String endTime = dateFormat.format(new Date());
			System.out.print("startTime----------"+startTime);
			System.out.print("endTime----------"+endTime);

			System.out.print("successCount----------"+successCount);
		}
//		blu.getJobListStatus("9f6884e6a9e9ebb8c5f921658f2b6355");
		*/
		
		/*
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		
		Date date2 = new Date();
		date2.setTime(Long.parseLong("1380118790000"));
		System.out.println(dateFormat.format(date2));
		
		
		Date date1 = new Date();
		try {
			date1 = dateFormat.parse("2013-9-25 20:46:00");
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println(date1.getTime());
		 * 
 */
	}
	
	public int analyzeFile(String ak, String geotable_id, String filePath){
		BaiduLabUpdate blu = new BaiduLabUpdate();
		int successCount = 0;
		File dataFile = new File(filePath);
		BufferedReader myBR = null;
		ArrayList<String> faildInsert = new ArrayList<String>();
		try {
			myBR = new BufferedReader(new InputStreamReader(new FileInputStream(dataFile), "utf-8"));
			String line = "";
			while((line = myBR.readLine())!=null){
				String[] paramsF = line.split(",");
				if(paramsF.length == 3){
					String status = blu.insertGeotable(ak, geotable_id, paramsF[0], "", paramsF[1], paramsF[2], "1", "GZ");
					if(status.equals("0")){
						successCount++;
					}else{
						faildInsert.add(paramsF[0]);
					}
				}
			}
			
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			try {
				myBR.close();
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			System.out.print("Failed insert:");
			for(int i=0; i<faildInsert.size(); i++){
				if(i%10 == 0){
					System.out.println("");
				}
				System.out.print(faildInsert.get(i)+"---");
			}
		}
		return successCount;
	}

}
