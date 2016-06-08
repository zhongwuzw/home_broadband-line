package com.opencassandra.resultsettest.separateapp;

import java.io.BufferedReader;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.URL;
import java.net.URLConnection;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import com.opencassandra.descfile.ConfParser;

public class GetPostTest {
	Statement statement;
	Connection conn;
	/**
	 * 向指定URL发送GET方法的请求
	 * 
	 * @param url
	 *            发送请求的URL
	 * @param param
	 *            请求参数，请求参数应该是 name1=value1&name2=value2 的形式。
	 * @return URL 所代表远程资源的响应结果
	 */
	public static String sendGet(String url, String param) {
		String result = "";
		BufferedReader in = null;
		try {
			String urlNameString = url + "?" + param;
			URL realUrl = new URL(urlNameString);
			// 打开和URL之间的连接
			URLConnection connection = realUrl.openConnection();
			// 设置通用的请求属性
			connection.setRequestProperty("accept", "*/*");
			connection.setRequestProperty("connection", "Keep-Alive");
			connection.setRequestProperty("user-agent",
					"Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1;SV1)");
			// 建立实际的连接
			connection.connect();
			// 获取所有响应头字段
			Map<String, List<String>> map = connection.getHeaderFields();
			// 遍历所有的响应头字段
			for (String key : map.keySet()) {
				System.out.println(key + "--->" + map.get(key));
			}
			// 定义 BufferedReader输入流来读取URL的响应
			in = new BufferedReader(new InputStreamReader(connection
					.getInputStream(),"UTF-8"));
			String line;
			while ((line = in.readLine()) != null) {
				result += line;
			}
		} catch (Exception e) {
			System.out.println("发送GET请求出现异常！" + e);
			e.printStackTrace();
		}
		// 使用finally块来关闭输入流
		finally {
			try {
				if (in != null) {
					in.close();
				}
			} catch (Exception e2) {
				e2.printStackTrace();
			}
		}
		return result;
	}

	/**
	 * 向指定 URL 发送POST方法的请求
	 * 
	 * @param url
	 *            发送请求的 URL
	 * @param param
	 *            请求参数，请求参数应该是 name1=value1&name2=value2 的形式。
	 * @return 所代表远程资源的响应结果
	 */
	public static String sendPost(String url, String param) {
		PrintWriter out = null;
		BufferedReader in = null;
		String result = "";
		try {
			URL realUrl = new URL(url);
			// 打开和URL之间的连接
			URLConnection conn = realUrl.openConnection();
			// 设置通用的请求属性
			conn.setRequestProperty("accept", "*/*");
			conn.setRequestProperty("connection", "Keep-Alive");
			conn.setRequestProperty("user-agent",
					"Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1;SV1)");
			// 发送POST请求必须设置如下两行
			conn.setDoOutput(true);
			conn.setDoInput(true);
			// 获取URLConnection对象对应的输出流
			out = new PrintWriter(conn.getOutputStream());
			// 发送请求参数
			out.print(param);
			// flush输出流的缓冲
			out.flush();
			// 定义BufferedReader输入流来读取URL的响应
			in = new BufferedReader(
					new InputStreamReader(conn.getInputStream()));
			String line;
			while ((line = in.readLine()) != null) {
				result += line;
			}
		} catch (Exception e) {
			System.out.println("发送 POST 请求出现异常！" + e);
			e.printStackTrace();
		}
		// 使用finally块来关闭输出流、输入流
		finally {
			try {
				if (out != null) {
					out.close();
				}
				if (in != null) {
					in.close();
				}
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}
		return result;
	}
	/**
	 * 获取所有路径 带有Default的层级结构
	 * @param appidList
	 * @return
	 */
	public String[] getPathsDefault(List appidList){
		ArrayList keyList = new ArrayList();
		if(ConfParser.code==null || ConfParser.code.length<=0){
			return new String[0];
		}
		for (int j = 0; j < ConfParser.code.length; j++) {
			String code1 = ConfParser.code[j];
			String codeStr = "code="+code1;
			String url = ConfParser.getCodePath;
			if(url.isEmpty()){
				return new String[0];
			}
			String sr=GetPostTest.sendGet(url, codeStr);//
	        System.out.println(sr);
	        JSONObject json = JSONObject.fromObject(sr);
	        try {
	        	JSONObject thisOrgInfo = json.getJSONObject("thisOrgInfo");
		        String keySelf = thisOrgInfo.getString("key");
		        String storePathSelf = thisOrgInfo.getString("storepath");
		        if(keySelf==null){
		        	keySelf = "";
		        }
		        if(!keySelf.isEmpty()){
		        	keyList.add(keySelf);
		        }
		        if(storePathSelf!=null && !storePathSelf.isEmpty()){
		        	storePathSelf = storePathSelf.replace("/", File.separator).replace("\\", File.separator);
		        	if(storePathSelf.endsWith("Default")){
		        		//获取读取出来的json中所有的文件路径 CMCC/CMCC_GUANGXI/CMCC_GUANGXI_PINZHIBU23G/CMCC_GUANGXI_PINZHIBU23G_HZ/Default
		        		storePathSelf = storePathSelf.replace(File.separator+"Default", "");
		        	}
		        	keyList.add(storePathSelf);
		        }
			} catch (Exception e) {
				e.printStackTrace();
				continue;
			}
		}
		System.out.println(keyList.get(0)+"   : "+keyList.get(1)+" : "+keyList.get(2)+" : "+keyList.get(3));
		List pathList = new ArrayList<String>();
		String prefix = ConfParser.org_prefix;//获取到配置中的头路径 （如：C:/test/my这为appid的上层路径）
		if((keyList!=null && keyList.size()>0) && (appidList==null || appidList.size()<=0)){
			//只有code的情况
			for (int i = 0; i < keyList.size(); i++) {
				String org_keyString = keyList.get(i).toString();
				if(!org_keyString.isEmpty()){
					//C:/test/my/+appid+/CMCC/CMCC_GUANGXI/CMCC_GUANGXI_PINZHIBU23G/CMCC_GUANGXI_PINZHIBU23G_HZ/Default
					//存放拼接好的文件的绝对路径
					pathList.add(prefix+org_keyString);
				}
			}
		}else if((keyList==null || keyList.size()<=0) && (appidList!=null && appidList.size()>0)){
			//只有appname的情况
			for (int i = 0; i < appidList.size(); i++) {
				String appid = appidList.get(i).toString();
				String floderPath = prefix+appid;
				File floder = new File(floderPath);
				if(floder.exists()){
					String[] files = floder.list();
					for (int j = 0; j < files.length; j++) {
						String filePath = files[j];
						pathList.add(filePath);
					}
				}
			}
		}else{
			//同时配置appname和code情况
			for (int i = 0; i < keyList.size(); i++) {
				String key = keyList.get(i).toString();
				String keyPath = prefix+key;
				File keyFile = new File(keyPath);
				if(keyFile.exists() && keyFile.isDirectory()){
					pathList.add(keyFile.getAbsolutePath());
				}
				for (int j = 0; j < appidList.size(); j++) {
					String appid = appidList.get(j).toString();
					String appFilePath = prefix+appid+File.separator+key;
					File appFile = new File(appFilePath);
					if(appFile.exists()){
						pathList.add(appFile.getAbsolutePath());
					}
				}
			}
		}
		//获取所有含有Default的路径
		List<String> defaultPathList = new ArrayList<String>();
		int num = 0;
		for (int i = 0; i < pathList.size(); i++) {
			String path = pathList.get(i).toString();
			File file = new File(path);
			if(file.exists()){
				File[] sonFolders = file.listFiles(new FilenameFilter(){
		            public boolean accept(File dir, String name){
		            	if(dir.isDirectory()){
		            		return true;
		            	}
		            	return false;
		            }
		        });
				for (int j = 0; j < sonFolders.length; j++) {
					String folderPath = "";
					folderPath = sonFolders[j].toString();
					File folder = new File(folderPath);
					boolean flag = true;
					if(folder.exists() && folder.isDirectory()){
						if(folder.getName().equals("Default")){
							defaultPathList.add(folder.getAbsolutePath());
							continue;
						}
					}
					getSonPath(folder, defaultPathList);
				}
			}
		}
		String paths[] = new String[defaultPathList.size()];
		for (int i = 0; i < defaultPathList.size(); i++) {
			String pathss = defaultPathList.get(i).toString();
			paths[num++] = pathss;
		}
		//C:\test\my\1708467f00836880517dad80e17b1d8c\CMCC\CMCC_GUANGXI\CMCC_GUANGXI_PINZHIBU23G\default
        return paths;//返回所有要解析文件夹的绝对路径
	}
	public String[] getPaths(List appidList,List keyList,String testtime){
		if(ConfParser.code==null || ConfParser.code.length<=0){
			return new String[0];
		}
		List pathList = new ArrayList<String>();
		String prefix = ConfParser.org_prefix;
		if((keyList!=null && keyList.size()>0) && (appidList==null || appidList.size()<=0)){
			//只有code的情况
			for (int i = 0; i < keyList.size(); i++) {
				String org_keyString = keyList.get(i).toString();
				if(!org_keyString.isEmpty()){
					pathList.add(prefix+org_keyString);
				}
			}
		}else if((keyList==null || keyList.size()<=0) && (appidList!=null && appidList.size()>0)){
			//只有appname的情况
			for (int i = 0; i < appidList.size(); i++) {
				String appid = appidList.get(i).toString();
				String floderPath = prefix+appid;
				File floder = new File(floderPath);
				if(floder.exists()){
					String[] files = floder.list();
					for (int j = 0; j < files.length; j++) {
						String filePath = files[j];
						pathList.add(filePath);
					}
				}
			}
		}else{
			String  testtime1=ConfParser.time;
			//同时配置appname和code情况
			for (int i = 0; i < keyList.size(); i++) {
				String key = keyList.get(i).toString();
				String keyPath = prefix+key;
				File keyFile = new File(keyPath);
				if(keyFile.exists() && keyFile.isDirectory()){
					pathList.add(keyFile.getAbsolutePath());
				}
				for (int j = 0; j < appidList.size(); j++) {
					if(testtime1==null || "".equals(testtime1)){
						String[] testtime2=testtime.split(",");
						for(String test1:testtime2){
							String appid = appidList.get(j).toString();
							String appFilePath = prefix+appid+File.separator+test1+File.separator+key;
							File appFile = new File(appFilePath);
							
							if(appFile.exists()){
								pathList.add(appFile.getAbsolutePath());
							}
						}
					}else{
						if(testtime1.indexOf(",")==-1){
							String appid = appidList.get(j).toString();
							String appFilePath =prefix+appid+File.separator+testtime1+File.separator+key;
							File appFile = new File(appFilePath);
							
							if(appFile.exists()){
								pathList.add(appFile.getAbsolutePath());
							}
						}else{
							String[] str=testtime1.split(",");
							for(String trs2:str){
								String appid = appidList.get(j).toString();
								String appFilePath = prefix+appid+File.separator+trs2+File.separator+key;
								File appFile = new File(appFilePath);
								if(appFile.exists()){
									pathList.add(appFile.getAbsolutePath());
								}
							}
						}
					}
					
				}
			}
		}
		//根据配置获取所有含有Default或分组的路径
		List<String> defaultPathList = new ArrayList<String>();
		int num = 0;
		for (int i = 0; i < pathList.size(); i++) {
			String path = pathList.get(i).toString();
			File file = new File(path);
			if(file.exists()){
				defaultPathList.add(path);
				File[] sonFolders = file.listFiles(new FilenameFilter(){
		            public boolean accept(File dir, String name){
		            	if(dir.isDirectory()){
		            		return true;
		            	}
		            	return false;
		            }
		        });
				if(ConfParser.pathType.equals("default")){
					for (int j = 0; j < sonFolders.length; j++) {
						String folderPath = "";
						folderPath = sonFolders[j].toString();
						File folder = new File(folderPath);
						boolean flag = true;
						if(folder.exists() && folder.isDirectory()){
							if(folder.getName().equals("Default")){
								defaultPathList.add(folder.getAbsolutePath());
								continue;
							}
								getSonPath(folder, defaultPathList);
						}
					}
				}else{
					for (int j = 0; j < sonFolders.length; j++) {
						String folderPath = "";
						folderPath = sonFolders[j].toString();
						File folder = new File(folderPath);
						boolean flag = true;
						if(folder.exists() && folder.isDirectory()){
//							if(folder.getName().equals("Default")){
								defaultPathList.add(folder.getAbsolutePath());
//								continue;
//							}
//								getSonPath(folder, defaultPathList);
						}
					}
				}
			}
		}
		String paths[] = new String[defaultPathList.size()];
		for (int i = 0; i < defaultPathList.size(); i++) {
			String pathss = defaultPathList.get(i).toString();
			paths[num++] = pathss;
		}
        return paths;
	}
	
	/**
	 * 获取出所有带有default的文件的路径
	 * @param file
	 * @param defaultPathList
	 * @return
	 */
	public List<String> getSonPath(File file,List<String> defaultPathList){
		File[] sonFolders = file.listFiles(new FilenameFilter(){
            public boolean accept(File dir, String name){
            	if(dir.isDirectory()){
            		return true;
            	}
            	return false;
            }
        });
		if(sonFolders!=null){
			for (int j = 0; j < sonFolders.length; j++) {
				String folderPath = "";
				folderPath = sonFolders[j].toString();
				File folder = new File(folderPath);
				boolean flag = true;
				if(folder.exists() && folder.isDirectory()){
					if(folder.getName().equals("Default")){
						defaultPathList.add(folder.getAbsolutePath());
						continue;
					}
				}
				getSonPath(folder, defaultPathList);
			}
		}
		
		return defaultPathList;
	}
	
	/**
	 * 平铺结构的路径获取 无Default
	 * @param appidList
	 * @return
	 */
	public String[] getPaths(List appidList){
		ArrayList keyList = new ArrayList();
		if(ConfParser.code==null || ConfParser.code.length<=0){
			return new String[0];
		}
		for (int j = 0; j < ConfParser.code.length; j++) {
			String code1 = ConfParser.code[j];
			String codeStr = "code="+code1;
			String url = ConfParser.getCodePath;
			if(url.isEmpty()){
				return new String[0];
			}
			String sr=GetPostTest.sendGet(url, codeStr);
	        System.out.println(sr);
	        JSONObject json = JSONObject.fromObject(sr);
	        JSONArray jsonArray = json.getJSONArray("detail");
	        try {
	        	JSONObject thisOrgInfo = json.getJSONObject("thisOrgInfo");
		        String keySelf = thisOrgInfo.getString("key");
		        String storePathSelf = thisOrgInfo.getString("storepath");
		        if(keySelf==null){
		        	keySelf = "";
		        }
		        if(!keySelf.isEmpty()){
		        	keyList.add(keySelf);	
		        }
		        if(storePathSelf!=null && !storePathSelf.isEmpty()){
		        	storePathSelf = storePathSelf.replace("/", File.separator).replace("\\", File.separator);
		        	keyList.add(storePathSelf);
		        }
		        for (int i = 0; i < jsonArray.size(); i++) {
		        	JSONObject jsonObject = jsonArray.getJSONObject(i);
		    		String code = jsonObject.getString("code");
		    		String key = jsonObject.getString("key");
		    		String storePath = jsonObject.getString("storepath");
		    		if(key!=null && !key.isEmpty()){
						keyList.add(key);	
					}
		    		if(storePath!=null && !storePath.isEmpty()){
		    			storePath = storePath.replace("/", File.separator).replace("\\", File.separator);
						keyList.add(storePath);	
					}
		    		if(code!=null && !code.isEmpty()){
		    			keyList = getSonList(keyList, code);
		    		}
				}
			} catch (Exception e) {
				e.printStackTrace();
				continue;
			}
	        
		}
		List pathList = new ArrayList<String>();
		String prefix = ConfParser.org_prefix;
		if((keyList!=null && keyList.size()>0) && (appidList==null || appidList.size()<=0)){
			//只有code的情况
			for (int i = 0; i < keyList.size(); i++) {
				String org_keyString = keyList.get(i).toString();
				if(!org_keyString.isEmpty()){
					pathList.add(prefix+org_keyString);
				}
			}
		}else if((keyList==null || keyList.size()<=0) && (appidList!=null && appidList.size()>0)){
			//只有appname的情况
			for (int i = 0; i < appidList.size(); i++) {
				String appid = appidList.get(i).toString();
				String floderPath = prefix+appid;
				File floder = new File(floderPath);
				if(floder.exists()){
					String[] files = floder.list();
					for (int j = 0; j < files.length; j++) {
						String filePath = files[j];
						pathList.add(filePath);
					}
				}
			}
		}else{
			//同时配置appname和code情况
			for (int i = 0; i < keyList.size(); i++) {
				String key = keyList.get(i).toString();
				String keyPath = prefix+key;
				File keyFile = new File(keyPath);
				if(keyFile.exists() && keyFile.isDirectory()){
					pathList.add(keyFile.getAbsolutePath());
				}
				for (int j = 0; j < appidList.size(); j++) {
					String appid = appidList.get(j).toString();
					String appFilePath = prefix+appid+File.separator+key;
					File appFile = new File(appFilePath);
					if(appFile.exists()){
						pathList.add(appFile.getAbsolutePath());
					}
				}
			}
		}
		String paths[] = new String[pathList.size()];
		int num = 0;
		for (int i = 0; i < pathList.size(); i++) {
			String path = pathList.get(i).toString();
			paths[num++] = path;
		}
		//最开始查询数据库获取分组的逻辑
//		int k = 0;
//        String []paths = new String[keyList.size()];
//        for (int i = 0; i < keyList.size(); i++) {
//			String org_keyString = keyList.get(i).toString();
//			paths[k++] = ConfParser.org_prefix+org_keyString;
//		}
//        if(codesList.size()>0){
//        	String orgIds = "(";
//            for (int i = 0; i < codesList.size(); i++) {
//            	String code = codesList.get(i).toString();
//    			orgIds += code+",";
//    		}
//            while(orgIds.endsWith(",")){
//            	orgIds = orgIds.substring(0,orgIds.length()-1);
//            }
//            orgIds += ")";
//            start();
//            String sql = "SELECT org_key FROM auth.t_org where id in "+orgIds+"";
//            try {
//				ResultSet rs = statement.executeQuery(sql);
//				while(rs.next()){
//					String org = rs.getString("org_key");
//					if(org==null || org.isEmpty()){
//						continue;
//					}
//					orgKeyList.add(org);
//				}
//			} catch (SQLException e) {
//				e.printStackTrace();
//			}
//        }
       
        return paths;
	}
	
	public ArrayList getSonList(ArrayList keyList, String code) {
		String codeStr = "code=" + code;
		String url = ConfParser.getCodePath;
		String sr = GetPostTest.sendGet(url, codeStr);
		JSONObject jsonSon = JSONObject.fromObject(sr);
		JSONArray jsonArraySon = jsonSon.getJSONArray("detail");
		for (int i = 0; i < jsonArraySon.size(); i++) {
			JSONObject jsonObject = jsonArraySon.getJSONObject(i);
			code = jsonObject.getString("code");
			String key = jsonObject.getString("key");
			String storepath = jsonObject.getString("storepath");
			if(key!=null && !key.isEmpty()){
				keyList.add(key);	
			}
			if(storepath!=null && !storepath.isEmpty()){
				storepath = storepath.replace("/", File.separator).replace("\\", File.separator);
				keyList.add(storepath);	
			}
			if (code != null && !code.isEmpty()) {
				keyList = getSonList(keyList, code);
			}
		}
		return keyList;
	}
	
	/*public ArrayList getSonList(ArrayList codesList, String code) {
		String codeStr = "code=" + code;
		String url = "http://218.206.179.179/ctp/orginfo/v3/query/getSecondOrgsByCode.do";
		String sr = GetPostTest.sendGet(url, codeStr);
		JSONObject jsonSon = JSONObject.fromObject(sr);
		JSONArray jsonArraySon = jsonSon.getJSONArray("detail");
		for (int i = 0; i < jsonArraySon.size(); i++) {
			JSONObject jsonObject = jsonArraySon.getJSONObject(i);
			code = jsonObject.getString("code");
			codesList.add(code);
			if (code != null && !code.isEmpty()) {
				codesList = getSonList(codesList, code);
			}
		}
		return codesList;
	}*/
	
	/*
	 * public String[] getPaths() { //发送 GET 请求 // String
	 * s=GetPostTest.sendGet("http://192.168.85.233:8383/ctp/auth/getOrgInfo.do",
	 * "apikey=97"); // System.out.println(s);
	 * 
	 * //发送 POST 请求 String apikeyStr = "apikey="+ConfParser.apikey;
	 * 
	 * String
	 * sr=GetPostTest.sendPost("http://192.168.85.233:8383/ctp/auth/getOrgInfo.do",
	 * apikeyStr); System.out.println(sr); // if(!sr.isEmpty()){ // sr =
	 * "\"result\":"+sr; // } JSONObject json = JSONObject.fromObject(sr);
	 * JSONArray jsonArray = json.getJSONObject("detail").getJSONArray("info");
	 * String []paths = new String[jsonArray.size()]; int j = 0; for (int i = 0;
	 * i < jsonArray.size(); i++) { JSONObject jsonObject =
	 * jsonArray.getJSONObject(i); String type = jsonObject.getString("type");
	 * if(type!=null && type.equals("1")){ String key =
	 * jsonObject.getString("key"); if(key!=null && !key.isEmpty()){ paths[j++] =
	 * ConfParser.org_prefix+key; } } } for (int i = 0; i < paths.length; i++) {
	 * System.out.println(paths[i]); } return paths; }
	 */
	public void start() {
		String driver = "com.mysql.jdbc.Driver";
//		String url = "jdbc:mysql://192.168.85.233:3306/testdataanalyse";
		String url = ConfParser.url;
		String user = ConfParser.user;
//		String password = "cmrictpdata";
		String password = ConfParser.password;
		try {
			Class.forName(driver);
			conn = DriverManager.getConnection(url, user, password);
			statement = conn.createStatement();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void close() throws SQLException {
		if(statement!=null){
			statement.close();
		}
		if (conn != null) {
			conn.close();
		}
	}
	
	
	public static void main(String[] args) {
		String ss = sendGet("http://192.168.16.97:7800/ctp/action/getSubOrgsByCode.do", "code=2398");
		System.out.println(ss);
	}
}
