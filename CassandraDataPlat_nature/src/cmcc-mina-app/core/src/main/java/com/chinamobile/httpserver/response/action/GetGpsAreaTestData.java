package com.chinamobile.httpserver.response.action;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.antlr.grammar.v3.ANTLRv3Parser.finallyClause_return;
import org.antlr.gunit.gUnitParser.file_return;
import org.apache.cassandra.cli.CliParser.value_return;
import org.apache.cassandra.thrift.Cassandra.AsyncProcessor.get;
import org.apache.log4j.Logger;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.example.httpserver.codec.HttpRequestMessage;
import org.apache.mina.example.httpserver.codec.HttpResponseMessage;

import com.chinamobile.httpserver.response.CmriJsonResponse;
import com.csvreader.CsvReader;
import com.opencassandra.descfile.ConfParser;

public class GetGpsAreaTestData  implements CmriJsonResponse{
	private static UpdateDataInfo instance = null;
	static Logger logger = Logger.getLogger(UpdateDataInfo.class);
	private double LNGinterval = 0.001;
	private double LATinterval = 0.0008;
	Statement statement;
	Connection conn;
	
	public static UpdateDataInfo getInstance() {
		if(instance == null){
			return new UpdateDataInfo();
		}
        return instance;
    }
	/*
	 * 查询文件内容
	 * (non-Javadoc)
	 * @see com.chinamobile.httpserver.response.CmriJsonResponse#execute(org.apache.mina.core.session.IoSession, java.lang.Object)
	 */
	@Override
	public HttpResponseMessage execute(IoSession session, Object message)
			throws IOException {
		String jsonMessage = ((HttpRequestMessage) message).getBody();
		String output = URLDecoder.decode(jsonMessage, "UTF-8");
		System.out.println("PostBody is:" + output);

		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
		JSONObject job = JSONObject.fromObject(output);
		
		String path = ConfParser.csvwritepath;
		String lonStart = job.getString("longitude_start");
		String latStart = job.getString("latitude_start");
		String lonEnd = job.getString("longitude_end");
		String latEnd = job.getString("latitude_end");
		String dateStr = job.getString("dateMonth");
		final String numType = job.getString("numType");
		String table = "";
		String result = "";
		JSONObject resultJson = new JSONObject();
		JSONArray jsonArray = new JSONArray();
		List<String> descFileList = new ArrayList<String>();
		
		String dateStartString = "20140101000000";
		String dateEndString = "";
		boolean flag = true;
		Date date;
		Date date1;
		long timeStart = 0;
		long timeEnd = 0;
		
		if(dateStr!=null && !dateStr.isEmpty()){
			if(dateStr.length()==6){
				Integer year = Integer.parseInt(dateStr.substring(0,4));
				Integer month =  Integer.parseInt(dateStr.substring(4,6));
				if(month==12){
					year = year + 1;
					month = 1;
				}else{
					month = month + 1;
				}
				if(month.toString().length() == 1){
					dateEndString = year+"0"+month+"0101000000";
				}else{
					dateEndString = year+""+month+"0101000000";
				}
			}
			try {
				date = sdf.parse(dateStartString);
				date1 = sdf.parse(dateEndString);
				timeStart = date.getTime();
				timeEnd = date1.getTime();
				
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}else{
			flag = false;
		}
		if(numType == null || numType.isEmpty() || !flag){
			
		}else{
			double lon_min = Double.parseDouble(lonStart);
			double lon_max = Double.parseDouble(lonEnd);
			double lat_min = Double.parseDouble(latStart);
			double lat_max = Double.parseDouble(latEnd);
			
			List<File> folderList = new ArrayList<File>();//获取所有符合的文件目录
			for (double i = Math.floor(lon_min); i <=Math.floor(lon_max) ; i=i+1) {
				for (double j = Math.floor(lat_min); j <=Math.floor(lat_max) ; j=j+1) {
					String folderName = i+"_"+j;
					String folderPath = path+File.separator+folderName;
					File folderFile = new File(folderPath);
					if(folderFile.exists()){
						folderList.add(folderFile);
					}
				}
			}
			for (int i = 0; i < folderList.size(); i++) {
				File files = folderList.get(i);
				File[] fileList = files.listFiles(new FilenameFilter(){
		            public boolean accept(File dir, String name){
		            	if(name.indexOf("_"+numType+".csv") >= 0){
		            		return true;
		            	}
		            	return false;
		            }
		        });
				for (int j = 0; j < fileList.length; j++) {
					File file = fileList[j];
					String filename = file.getName();
					String[] values = filename.split("_");
					double center_lon = Double.parseDouble(values[0]);
					double center_lat = Double.parseDouble(values[1]);
					
					double center_lon_max = center_lon+LNGinterval;
					double center_lon_min = center_lon-LNGinterval;
					double center_lat_max = center_lat+LATinterval;
					double center_lat_min = center_lat-LATinterval;
					
					if(center_lon_max<=lon_max && center_lon_min>=lon_min && center_lat_max<=lat_max && center_lat_min>=lat_min){//中心点范围在经纬度范围内，此文件所有测试报告可呈现
						jsonArray = this.dealFile(descFileList,jsonArray, file,timeStart,timeEnd,numType);
					}else if(center_lon_min>lon_max || center_lon_max<lon_min || center_lat_min >lat_max || center_lat_max<lat_min){//中心点范围在经纬度范围外
						//跳过
					}else{//有交集
						jsonArray = this.dealFile(descFileList,jsonArray, file,lon_max,lon_min,lat_max,lat_min,timeStart,timeEnd,numType);
					}
				}
			}
			Map resultMap = new HashMap();
			resultMap.put("info", jsonArray);
			resultMap.put("fileList", descFileList);
			resultJson = resultJson.fromObject(resultMap);
			if(resultJson == null){
				result = "";
			}else{
				result = resultJson.toString();
			}
		}
		
		HttpResponseMessage response = new HttpResponseMessage();
		response.setResponseCode(HttpResponseMessage.HTTP_STATUS_SUCCESS);
		response.appendBody(result);
		return response;
	}
	public JSONArray dealFile(List<String> descFileList, JSONArray jsonArray,File file,long timeStart, long timeEnd, String numType){
		List<String []> list = this.readCsv(file);
		String [] names = list.get(0);
		int time_index = 0;
		int lat_index = 0;
		int lon_index = 0;
		int operator_index = 0;
		int net_type_index = 0;
		int signal_strength_index = 0;
		int sinr_index = 0;
		int lac_tac_index = 0;
		int cid_pci_index = 0;
		int model_index = 0; 
		//HTTP
		int success_rate_index = 0; //http and ping
		int url_index = 0;
		int resource_size_index = 0;
		int duration_index = 0;
		int avg_rate_index = 0;
		//PING
		int domain_address_index = 0;
		int max_rtt_index = 0;
		int min_rtt_index = 0;
		int avg_rtt_index = 0;
		int times_index = 0;
		//Speed Test
		int max_up_index = 0;
		int avg_up_index = 0;
		int max_down_index = 0;
		int avg_down_index = 0;
		int protocol_index = 0;
		int delay_index = 0;
		int server_ip_index = 0;
		Map<String,Integer> indexMap = new HashMap<String,Integer>();
		for (int i = 0; i < names.length; i++) {
			if(names[i]!=null){
				indexMap.put(names[i],i);
				/*if(names[i].equals("longitude")){
					lon_index = i;
				}
				if(names[i].equals("latitude")){
					lat_index = i;
					indexMap.put("lon_index",lon_index);
				}
				if(names[i].equals("time")){
					time_index = i;
				}
				if(names[i].equals("operator")){
					operator_index = i;
				}
				if(names[i].equals("net_type")){
					net_type_index = i;
				}
				if(names[i].equals("signal_strength")){
					signal_strength_index = i;
				}
				if(names[i].equals("sinr")){
					sinr_index = i;
				}
				if(names[i].equals("lac_tac")){
					lac_tac_index = i;
				}
				if(names[i].equals("cid_pci")){
					cid_pci_index = i;
				}
				if(names[i].equals("model")){
					model_index = i;
				}
				//http
				if(names[i].equals("success_rate")){
					success_rate_index = i;
				}
				if(names[i].equals("url")){
					url_index = i;
				}
				if(names[i].equals("resource_size")){
					resource_size_index = i;
				}
				if(names[i].equals("duration")){
					duration_index = i;
				}
				if(names[i].equals("avg_rate")){
					avg_rate_index = i;
				}
				//ping
				if(names[i].equals("domain_address")){
					domain_address_index = i;
				}
				if(names[i].equals("max_rtt")){
					max_rtt_index = i;
				}
				if(names[i].equals("min_rtt")){
					min_rtt_index = i;
				}
				if(names[i].equals("avg_rtt")){
					avg_rtt_index = i;
				}
				if(names[i].equals("times")){
					times_index = i;
				}
				//speed_test
				if(names[i].equals("max_up")){
					max_up_index = i;
				}
				if(names[i].equals("avg_up")){
					avg_up_index = i;
				}
				if(names[i].equals("max_down")){
					max_down_index = i;
				}
				if(names[i].equals("avg_down")){
					avg_down_index = i;
				}
				if(names[i].equals("protocol")){
					protocol_index = i;
				}if(names[i].equals("delay")){
					delay_index = i;
				}
				if(names[i].equals("server_ip")){
					server_ip_index = i;
				}*/
			}
		}
		for (int i = 1; i < list.size(); i++) {
			if(!isBlankLine(list.get(i))){
				String values[] = list.get(i);
				String longitude = indexMap.containsKey("longitude")?values[indexMap.get("longitude")]:"";
				String latitude = indexMap.containsKey("latitude")?values[indexMap.get("latitude")]:"";
				String timeString = indexMap.containsKey("time")?values[indexMap.get("time")]:"";
				String operator = indexMap.containsKey("operator")?values[indexMap.get("operator")]:"";
				String net_type = indexMap.containsKey("net_type")?values[indexMap.get("net_type")]:"";
				String signal_strength = indexMap.containsKey("signal_strength")?values[indexMap.get("signal_strength")]:"";
				String sinr = indexMap.containsKey("sinr")?values[indexMap.get("sinr")]:"";
				String lac_tac = indexMap.containsKey("lac_tac")?values[indexMap.get("lac_tac")]:"";
				String cid_pci = indexMap.containsKey("cid_pci")?values[indexMap.get("cid_pci")]:"";
				String model = indexMap.containsKey("model")?values[indexMap.get("model")]:"";
				
				timeString = timeString.replace("`", "");
				if(timeString.isEmpty() || !isNum(timeString)){
					continue;
				}else {
					values[indexMap.get("time")] = timeString;
					long time = Long.parseLong(timeString);
					if(time<=timeEnd && time>=timeStart){
						
					}else{
						continue;
					}
				}
				Map map = new HashMap();
				
				map.put("longitude",longitude);
				map.put("latitude", latitude);
				map.put("time", timeString);
				map.put("operator", operator);
				map.put("net_type", net_type);
				map.put("signal_strength", signal_strength);
				map.put("sinr", sinr);
				map.put("lac_tac", lac_tac);
				map.put("cid_pci", cid_pci);
				map.put("model", model);
				
				if(numType.equals("03001")){
					String success_rate = indexMap.containsKey("success_rate")?values[indexMap.get("success_rate")]:"";
					String url = indexMap.containsKey("url")?values[indexMap.get("url")]:"";
					String resource_size = indexMap.containsKey("resource_size")?values[indexMap.get("resource_size")]:"";
					String duration = indexMap.containsKey("duration")?values[indexMap.get("duration")]:"";
					String avg_rate = indexMap.containsKey("avg_rate")?values[indexMap.get("avg_rate")]:"";
					map.put("success_rate", success_rate);
					map.put("url", url);
					map.put("resource_size",resource_size);
					map.put("duration", duration);
					map.put("avg_rate", avg_rate);
				}else if(numType.equals("04002")){
					String success_rate = indexMap.containsKey("success_rate")?values[indexMap.get("success_rate")]:"";; 
					String domain_address = indexMap.containsKey("domain_address")?values[indexMap.get("domain_address")]:"";;
					String max_rtt = indexMap.containsKey("max_rtt")?values[indexMap.get("max_rtt")]:"";;
					String min_rtt = indexMap.containsKey("min_rtt")?values[indexMap.get("min_rtt")]:"";;
					String avg_rtt = indexMap.containsKey("avg_rtt")?values[indexMap.get("avg_rtt")]:"";;
					String times = indexMap.containsKey("times")?values[indexMap.get("times")]:"";;
					map.put("success_rate", success_rate);
					map.put("domain_address", domain_address);
					map.put("max_rtt",max_rtt);
					map.put("min_rtt", min_rtt);
					map.put("avg_rtt", avg_rtt);
					map.put("times", times);
				}else if(numType.equals("01001")){
					String max_up = indexMap.containsKey("max_up")?values[indexMap.get("max_up")]:"";;
					String avg_up = indexMap.containsKey("avg_up")?values[indexMap.get("avg_up")]:"";;
					String max_down = indexMap.containsKey("max_down")?values[indexMap.get("max_down")]:"";;
					String avg_down = indexMap.containsKey("avg_down")?values[indexMap.get("avg_down")]:"";;
					String protocol = indexMap.containsKey("protocol")?values[indexMap.get("protocol")]:"";;
					String delay = indexMap.containsKey("delay")?values[indexMap.get("delay")]:"";; 
					String server_ip = indexMap.containsKey("server_ip")?values[indexMap.get("server_ip")]:"";;
					map.put("max_up", max_up);
					map.put("avg_up", avg_up);
					map.put("max_down",max_down);
					map.put("avg_down", avg_down);
					map.put("protocol", protocol);
					map.put("delay", delay);
					map.put("server_ip", server_ip);
				}
				jsonArray.add(JSONArray.fromObject(map));
				if(!descFileList.contains(file.getAbsolutePath())){
					descFileList.add(file.getAbsolutePath());	
				}
			}
		}
		return jsonArray;
	}
	public JSONArray dealFile(List<String> descFileList, JSONArray jsonArray,File file, double lon_max, double lon_min, double lat_max, double lat_min, long timeStart, long timeEnd,String numType){
		String fileName = file.getName();
		
		List<String []> list = this.readCsv(file);
		String [] names = list.get(0);
		int lat_index = 0;
		int lon_index = 0;
		int time_index = 0;
		int operator_index = 0;
		int net_type_index = 0;
		int signal_strength_index = 0;
		int sinr_index = 0;
		int lac_tac_index = 0;
		int cid_pci_index = 0;
		int model_index = 0; 
		
		//HTTP
		int success_rate_index = 0; //http and ping
		int url_index = 0;
		int resource_size_index = 0;
		int duration_index = 0;
		int avg_rate_index = 0;
		int file_type_index = 0;
		//PING
		int domain_address_index = 0;
		int max_rtt_index = 0;
		int min_rtt_index = 0;
		int avg_rtt_index = 0;
		int times_index = 0;
		//Speed Test
		int max_up_index = 0;
		int avg_up_index = 0;
		int max_down_index = 0;
		int avg_down_index = 0;
		int protocol_index = 0;
		int delay_index = 0;
		int server_ip_index = 0;
		Map<String,Integer> indexMap = new HashMap<String,Integer>();
		
		for (int i = 0; i < names.length; i++) {
			if(names[i]!=null){
				indexMap.put(names[i], i);
				/*if(names[i].equals("longitude")){
					lon_index = i;
				}
				if(names[i].equals("latitude")){
					lat_index = i;
				}
				if(names[i].equals("time")){
					time_index = i;
				}
				if(names[i].equals("operator")){
					operator_index = i;
				}
				if(names[i].equals("net_type")){
					net_type_index = i;
				}
				if(names[i].equals("signal_strength")){
					signal_strength_index = i;
				}
				if(names[i].equals("sinr")){
					sinr_index = i;
				}
				if(names[i].equals("lac_tac")){
					lac_tac_index = i;
				}
				if(names[i].equals("cid_pci")){
					cid_pci_index = i;
				}
				if(names[i].equals("model")){
					model_index = i;
				}
				//http
				if(names[i].equals("success_rate")){
					success_rate_index = i;
				}
				if(names[i].equals("url")){
					url_index = i;
				}
				if(names[i].equals("resource_size")){
					resource_size_index = i;
				}
				if(names[i].equals("duration")){
					duration_index = i;
				}
				if(names[i].equals("avg_rate")){
					avg_rate_index = i;
				}
				if(names[i].equals("file_type")){
					file_type_index = i;
				}
				//ping
				if(names[i].equals("domain_address")){
					domain_address_index = i;
				}
				if(names[i].equals("max_rtt")){
					max_rtt_index = i;
				}
				if(names[i].equals("min_rtt")){
					min_rtt_index = i;
				}
				if(names[i].equals("avg_rtt")){
					avg_rtt_index = i;
				}
				if(names[i].equals("times")){
					times_index = i;
				}
				//speed_test
				if(names[i].equals("max_up")){
					max_up_index = i;
				}
				if(names[i].equals("avg_up")){
					avg_up_index = i;
				}
				if(names[i].equals("max_down")){
					max_down_index = i;
				}
				if(names[i].equals("avg_down")){
					avg_down_index = i;
				}
				if(names[i].equals("protocol")){
					protocol_index = i;
				}if(names[i].equals("delay")){
					delay_index = i;
				}
				if(names[i].equals("server_ip")){
					server_ip_index = i;
				}*/
			}
		}
		for (int i = 1; i < list.size(); i++) {
			
			if(!isBlankLine(list.get(i))){
				String values[] = list.get(i);
				String longitude = indexMap.containsKey("longitude")?values[indexMap.get("longitude")]:"";
				String latitude = indexMap.containsKey("latitude")?values[indexMap.get("latitude")]:"";
				String timeString = indexMap.containsKey("time")?values[indexMap.get("time")]:"";
				String operator = indexMap.containsKey("operator")?values[indexMap.get("operator")]:"";
				String net_type = indexMap.containsKey("net_type")?values[indexMap.get("net_type")]:"";
				String signal_strength = indexMap.containsKey("signal_strength")?values[indexMap.get("signal_strength")]:"";
				String sinr = indexMap.containsKey("sinr")?values[indexMap.get("sinr")]:"";
				String lac_tac = indexMap.containsKey("lac_tac")?values[indexMap.get("lac_tac")]:"";
				String cid_pci = indexMap.containsKey("cid_pci")?values[indexMap.get("cid_pci")]:"";
				String model = indexMap.containsKey("model")?values[indexMap.get("model")]:"";
				
				timeString = timeString.replace("`", "");
				if(timeString.isEmpty() || !isNum(timeString)){
					continue;
				}else {
					values[indexMap.get("time")] = timeString;
					long time = Long.parseLong(timeString);
					if(time<=timeEnd && time>=timeStart){
						
					}else{
						continue;
					}
				}
				try {
					double lonNum = Double.parseDouble(longitude);
					double latNum = Double.parseDouble(latitude);
					if((lonNum<=lon_max && lonNum>= lon_min) && (latNum>=lat_min && latNum <= lat_max)){
						
						Map map = new HashMap();
						//公共字段
						map.put("longitude",longitude);
						map.put("latitude", latitude);
						map.put("time", timeString);
						map.put("operator", operator);
						map.put("net_type", net_type);
						map.put("signal_strength", signal_strength);
						map.put("sinr", sinr);
						map.put("lac_tac", lac_tac);
						map.put("cid_pci", cid_pci);
						map.put("model", model);
						
						if(numType.equals("03001")){

							String success_rate = indexMap.containsKey("success_rate")?values[indexMap.get("success_rate")]:"";
							String url = indexMap.containsKey("url")?values[indexMap.get("url")]:"";
							String resource_size = indexMap.containsKey("resource_size")?values[indexMap.get("resource_size")]:"";
							String duration = indexMap.containsKey("duration")?values[indexMap.get("duration")]:"";
							String avg_rate = indexMap.containsKey("avg_rate")?values[indexMap.get("avg_rate")]:"";
							String file_type = indexMap.containsKey("file_type")?values[indexMap.get("file_type")]:"";
							if(file_type.equals("上传") || file_type.toLowerCase().equals("upload")){
								return jsonArray;
							}
							
							map.put("success_rate", success_rate);
							map.put("url", url);
							map.put("resource_size",resource_size);
							map.put("duration", duration);
							map.put("avg_rate", avg_rate);
							
							map.put("file_type", file_type);
							map.put("success_rate", success_rate);
							map.put("url", url);
							map.put("resource_size",resource_size);
							map.put("duration", duration);
							map.put("avg_rate", avg_rate);
						}else if(numType.equals("04002")){
							String success_rate = indexMap.containsKey("success_rate")?values[indexMap.get("success_rate")]:"";
							String domain_address = indexMap.containsKey("domain_address")?values[indexMap.get("domain_address")]:"";
							String max_rtt = indexMap.containsKey("max_rtt")?values[indexMap.get("max_rtt")]:"";
							String min_rtt = indexMap.containsKey("min_rtt")?values[indexMap.get("min_rtt")]:"";
							String avg_rtt = indexMap.containsKey("avg_rtt")?values[indexMap.get("avg_rtt")]:"";
							String times = indexMap.containsKey("times")?values[indexMap.get("times")]:"";
							map.put("success_rate", success_rate);
							map.put("domain_address", domain_address);
							map.put("max_rtt",max_rtt);
							map.put("min_rtt", min_rtt);
							map.put("avg_rtt", avg_rtt);
							map.put("times", times);
						}else if(numType.equals("01001")){
							String max_up = indexMap.containsKey("max_up")?values[indexMap.get("max_up")]:"";
							String avg_up = indexMap.containsKey("avg_up")?values[indexMap.get("avg_up")]:"";
							String max_down = indexMap.containsKey("max_down")?values[indexMap.get("max_down")]:"";
							String avg_down = indexMap.containsKey("avg_down")?values[indexMap.get("avg_down")]:"";
							String protocol = indexMap.containsKey("protocol")?values[indexMap.get("protocol")]:"";
							String delay = indexMap.containsKey("delay")?values[indexMap.get("delay")]:""; 
							String server_ip = indexMap.containsKey("server_ip")?values[indexMap.get("server_ip")]:"";
							map.put("max_up", max_up);
							map.put("avg_up", avg_up);
							map.put("max_down",max_down);
							map.put("avg_down", avg_down);
							map.put("protocol", protocol);
							map.put("delay", delay);
							map.put("server_ip", server_ip);
						}
						
//						for (int j = 0; j <	names.length; j++) {
//							if(names[j]==null || names[j].isEmpty()){
//								continue;
//							}
//							String name = names[j];
//							String value = values[j];
//							map.put(name, value);
//						}
						jsonArray.add(JSONArray.fromObject(map));
						if(!descFileList.contains(file.getAbsolutePath())){
							descFileList.add(file.getAbsolutePath());	
						}
//						if(jsonArray.size()>=100){
//							return jsonArray;
//						}
					}else{
						continue;
					}
				} catch (Exception e) {
					continue;
				}
			}
		}
		return jsonArray;
	}
	
	/*@Override
	public HttpResponseMessage execute(IoSession session,Object message) throws IOException {		
		String jsonMessage = ((HttpRequestMessage)message).getBody();
		String output = URLDecoder.decode(jsonMessage, "UTF-8");
		System.out.println("PostBody is:"+output);
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		JSONObject job = JSONObject.fromObject(output);
		String lonStart = job.getString("longitude_start");
		String latStart = job.getString("latitude_start");
		String lonEnd = job.getString("longitude_end");
		String latEnd = job.getString("latitude_end");
		String dateStr = job.getString("dateMonth");
		String numType = job.getString("numType");
		String table = "";
		String result = "";
		boolean flag = true;
		
		if(numType.equals("01001")){
			table = "speed_test_new";
		}else if(numType.equals("02001")){
			table = "web_browsing_new";
		}else if(numType.equals("02011")){
			table = "video_test_new";
		}else if(numType.equals("03001") || numType.equals("03011")){
			table = "http_test_new";
		}else if(numType.equals("04002")){
			table = "ping_new";
		}else if(numType.equals("04006")){
			table = "hand_over_new";
		}else if(numType.equals("05001")){
			table = "call_test_new";
		}else if(numType.equals("05002")){
			table = "sms_new";
		}else {
			result = "";
			flag = false;
		}
		if(flag){
			String dateStartString = "";
			String dateEndString = "";
			if(dateStr!=null && !dateStr.isEmpty()){
				String year = dateStr.substring(0,4);
				String month = dateStr.substring(4,6);
				if(dateStr.length()==6){
					dateStartString = dateStr+"01";
					if(month.equals("12")){
						dateEndString =	(Integer.parseInt(year)+1)+"0101";
					}else{
						dateEndString = year+(Integer.parseInt(month)+1)+"01";
					}
				}
				
				Date date;
				Date date1;
				long timeStart = 0;
				long timeEnd = 0;
				try {
					date = sdf.parse(dateStartString);
					date1 = sdf.parse(dateEndString);
					timeStart = date.getTime();
					timeEnd = date1.getTime();
					
				} catch (ParseException e) {
					e.printStackTrace();
					flag = false;
				}
				if(flag){
					String sql = "select * from "+table+" where longitude >="+lonStart+" and longitude<="+lonEnd+" and latitude >="+latStart+" and latitude <="+ latEnd+" and time >= "+timeStart+" and time <="+timeEnd;
					result = this.getResultJson(sql);
				}else {
					result = ""; 
				}
			}else{
				flag = false;
			}
		}
		
		HttpResponseMessage response = new HttpResponseMessage();
		response.setResponseCode(HttpResponseMessage.HTTP_STATUS_SUCCESS);
		response.appendBody(result);
		return response;
	}*/
	
	public String getResultJson(String sql){
		JSONArray jsonArray = new JSONArray();
		try {
			start();
			System.out.println(sql);
			ResultSet rs = statement.executeQuery(sql);
			ResultSetMetaData rsData = rs.getMetaData();
			while(rs.next()){
				int count = rsData.getColumnCount();
				Map map = new HashMap();
				for (int i = 1; i <=count; i++) {
					String name = rsData.getColumnName(i);
					String value = rs.getString(name);
					if(name!=null && !name.isEmpty()){
						map.put(name, value);
					}
				}
				JSONArray jsona = JSONArray.fromObject(map);
				jsonArray.add(jsona);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			try {
				close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return jsonArray.toString();
	}
	
	public void start() {
		String driver = "com.mysql.jdbc.Driver";
		String url = ConfParser.url;
//		String url = "jdbc:mysql://218.206.179.109:3306/testdataanalyse";
		String user = ConfParser.user;
		String password = ConfParser.password;
//		String password = "Bi123456";
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
	
	public boolean insert(String sql) {
		System.out.println("sql:" + sql);
		boolean flag = false;
		if(sql.isEmpty()){
			flag = false;
		}else{
			try {
				flag = statement.execute(sql);
			} catch (SQLException e) {
				e.printStackTrace();
				return false;
			}	
		}
		return flag;
	}
	
	/**
	 * 读取CSV文件
	 */
	public ArrayList<String[]> readCsv(File filePath) {
		ArrayList<String[]> csvList = null;
		try {
			csvList = new ArrayList<String[]>(); // 用来保存数据
			String csvFilePath = filePath.getAbsolutePath();
			CsvReader reader = new CsvReader(csvFilePath, ',',
					Charset.forName("GB2312")); // 一般用这编码读就可以了

			// reader.readHeaders(); // 跳过表头 如果需要表头的话，不要写这句。

			while (reader.readRecord()) { // 逐行读入除表头的数据
				String[] values = null;
				values = reader.getValues();
				csvList.add(values);
			}
			reader.close();

		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {

		}
		return csvList;
	}

	/**
	 * 判断CSV文件中某一行是不是空行
	 * 
	 * @param values
	 * @return
	 */
	public boolean isBlankLine(String[] values) {
		boolean isBlank = true;
		if (values != null) {
			for (int i = 0; i < values.length; i++) {
				if (values[i] != null && !"".equals(values[i])) {
					isBlank = false;
				}
			}
		}
		return isBlank;
	}
	
	public boolean isNum(String str){
		return str.matches("^[-+]?(([0-9]+)([.]([0-9]+))?|([.]([0-9]+))?)$");
	}
}
