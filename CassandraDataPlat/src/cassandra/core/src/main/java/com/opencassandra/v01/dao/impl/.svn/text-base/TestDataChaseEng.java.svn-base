package com.opencassandra.v01.dao.impl;

import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import me.prettyprint.cassandra.service.template.ColumnFamilyResult;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.IndexedSlicesPredicate;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.RangeSlicesQuery;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.apache.cassandra.thrift.IndexOperator;

import com.opencassandra.v01.dao.factory.KeySpaceFactory;
import com.opencassandra.v01.dao.factory.QueryExpression;

public class TestDataChaseEng {
	JSONObject requestBody = null;
	
	public TestDataChaseEng(JSONObject requestBody){
		this.requestBody = requestBody;
	} 
	public TestDataChaseEng(){
	}
//	public static void main(String[] args) {
//		JSONObject json = new JSONObject();
//		json.put("org", "EN_CMCC_CMPAK_CEM");
////		json.put("org", "VOICE_MOS_CLOUD");
//		json.put("last_key", "");
//		json.put("type", "02001");
////		json.put("menutype", "ios");
//		json.put("menutype", "android");
//		json.put("httptype", "download");
////		json.put("httptype", "upload");
//		json.put("ftptype", "download");
////		json.put("ftptype", "upload");
//		json.put("limit", 30);
//		TestDataChaseEng tdc = new TestDataChaseEng();
//		try {
//			Map map = tdc.getTestDataByOrg(json);
//			JSONArray jsonarray = JSONArray.fromObject(map.get("detail"));
//			System.out.println(jsonarray.toString());
//			
//			Set set = map.keySet();
//			Iterator iter = set.iterator();
//			while(iter.hasNext()){
//				System.out.println(iter.next());
//			}
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//	}
	public Map<String, Object> getTestDataByOrg() throws Exception{
		String startTime = "";
		String endTime = "";
		String type = "";
		String lastKey = "";
		String org = null;
		String menuType = "";
		String httptype = "";
		String ftptype = "";
		String limit = "";
		Map<String,Object> result = new HashMap<String,Object>();
		
		if(requestBody!=null){
			startTime = (String)requestBody.get("start_time");
			endTime = (String)requestBody.get("end_time");
			org = (String)requestBody.get("org");
			type = (String)requestBody.get("type");
			lastKey = (String)requestBody.get("last_key");
			menuType = (String)requestBody.get("menutype");
			httptype = (String)requestBody.get("httptype");
			ftptype = (String)requestBody.get("ftptype");
			limit = (String)requestBody.get("limit");
			
		}
//		if(json!=null)
//		{
////			startTime = json.getString("start_time");
////			endTime = json.getString("end_time");
//			org = json.getString("org");
//			type = json.getString("type");
//			lastKey = json.getString("last_key");
//			if(json.containsKey("menutype"))
//			{
//				menuType = json.getString("menutype");	
//			}
//			if(json.containsKey("httptype")){
//				httptype = json.getString("httptype");	
//			}
//			if(json.containsKey("ftptype")){
//				ftptype = json.getString("ftptype");	
//			}
//			if(json.containsKey("limit")){
//				limit = json.getString("limit");
//			}
//			
//		}
		if(limit==null || limit.equals("")){
			limit = "30";
		}
		SimpleDateFormat dateFormat = new SimpleDateFormat(
				"yyyy-MM-dd HH:mm:ss");
		Date startDate = null;
		Date endDate = null;
		try {
			if(startTime!=null && !"".equals(startTime)){
				startDate = dateFormat.parse(startTime);
			}
			if(endTime!=null && !"".equals(endTime)){
				endDate = dateFormat.parse(endTime);
			}
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		if(type!=null && org!=null && !"".equals(org)){
			try {
				if(type.equals("01001")){//测速
					result = this.getTestDataByOrgSpeedtest(type, menuType, lastKey, org, startDate, endDate ,limit);
				}else if(type.equals("02001")){//网页浏览
					result = this.getTestDataByOrg(type, menuType, lastKey, org, startDate, endDate, limit);
				}else if(type.equals("03001")){//HTTP文件上传或者下载
					result = this.getTestDataByHttp(type, menuType,httptype, lastKey, org, startDate, endDate, limit);
				}else if(type.equals("04002")){//PING
					result = this.getTestDataByPING(type, menuType, lastKey, org, startDate, endDate, limit);
				}else if(type.equals("02011")){//视频
					result = this.getTestDataByVideoTest(type, menuType, lastKey, org, startDate, endDate, limit);
				}else if(type.equals("04006")){//切换时延
					result = this.getTestDataByResideTest(type, menuType, lastKey, org, startDate, endDate, limit);
				}else if(type.equals("05001") || type.equals("05001.C")){//话音
					result = this.getTestDataByDial(type, menuType, lastKey, org, startDate, endDate, limit);
				}else if(type.equals("03002")){//FTP
					result = this.getTestDataByFTP(type, menuType,ftptype, lastKey, org, startDate, endDate, limit);
				}else if(type.equals("05004")){//answer
					result = this.getTestDataByAnswer(type, menuType, lastKey, org, startDate, endDate, limit);
				}else if(type.equals("05002")){//sms
					result = this.getTestDataBySMS(type, menuType, lastKey, org, startDate, endDate, limit);
				}
				result.put("status", "success");
				
			} catch (Exception e) {
				result.put("status", "value is null");
				e.printStackTrace();
			}
			return result;
		}
		return null;
	}
	/**
	 * speedTest01001 测速
	 * @author Ocean
	 * @param province
	 * @param city
	 * @param district
	 * @param orgs
	 * @param limit 
	 * @param testTypes
	 */
	public Map<String, Object> getTestDataByOrgSpeedtest(String type,String menuType, String startKey, String orgs, Date startDate, Date endDate, String limit) throws Exception{
		Integer max = Integer.parseInt(limit);
		Map<String,Object> respResult = new HashMap<String,Object> ();
		ArrayList<HashMap<String, String>> detail = new ArrayList<HashMap<String, String>>();
		Keyspace keyspaceOperator = null;
		try{
			keyspaceOperator = HFactory.createKeyspace(orgs, KeySpaceFactory.cluster);
		}catch (Exception e){
			e.printStackTrace();
		}
		RangeSlicesQuery<String, String, String> rangeQuery = HFactory
				.createRangeSlicesQuery(keyspaceOperator, KeySpaceFactory.stringSerializer, KeySpaceFactory.stringSerializer, KeySpaceFactory.stringSerializer);
		
		rangeQuery.setColumnFamily(type);
		rangeQuery.setRowCount(200);
		if(startKey==null || startKey.equals("")){
			startKey = "";
		}
		rangeQuery.setKeys(startKey, "");
		//"time","terminal model","OS version","GPS position","GPS rate","network type","Network Name","server information","the terminal network address","terminal public address","terminal MAC address","signal strength","WIFI signal strength"
		rangeQuery.setColumnNames("网络类型","网络制式","Network Type","network type","小区信息","Network cell","SINR","网络(1)SINR","Network(1) SINR","网络(1)信号强度","Network(1) Signal Strength","网络(1)小区信息","Network(1) Cell","网络(1)网络制式","Network(1) Type","Network (1) standard","Network (1) standard","网络(2)SINR","Network(2) SINR","网络(2)信号强度","Network(2) Signal Strength","网络(2)小区信息","Network(2) Cell","网络(2)网络制式","Network(2) Type","Network (2) standard","信号强度","Signal Strength","终端类型","终端型号","Terminal Model","terminal model","Terminal Type","Device Type","Terminal Mode","Device Model","固件版本","软件版本号","OS version","data_time","平均速率(Kbps)","Avg Speed(Kbps)","imei","LAC","CID","LAC/CID","TAC","PCI","TAC/PCI","cassandra_province","cassandra_city","cassandra_district","cassandra_street","cassandra_street_number","fileName","fileSize","filePath","file_lastModifies","时延","时延(ms)","平均时延(ms)","Avg Time(ms)","Latency","Delay","Avg Latency(ms)","服务器信息","服务器地址","Service Info","Server Info","上行速率","上行速率(Mbps)","Up Speed","Up Speed(Mbps)","Up Speed(Kbps)","Upload Speed","下行速率","下行速率(Mbps)","Down Speed","Down Speed(Mbps)","Down Speed(Kbps)","Download Speed","测速类型","测试类型","Protocol","上行线程数","Uplink Thread Count","Uplink Thread(s)","下行线程数","Downlink Thread Count","Downlink Thread(s)");
		QueryResult<OrderedRows<String, String, String>> result = rangeQuery
				.execute();
		OrderedRows<String, String, String> oRows = result.get();
		int count = 1;
		String lastKey = "";
		List<Row<String, String, String>> rowList = oRows.getList();
		for (Row<String, String, String> row : rowList) {
			if(row.getKey().equals(startKey)){
				continue;
			}
			System.out.println(row.getKey());
			HashMap<String, String> myData = new HashMap<String, String>();
			
			List<HColumn<String, String>> columns = row.getColumnSlice()
					.getColumns();
			String speed = "";

			String lac = "";
			String cid = "";
			String tac = "";
			String pci = "";
			String netType = "";
			String testTime = "";
			String deviceType = "";
			String toolsVersion = "";
			String imei = "";
			String cell_info = "";
			
			String topSpeed = "";//上行速率
			String max_top = "";//最大上行速率
			String downSpeed = "";//下行速率
			String max_down = "";//最大下行速率
			String delay = "";//时延
			String serverIp = "";//服务器信息
			String protocol = "";//测速类型
			String topThread = "";//上行线程数
			String downThread = "";//下行线程数
			
			String province = "";
			String city = "";
			String district = "";
			String street = "";
			String street_no = "";
			
			String fileName = "";
			String filePath = "";
			String fileSize = "";
			String fileLastModifies = "";
			
			String modelvalue = "";
			boolean flag1 = false;//判断小区1网络制式是否存在
			boolean flag2 = false;//小区2
			String sinr = "";//SINR
			String netWorkType = "";//网络类型 或者 网络制式
			String signal = "";//信号强度
			
			String sinr1 = "";
			String sinr2 = "";
			String netWorkType1 = "";
			String netWorkType2 = "";
			String cell1 = "";
			String cell2 = "";
			String signal1 = "";
			String signal2 = "";
			for (HColumn<String, String> hColumn : columns) {
				String name = hColumn.getName();
				if(name.contains("终端类型") || name.contains("终端型号") || "Device Type".equals(hColumn.getName()) || "Terminal Model".equals(hColumn.getName()) || "Terminal Type".equals(hColumn.getName()) || "Terminal Mode".equals(hColumn.getName()) || "Device Model".equals(hColumn.getName()) || "terminal model".equals(hColumn.getName())){
					modelvalue = hColumn.getValue();
				}
				if(name.contains("网络(1)网络制式") || name.contains("Network(1) Type") || name.contains("Network (1) standard")){
					flag1 = true;
				}
				if(name.contains("网络(2)网络制式") || name.contains("Network(2) Type") || name.contains("Network (2) standard")){
					flag2 = true;
				}
			}
			if(menuType.equals("ios") && modelvalue!=null && !modelvalue.toLowerCase().contains("iphone")){
				continue;
			}else if(menuType.equals("android") && modelvalue!=null && modelvalue.toLowerCase().contains("iphone")){
				continue;
			}
			for (HColumn<String, String> hColumn : columns) {
				if("平均速率(Kbps)".equals(hColumn.getName()) || "Avg Speed(Kbps)".equals(hColumn.getName())){
					speed = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("信号强度".equals(hColumn.getName()) || "Signal Strength".equals(hColumn.getName())){
					signal = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("data_time".equals(hColumn.getName())){
					testTime = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("终端类型".equals(hColumn.getName()) || "终端型号".equals(hColumn.getName()) || "Device Type".equals(hColumn.getName()) || "Terminal Model".equals(hColumn.getName()) || "Terminal Type".equals(hColumn.getName()) || "Terminal Mode".equals(hColumn.getName()) || "Device Model".equals(hColumn.getName()) || "terminal model".equals(hColumn.getName())){
					deviceType = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("固件版本".equals(hColumn.getName())||"软件版本号".equals(hColumn.getName()) || "OS version".equals(hColumn.getName())){
					toolsVersion = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("imei".equals(hColumn.getName())){
					imei = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("fileName".equals(hColumn.getName())){
					fileName = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("filePath".equals(hColumn.getName())){
					filePath = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("fileSize".equals(hColumn.getName())){
					fileSize = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("file_lastModifies".equals(hColumn.getName())){
					fileLastModifies = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("cassandra_province".equals(hColumn.getName())){
					province = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("cassandra_city".equals(hColumn.getName())){
					city = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("cassandra_district".equals(hColumn.getName())){
					district = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("cassandra_street".equals(hColumn.getName())){
					street = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("cassandra_street_no".equals(hColumn.getName())){
					street_no = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("LAC".equals(hColumn.getName())){
					String lacStr = hColumn.getValue()==null?"":hColumn.getValue();
					if(lacStr.indexOf("=")!=-1){
						lac = lacStr.substring(lacStr.indexOf("=")+1,lacStr.length());
					}else{
						lac = lacStr;
					}
				}else if("CID".equals(hColumn.getName())){
					String cidStr = hColumn.getValue()==null?"":hColumn.getValue();
					if(cidStr.indexOf("=")!=-1){
						cid = cidStr.substring(cidStr.indexOf("=")+1,cidStr.length());
					}else{
						cid = cidStr;
					}
				}else if("LAC/CID".equals(hColumn.getName())){
					String lacCid = hColumn.getValue()==null?"":hColumn.getValue();
					if(lacCid.indexOf("/")!=-1 || lacCid.indexOf("N/A")!=-1){
						String[] cellInfo = lacCid.split("/");
						lac = cellInfo[0];
						cid = cellInfo[1];
					}
				}else if("TAC".equals(hColumn.getName())){
					String tacStr = hColumn.getValue()==null?"":hColumn.getValue();
					if(tacStr.indexOf("=")!=-1){
						tac = tacStr.substring(tacStr.indexOf("=")+1,tacStr.length()-1);
					}else{
						tac = tacStr;
					}
				}else if("PCI".equals(hColumn.getName())){
					String pciStr = hColumn.getValue()==null?"":hColumn.getValue();
					if(pciStr.indexOf("=")!=-1){
						pci = pciStr.substring(pciStr.indexOf("=")+1,pciStr.length()-1);
					}else{
						pci = pciStr;
					}
				}else if("TAC/PCI".equals(hColumn.getName())){
					String tacPci = hColumn.getValue()==null?"":hColumn.getValue();
					if(tacPci.equals("N/A/N/A")){
						tac = "N/A";
						pci = "N/A";
					}else{
						if(tacPci.indexOf("/")!=-1 || tacPci.indexOf("N/A")!=-1){
							String[] cellInfo = tacPci.split("/");
							tac = cellInfo[0];
							pci = cellInfo[1];
						}
					}
				}else if("Maximum Up".equals(hColumn.getName()) || "上行最大速率(Mbps)".equals(hColumn.getName())){
					max_top = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("Maximum Down".equals(hColumn.getName()) || "下行最大速率(Mbps)".equals(hColumn.getName())){
					max_down = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("上行速率".equals(hColumn.getName())||"上行速率(Mbps)".equals(hColumn.getName()) || "Up Speed".equals(hColumn.getName()) || "Up Speed(Mbps)".equals(hColumn.getName()) || "Up Speed(Kbps)".equals(hColumn.getName()) || "Upload Speed".equals(hColumn.getName())){
					topSpeed = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("下行速率".equals(hColumn.getName())||"下行速率(Mbps)".equals(hColumn.getName())  || "Down Speed".equals(hColumn.getName()) || "Down Speed(Mbps)".equals(hColumn.getName()) || "Down Speed(Kbps)".equals(hColumn.getName()) || "Download Speed".equals(hColumn.getName())){
					downSpeed = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("Delay".equals(hColumn.getName()) || "Latency".equals(hColumn.getName()) || "时延".equals(hColumn.getName())||"时延(ms)".equals(hColumn.getName())||"平均时延(ms)".equals(hColumn.getName())){
					delay = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("服务器信息".equals(hColumn.getName()) || "服务器地址".equals(hColumn.getName()) || "Server Info".equals(hColumn.getName()) || "Service Info".equals(hColumn.getName())){
					serverIp = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("测速类型".equals(hColumn.getName()) || "测试类型".equals(hColumn.getName()) || "Protocol".equals(hColumn.getName())){
					protocol = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("上行线程数".equals(hColumn.getName()) || "Uplink Thread(s)".equals(hColumn.getName()) || "Uplink Thread Count".equals(hColumn.getName())){
					topThread = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("下行线程数".equals(hColumn.getName()) || "Downlink Thread(s)".equals(hColumn.getName()) || "Downlink Thread Count".equals(hColumn.getName())){
					downThread = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("网络类型".equals(hColumn.getName()) || "网络制式".equals(hColumn.getName()) || "Network Type".equals(hColumn.getName()) || "network type".equals(hColumn.getName())){
					netWorkType = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("SINR".equals(hColumn.getName())){
					sinr = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("网络(1)网络制式".equals(hColumn.getName()) || "Network(1) Type".equals(hColumn.getName()) || "Network (1) standard".equals(hColumn.getName())){
					netWorkType1 = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("网络(1)信号强度".equals(hColumn.getName()) || "Network(1) Signal Strength".equals(hColumn.getName())){
					signal1 = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("网络(1)小区信息".equals(hColumn.getName()) || "Network(1) Cell".equals(hColumn.getName()) || "Network(1)".equals(hColumn.getName())){
					cell1 = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("网络(1)SINR".equals(hColumn.getName()) || "Network(1) SINR".equals(hColumn.getName())){
					sinr1 = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("网络(2)网络制式".equals(hColumn.getName()) || "Network(2) Type".equals(hColumn.getName()) || "Network (2) standard".equals(hColumn.getName())){
					netWorkType2 = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("网络(2)信号强度".equals(hColumn.getName()) || "Network(2) Signal Strength".equals(hColumn.getName())){
					signal2 = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("网络(2)小区信息".equals(hColumn.getName()) || "Network(2) Cell".equals(hColumn.getName())){
					cell2 = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("网络(2)SINR".equals(hColumn.getName()) || "Network(2) SINR".equals(hColumn.getName())){
					sinr2 = hColumn.getValue()==null?"":hColumn.getValue();
				}else{
					myData.put(hColumn.getName(), hColumn.getValue());
				}
			}
//			if(topSpeed!=null && downSpeed!=null && delay!=null && serverIp!=null && protocol!=null && topThread!=null && downThread!=null){
				if(topSpeed.contains("dBm") || topSpeed.contains("信号强度") || downSpeed.contains("dBm") || downSpeed.contains("信号强度")){
					continue;
				}
				myData.put("max_upstream_rate", topSpeed);
				myData.put("mean_upstream_rate", topSpeed);
				myData.put("max_downstream_rate", downSpeed);
				myData.put("mean_downstream_rate", downSpeed);
				if(max_down.equals("") && !downSpeed.isEmpty()){
					max_down = downSpeed;
					myData.put("max_downstream_rate", max_down);
				}
				if(max_top.equals("") && !topSpeed.isEmpty()){
					max_top = topSpeed;
					myData.put("max_upstream_rate", max_top);
				}
				myData.put("protocol",protocol);
				myData.put("downThread", downThread);
				myData.put("topThread", topThread);
				myData.put("e_time_delay", delay);
				myData.put("server_ip", serverIp);
				lastKey = row.getKey();
//			}else{
//				continue;
//			}
				String lac1 = "";
				String cid1 = "";
				if(cell1.indexOf("/")!=-1 || cell1.indexOf("N/A")!=-1){
					String[] cellInfo = cell1.split("/");
					if(cellInfo[0]!=null && !cellInfo[0].equals("")){
						lac1 = cellInfo[0];
					}
					if(tac!=null && !tac.equals("")){
						lac1 = tac;
					}
					if(cellInfo[1]!=null && !cellInfo[1].equals("")){
						cid1 = cellInfo[1];
					}
					if(pci!=null && !pci.equals("")){
						cid1 = pci;
					}
					if(tac!=null && !tac.equals("") && cellInfo[0]!=null && !cellInfo[0].equals("")){
						lac1 = cellInfo[0]+"|"+tac;
					}
					if(pci!=null && !pci.equals("") && cellInfo[1]!=null && !cellInfo[1].equals("")){
						cid1 = cellInfo[1]+"|"+pci;
					}
				}
				String lac2 = "";
				String cid2 = "";
				if(cell2.indexOf("/")!=-1 || cell2.indexOf("N/A")!=-1){
					String[] cellInfo = cell2.split("/");
					if(cellInfo[0]!=null && !cellInfo[0].equals("")){
						lac2 = cellInfo[0];
					}
					if(tac!=null && !tac.equals("")){
						lac2 = tac;
					}
					if(cellInfo[1]!=null && !cellInfo[1].equals("")){
						cid2 = cellInfo[1];
					}
					if(pci!=null && !pci.equals("")){
						cid2 = pci;
					}
					if(tac!=null && !tac.equals("") && cellInfo[0]!=null && !cellInfo[0].equals("")){
						lac2 = cellInfo[0]+"|"+tac;
					}
					if(pci!=null && !pci.equals("") && cellInfo[1]!=null && !cellInfo[1].equals("")){
						cid2 = cellInfo[1]+"|"+pci;
					}
				}
				//网络(1)信息存在时 当前数据返回的是网络(1)的信息
				if(flag1 && netWorkType1.equals(netWorkType)){
					myData.put("sinr", sinr1);
					myData.put("networktype", netWorkType1);
					myData.put("signal", signal1);
					myData.put("lac", lac1);
					myData.put("cid", cid1);
				}
				//网络(2)信息存在时 当前数据返回的是网络(2)的信息
				if(flag2 && netWorkType2.equals(netWorkType)){
					myData.put("sinr", sinr2);
					myData.put("networktype", netWorkType2);
					myData.put("signal", signal2);
					myData.put("lac", lac2);
					myData.put("cid", cid2);
				}
				if(!(flag1 && netWorkType.equals(netWorkType1)) && !(flag2 && netWorkType.equals(netWorkType2))){
					myData.put("sinr", sinr);
					myData.put("networktype", netWorkType);
					myData.put("signal", signal);
					if(lac!=null && !lac.equals("")){
						myData.put("lac", lac);
					}
					if(tac!=null && !tac.equals("")){
						lac2 = tac;
						myData.put("lac", tac);
					}
					if(cid!=null && !cid.equals("")){
						myData.put("cid", cid);
					}
					if(pci!=null && !pci.equals("")){
						cid2 = pci;
						myData.put("cid", pci);
					}
					if(tac!=null && !tac.equals("") && lac!=null && !lac.equals("")){
						lac2 = lac+"|"+tac;
						myData.put("lac", lac+"|"+tac);	
					}
					if(pci!=null && !pci.equals("") && cid!=null && !cid.equals("")){
						cid2 = cid+"|"+pci;
						myData.put("cid", cid+"|"+pci);	
					}
				}
				myData.put("sinr1", sinr1);
				myData.put("networktype1", netWorkType1);
				myData.put("signal1", signal1);
				myData.put("lac1", lac1);
				myData.put("cid1", cid1);
				
				myData.put("sinr2", sinr2);
				myData.put("networktype2", netWorkType2);
				myData.put("signal2", signal2);
				myData.put("lac2", lac2);
				myData.put("cid2", cid2);
			myData.put("GSMLAC", lac);
			myData.put("GSMCID", cid);
			myData.put("LTETAC", tac);
			myData.put("LTEPCI", pci);
			if(netType.indexOf("LTE")!=-1){
				myData.put("LTESignal", signal);
			}else{
				myData.put("GSMSignal", signal);
			}
			if(!lac.isEmpty() && !cid.isEmpty()){
				cell_info = lac+","+cid;
			}
			
			myData.put("cell_info", cell_info);
			myData.put("test_location", province+city+district+street+street_no);
			myData.put("testing_time", testTime);
			myData.put("terminal_model", deviceType);
			myData.put("software_edition",toolsVersion);
			myData.put("imei", imei);
			myData.put("testSpeed",speed);
			myData.put("file_size",fileSize);
			myData.put("file_name", fileName);
			myData.put("file_path", filePath);
			myData.put("file_lastModifies", fileLastModifies);
			detail.add(myData);
			count++;
			if(count>max){
				break;
			}
		}
		respResult.put("total",max);
		respResult.put("detail",detail);
		respResult.put("last_key",lastKey);
		return respResult;
	}
	
	/**
	 * 网页浏览02001
	 * @author Ocean
	 * @param province
	 * @param city
	 * @param district
	 * @param orgs
	 * @param limit 
	 * @param testTypes
	 */
	public Map<String, Object> getTestDataByOrg(String type, String menuType, String startKey, String orgs, Date startDate, Date endDate, String limit) throws Exception{
		Integer max = Integer.parseInt(limit);
		Map<String,Object> respResult = new HashMap<String,Object> ();
		ArrayList<HashMap<String, String>> detail = new ArrayList<HashMap<String, String>>();
		Keyspace keyspaceOperator = null;
		try{
			keyspaceOperator = HFactory.createKeyspace(orgs, KeySpaceFactory.cluster);
		}catch (Exception e){
			e.printStackTrace();
		}
		RangeSlicesQuery<String, String, String> rangeQuery = HFactory
				.createRangeSlicesQuery(keyspaceOperator, KeySpaceFactory.stringSerializer, KeySpaceFactory.stringSerializer, KeySpaceFactory.stringSerializer);
		
		rangeQuery.setColumnFamily(type);
		rangeQuery.setRowCount(200);
		if(startKey==null || startKey.equals("")){
			startKey = "";
		}
		rangeQuery.setKeys(startKey, "");
		//IOS   平均时延 = 平均时长(ms)  无80%加载时长
		
		rangeQuery.setColumnNames("网络类型","网络制式","Network Type","network type","信号强度","Signal Strength","小区信息","Network Cell","SINR","网络(1)SINR","Network(1) SINR","网络(1)信号强度","Network(1) Signal Strength","网络(1)小区信息","Network(1) Cell","网络(1)网络制式","Network(1) Type","Network (1) standard","Network (1) standard","网络(2)SINR","Network(2) SINR","网络(2)信号强度","Network(2) Signal Strength","网络(2)小区信息","Network(2) Cell","网络(2)网络制式","Network(2) Type","Network (2) standard","终端类型","终端型号","Terminal Model","terminal model","Terminal Type","Device Type","Terminal Mode","Device Model","固件版本","软件版本号","OS version","data_time","平均速率(Kbps)","Avg Speed(Kbps)","imei","LAC","CID","LAC/CID","TAC","PCI","TAC/PCI","cassandra_province","cassandra_city","cassandra_district","cassandra_street","cassandra_street_number","fileName","fileSize","filePath","file_lastModifies","网址","地址","Website","address","加载80%平均时长","80% Avg Time","平均时长(ms)","平均时延(ms)","平均时延","Avg Time(ms)","Latency","Delay","Avg Latency(ms)","吞吐量(KB)");
		QueryResult<OrderedRows<String, String, String>> result = rangeQuery
				.execute();
		OrderedRows<String, String, String> oRows = result.get();
		int count = 1;
		String lastKey = "";
		List<Row<String, String, String>> rowList = oRows.getList();
		for (Row<String, String, String> row : rowList) {
			if(row.getKey().equals(startKey)){
				continue;
			}
			System.out.println(row.getKey());
			HashMap<String, String> myData = new HashMap<String, String>();
			
			List<HColumn<String, String>> columns = row.getColumnSlice()
					.getColumns();
			String speed = "";

			String lac = "";
			String cid = "";
			String tac = "";
			String pci = "";
			String netType = "";
			String testTime = "";
			String deviceType = "";
			String toolsVersion = "";
			String imei = "";
			String cell_info = "";
			
			String province = "";
			String city = "";
			String district = "";
			String street = "";
			String street_no = "";
			
			String fileName = "";
			String filePath = "";
			String fileSize = "";
			String fileLastModifies = "";
			
			String address = "";
			String eighty_loading_time = "";
			String full_loading_time = "";
			String avg_speed = "";
			String modelvalue = "";
			boolean flag1 = false;//判断小区1网络制式是否存在
			boolean flag2 = false;//小区2
			String sinr = "";//SINR
			String netWorkType = "";//网络类型 或者 网络制式
			String signal = "";//信号强度
			
			String sinr1 = "";
			String sinr2 = "";
			String netWorkType1 = "";
			String netWorkType2 = "";
			String cell1 = "";
			String cell2 = "";
			String signal1 = "";
			String signal2 = "";
			
			for (HColumn<String, String> hColumn : columns) {
				String name = hColumn.getName();
				if(name.contains("终端类型") || name.contains("终端型号") || "Device Type".equals(hColumn.getName()) || "Terminal Model".equals(hColumn.getName()) || "Terminal Type".equals(hColumn.getName()) || "Terminal Mode".equals(hColumn.getName()) || "Device Model".equals(hColumn.getName()) || "terminal model".equals(hColumn.getName())){
					modelvalue = hColumn.getValue();
				}
				if(name.contains("网络(1)网络制式") || name.contains("Network(1) Type") || name.contains("Network (1) standard")){
					flag1 = true;
				}
				if(name.contains("网络(2)网络制式") || name.contains("Network(2) Type") || name.contains("Network (2) standard")){
					flag2 = true;
				}
				if(name.contains("吞吐量")){
					System.out.println(name+":"+hColumn.getValue());
				}
			}
			if(menuType.equals("ios") && modelvalue!=null && !modelvalue.toLowerCase().contains("iphone")){
				continue;
			}else if(menuType.equals("android") && modelvalue!=null && modelvalue.toLowerCase().contains("iphone")){
				continue;
			}
			for (HColumn<String, String> hColumn : columns) {
				if("平均速率(Kbps)".equals(hColumn.getName()) || "Avg Speed(Kbps)".equals(hColumn.getName())){
					speed = hColumn.getValue()==null?"":hColumn.getValue();
					avg_speed = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("信号强度".equals(hColumn.getName()) || "Signal Strength".equals(hColumn.getName())){
					signal = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("data_time".equals(hColumn.getName())){
					testTime = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("终端类型".equals(hColumn.getName()) || "终端型号".equals(hColumn.getName()) || "Device Type".equals(hColumn.getName()) || "Terminal Model".equals(hColumn.getName()) || "Terminal Type".equals(hColumn.getName()) || "Terminal Mode".equals(hColumn.getName()) || "Device Model".equals(hColumn.getName()) || "terminal model".equals(hColumn.getName())){
					deviceType = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("固件版本".equals(hColumn.getName())||"软件版本号".equals(hColumn.getName()) || "OS version".equals(hColumn.getName())){
					toolsVersion = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("imei".equals(hColumn.getName())){
					imei = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("fileName".equals(hColumn.getName())){
					fileName = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("filePath".equals(hColumn.getName())){
					filePath = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("fileSize".equals(hColumn.getName())){
					fileSize = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("file_lastModifies".equals(hColumn.getName())){
					fileLastModifies = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("cassandra_province".equals(hColumn.getName())){
					province = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("cassandra_city".equals(hColumn.getName())){
					city = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("cassandra_district".equals(hColumn.getName())){
					district = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("cassandra_street".equals(hColumn.getName())){
					street = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("cassandra_street_no".equals(hColumn.getName())){
					street_no = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("LAC".equals(hColumn.getName())){
					String lacStr = hColumn.getValue()==null?"":hColumn.getValue();
					if(lacStr.indexOf("=")!=-1){
						lac = lacStr.substring(lacStr.indexOf("=")+1,lacStr.length());
					}else{
						lac = lacStr;
					}
				}else if("CID".equals(hColumn.getName())){
					String cidStr = hColumn.getValue()==null?"":hColumn.getValue();
					if(cidStr.indexOf("=")!=-1){
						cid = cidStr.substring(cidStr.indexOf("=")+1,cidStr.length());
					}else{
						cid = cidStr;
					}
				}else if("LAC/CID".equals(hColumn.getName())){
					String lacCid = hColumn.getValue()==null?"":hColumn.getValue();
					if(lacCid.indexOf("/")!=-1 || lacCid.indexOf("N/A")!=-1){
						String[] cellInfo = lacCid.split("/");
						lac = cellInfo[0];
						cid = cellInfo[1];
					}
				}else if("TAC".equals(hColumn.getName())){
					String tacStr = hColumn.getValue()==null?"":hColumn.getValue();
					if(tacStr.indexOf("=")!=-1){
						tac = tacStr.substring(tacStr.indexOf("=")+1,tacStr.length()-1);
					}else{
						tac = tacStr;
					}
				}else if("PCI".equals(hColumn.getName())){
					String pciStr = hColumn.getValue()==null?"":hColumn.getValue();
					if(pciStr.indexOf("=")!=-1){
						pci = pciStr.substring(pciStr.indexOf("=")+1,pciStr.length()-1);
					}else{
						pci = pciStr;
					}
				}else if("TAC/PCI".equals(hColumn.getName())){
					String tacPci = hColumn.getValue()==null?"":hColumn.getValue();
					if(tacPci.equals("N/A/N/A")){
						tac = "N/A";
						pci = "N/A";
					}else{
						if(tacPci.indexOf("/")!=-1 || tacPci.indexOf("N/A")!=-1){
							String[] cellInfo = tacPci.split("/");
							tac = cellInfo[0];
							pci = cellInfo[1];
						}
					}
				}else if("地址".equals(hColumn.getName()) || "网址".equals(hColumn.getName()) || "Website".equals(hColumn.getName()) || "address".equals(hColumn.getName())){
					address = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("加载80%平均时长".equals(hColumn.getName()) || "80% Avg Time".equals(hColumn.getName())){
					eighty_loading_time = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("平均时长(ms)".equals(hColumn.getName()) || "平均时延".equals(hColumn.getName()) || "平均时延(ms)".equals(hColumn.getName()) || "Avg Latency(ms)".equals(hColumn.getName()) || "Avg Time(ms)".equals(hColumn.getName()) || "Latency".equals(hColumn.getName()) || "Delay".equals(hColumn.getName())){
					full_loading_time = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("网络类型".equals(hColumn.getName()) || "网络制式".equals(hColumn.getName()) || "Network Type".equals(hColumn.getName()) || "network type".equals(hColumn.getName())){
					netWorkType = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("SINR".equals(hColumn.getName())){
					sinr = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("网络(1)网络制式".equals(hColumn.getName()) || "Network(1) Type".equals(hColumn.getName()) || "Network (1) standard".equals(hColumn.getName())){
					netWorkType1 = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("网络(1)信号强度".equals(hColumn.getName()) || "Network(1) Signal Strength".equals(hColumn.getName())){
					signal1 = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("网络(1)小区信息".equals(hColumn.getName()) || "Network(1) Cell".equals(hColumn.getName()) || "Network(1)".equals(hColumn.getName())){
					cell1 = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("网络(1)SINR".equals(hColumn.getName()) || "Network(1) SINR".equals(hColumn.getName())){
					sinr1 = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("网络(2)网络制式".equals(hColumn.getName()) || "Network(2) Type".equals(hColumn.getName()) || "Network (2) standard".equals(hColumn.getName())){
					netWorkType2 = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("网络(2)信号强度".equals(hColumn.getName()) || "Network(2) Signal Strength".equals(hColumn.getName())){
					signal2 = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("网络(2)小区信息".equals(hColumn.getName()) || "Network(2) Cell".equals(hColumn.getName())){
					cell2 = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("网络(2)SINR".equals(hColumn.getName()) || "Network(2) SINR".equals(hColumn.getName())){
					sinr2 = hColumn.getValue()==null?"":hColumn.getValue();
				}else{
					myData.put(hColumn.getName(), hColumn.getValue());
				}
			}

//			if(address!=null && !address.equals("") && eighty_loading_time!=null && !eighty_loading_time.equals("") && full_loading_time!=null && !full_loading_time.equals("") && avg_speed!=null && !avg_speed.equals("")){
				myData.put("access_address", address);
				myData.put("80_loading_time", eighty_loading_time);
				myData.put("100_loading_time", full_loading_time);
				myData.put("reference_rate", avg_speed);
				lastKey = row.getKey();
//			}else{
//				continue;
//			}
			String lac1 = "";
			String cid1 = "";
			if(cell1.indexOf("/")!=-1 || cell1.indexOf("N/A")!=-1){
				String[] cellInfo = cell1.split("/");
				if(cellInfo[0]!=null && !cellInfo[0].equals("")){
					lac1 = cellInfo[0];
				}
				if(tac!=null && !tac.equals("")){
					lac1 = tac;
				}
				if(cellInfo[1]!=null && !cellInfo[1].equals("")){
					cid1 = cellInfo[1];
				}
				if(pci!=null && !pci.equals("")){
					cid1 = pci;
				}
				if(tac!=null && !tac.equals("") && cellInfo[0]!=null && !cellInfo[0].equals("")){
					lac1 = cellInfo[0]+"|"+tac;
				}
				if(pci!=null && !pci.equals("") && cellInfo[1]!=null && !cellInfo[1].equals("")){
					cid1 = cellInfo[1]+"|"+pci;
				}
			}
			String lac2 = "";
			String cid2 = "";
			if(cell2.indexOf("/")!=-1 || cell2.indexOf("N/A")!=-1){
				String[] cellInfo = cell2.split("/");
				if(cellInfo[0]!=null && !cellInfo[0].equals("")){
					lac2 = cellInfo[0];
				}
				if(tac!=null && !tac.equals("")){
					lac2 = tac;
				}
				if(cellInfo[1]!=null && !cellInfo[1].equals("")){
					cid2 = cellInfo[1];
				}
				if(pci!=null && !pci.equals("")){
					cid2 = pci;
				}
				if(tac!=null && !tac.equals("") && cellInfo[0]!=null && !cellInfo[0].equals("")){
					lac2 = cellInfo[0]+"|"+tac;
				}
				if(pci!=null && !pci.equals("") && cellInfo[1]!=null && !cellInfo[1].equals("")){
					cid2 = cellInfo[1]+"|"+pci;
				}
			}
			//网络(1)信息存在时 当前数据返回的是网络(1)的信息
			if(flag1 && netWorkType1.equals(netWorkType)){
				myData.put("sinr", sinr1);
				myData.put("networktype", netWorkType1);
				myData.put("signal", signal1);
				myData.put("lac", lac1);
				myData.put("cid", cid1);
			}
			//网络(2)信息存在时 当前数据返回的是网络(2)的信息
			if(flag2 && netWorkType2.equals(netWorkType)){
				myData.put("sinr", sinr2);
				myData.put("networktype", netWorkType2);
				myData.put("signal", signal2);
				myData.put("lac", lac2);
				myData.put("cid", cid2);
			}
			if(!(flag1 && netWorkType.equals(netWorkType1)) && !(flag2 && netWorkType.equals(netWorkType2))){
				myData.put("sinr", sinr);
				myData.put("networktype", netWorkType);
				myData.put("signal", signal);
				if(lac!=null && !lac.equals("")){
					myData.put("lac", lac);
				}
				if(tac!=null && !tac.equals("")){
					lac2 = tac;
					myData.put("lac", tac);
				}
				if(cid!=null && !cid.equals("")){
					myData.put("cid", cid);
				}
				if(pci!=null && !pci.equals("")){
					cid2 = pci;
					myData.put("cid", pci);
				}
				if(tac!=null && !tac.equals("") && lac!=null && !lac.equals("")){
					lac2 = lac+"|"+tac;
					myData.put("lac", lac+"|"+tac);	
				}
				if(pci!=null && !pci.equals("") && cid!=null && !cid.equals("")){
					cid2 = cid+"|"+pci;
					myData.put("cid", cid+"|"+pci);	
				}
			}
			myData.put("sinr1", sinr1);
			myData.put("networktype1", netWorkType1);
			myData.put("signal1", signal1);
			myData.put("lac1", lac1);
			myData.put("cid1", cid1);
			
			myData.put("sinr2", sinr2);
			myData.put("networktype2", netWorkType2);
			myData.put("signal2", signal2);
			myData.put("lac2", lac2);
			myData.put("cid2", cid2);
			myData.put("GSMLAC", lac);
			myData.put("GSMCID", cid);
			myData.put("LTETAC", tac);
			myData.put("LTEPCI", pci);
			if(netType.indexOf("LTE")!=-1){
				myData.put("LTESignal", signal);
			}else{
				myData.put("GSMSignal", signal);
			}
			if(!lac.isEmpty() && !cid.isEmpty()){
				cell_info = lac+","+cid;
			}
			
			myData.put("cell_info", cell_info);
			myData.put("test_location", province+city+district+street+street_no);
			myData.put("testing_time", testTime);
			myData.put("terminal_model", deviceType);
			myData.put("software_edition",toolsVersion);
			myData.put("imei", imei);
			myData.put("testSpeed",speed);
			myData.put("file_size",fileSize);
			myData.put("file_name", fileName);
			myData.put("file_path", filePath);
			myData.put("file_lastModifies", fileLastModifies);
			detail.add(myData);
			count++;
			if(count>max){
				break;
			}
		}
		respResult.put("total",max);
		respResult.put("detail",detail);
		respResult.put("last_key",lastKey);
		return respResult;
	}
	
	/**
	 * Http 文件上传或文件下载 03001
	 * @author Ocean
	 * @param province
	 * @param city
	 * @param district
	 * @param orgs
	 * @param limit 
	 * @param testTypes
	 */
	public Map<String, Object> getTestDataByHttp(String type, String menuType,String httptype, String startKey, String orgs, Date startDate, Date endDate, String limit) throws Exception{
		Integer max = Integer.parseInt(limit);
		Map<String,Object> respResult = new HashMap<String,Object> ();
		ArrayList<HashMap<String, String>> detail = new ArrayList<HashMap<String, String>>();
		Keyspace keyspaceOperator = null;
		try{
			keyspaceOperator = HFactory.createKeyspace(orgs, KeySpaceFactory.cluster);
		}catch (Exception e){
			e.printStackTrace();
		}
		RangeSlicesQuery<String, String, String> rangeQuery = HFactory
				.createRangeSlicesQuery(keyspaceOperator, KeySpaceFactory.stringSerializer, KeySpaceFactory.stringSerializer, KeySpaceFactory.stringSerializer);
		//httptype 为upload 并且为ios时 columnfamily为03011
		if(menuType!=null && "ios".equals(menuType) && httptype!=null && "upload".equals(httptype)){
			rangeQuery.setColumnFamily("03011");	
		}else{
			rangeQuery.setColumnFamily(type);	
		}
		
		rangeQuery.setRowCount(200);
		if(startKey==null || startKey.equals("")){
			startKey = "";
		}
		
		rangeQuery.setKeys(startKey, "");
		//时长(s)  =  时长（s）
		rangeQuery.setColumnNames("网络类型","网络制式","Network Type","network type","信号强度","Signal Strength","小区信息","Network Cell","SINR","网络(1)SINR","Network(1) SINR","网络(1)信号强度","Network(1) Signal Strength","网络(1)小区信息","Network(1) Cell","网络(1)网络制式","Network(1) Type","Network (1) standard","Network (1) standard","网络(2)SINR","Network(2) SINR","网络(2)信号强度","Network(2) Signal Strength","网络(2)小区信息","Network(2) Cell","网络(2)网络制式","Network(2) Type","Network (2) standard","终端类型","终端型号","Terminal Model","terminal model","Terminal Type","Device Type","Terminal Mode","Device Model","固件版本","软件版本号","OS Version","data_time","平均速率(Kbps)","Average Speed (Kbps)","imei","LAC","CID","LAC/CID","TAC","PCI","TAC/PCI","cassandra_province","cassandra_city","cassandra_district","cassandra_street","cassandra_street_number","fileName","fileSize","filePath","file_lastModifies","类型","Type","文件","File","文件大小","File Size","时长(s)","时长（s）","duration","平均速率（Mbps）","平均速率(Mbps)","平均速率(Mbps)","Average Speed (Mbps)","吞吐量(KB)");
		QueryResult<OrderedRows<String, String, String>> result = rangeQuery
				.execute();
		OrderedRows<String, String, String> oRows = result.get();
		int count = 1;
		String lastKey = "";
		List<Row<String, String, String>> rowList = oRows.getList();
		for (Row<String, String, String> row : rowList) {
			if(row.getKey().equals(startKey)){
				continue;
			}
			
			System.out.println(row.getKey());
			HashMap<String, String> myData = new HashMap<String, String>();
			
			List<HColumn<String, String>> columns = row.getColumnSlice()
					.getColumns();
			String lac = "";
			String cid = "";
			String tac = "";
			String pci = "";
			String netType = "";
			String testTime = "";
			String deviceType = "";
			String toolsVersion = "";
			String imei = "";
			String cell_info = "";
			
			String province = "";
			String city = "";
			String district = "";
			String street = "";
			String street_no = "";
			
			String fileName = "";
			String filePath = "";
			String fileSize = "";
			String fileLastModifies = "";
			
			String fileType = "";
			String destFile = "";
			String destFileSize = "";
			String time = "";
			String speed = "";
			String modelvalue = "";
			String httpvalue = "";
			boolean flag1 = false;//判断小区1网络制式是否存在
			boolean flag2 = false;//小区2
			String sinr = "";//SINR
			String netWorkType = "";//网络类型 或者 网络制式
			String signal = "";//信号强度
			
			String sinr1 = "";
			String sinr2 = "";
			String netWorkType1 = "";
			String netWorkType2 = "";
			String cell1 = "";
			String cell2 = "";
			String signal1 = "";
			String signal2 = "";
			for (HColumn<String, String> hColumn : columns) {
				String name = hColumn.getName();
				if(name.contains("终端类型") || name.contains("终端型号") || "Device Type".equals(hColumn.getName()) || "Terminal Model".equals(hColumn.getName()) || "Terminal Type".equals(hColumn.getName()) || "Terminal Mode".equals(hColumn.getName()) || "Device Model".equals(hColumn.getName()) || "terminal model".equals(hColumn.getName())){
					modelvalue = hColumn.getValue();
				}
				if("类型".equals(name) || "Type".equals(name)){
					httpvalue = hColumn.getValue();
				}
				if(name.contains("网络(1)网络制式") || name.contains("Network(1) Type") || name.contains("Network (1) standard")){
					flag1 = true;
				}
				if(name.contains("网络(2)网络制式") || name.contains("Network(2) Type") || name.contains("Network (2) standard")){
					flag2 = true;
				}
				if(name.contains("吞吐量")){
					System.out.println(name+":"+hColumn.getValue());
				}
			}
			if(menuType.equals("ios") && modelvalue!=null && !modelvalue.toLowerCase().contains("iphone")){
				continue;
			}else if(menuType.equals("android") && modelvalue!=null && modelvalue.toLowerCase().contains("iphone")){
				continue;
			}else if(httptype.equals("upload") && httpvalue!=null && (httpvalue.equals("下载") || httpvalue.equals("") || httpvalue.equals("Download"))){
				continue;
			}else if(httptype.equals("download") && httpvalue!=null && (httpvalue.equals("上传")||httpvalue.equals("Upload"))){
				continue;
			}
			for (HColumn<String, String> hColumn : columns) {
				if("平均速率(Kbps)".equals(hColumn.getName()) || "平均速率(Mbps)".equals(hColumn.getName()) ||"平均速率（Mbps）".equals(hColumn.getName()) || "Average Speed (Mbps)".equals(hColumn.getName()) || "Average Speed (Kbps)".equals(hColumn.getName())){
					speed = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("信号强度".equals(hColumn.getName()) || "Signal Strength".equals(hColumn.getName())){
					signal = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("data_time".equals(hColumn.getName())){
					testTime = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("终端类型".equals(hColumn.getName()) || "终端型号".equals(hColumn.getName()) || "Device Type".equals(hColumn.getName()) || "Terminal Model".equals(hColumn.getName()) || "Terminal Type".equals(hColumn.getName()) || "Terminal Mode".equals(hColumn.getName()) || "Device Model".equals(hColumn.getName()) || "terminal model".equals(hColumn.getName())){
					deviceType = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("固件版本".equals(hColumn.getName())||"软件版本号".equals(hColumn.getName()) || "OS Version".equals(hColumn.getName())){
					toolsVersion = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("imei".equals(hColumn.getName())){
					imei = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("fileName".equals(hColumn.getName())){
					fileName = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("filePath".equals(hColumn.getName())){
					filePath = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("fileSize".equals(hColumn.getName())){
					fileSize = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("file_lastModifies".equals(hColumn.getName())){
					fileLastModifies = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("cassandra_province".equals(hColumn.getName())){
					province = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("cassandra_city".equals(hColumn.getName())){
					city = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("cassandra_district".equals(hColumn.getName())){
					district = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("cassandra_street".equals(hColumn.getName())){
					street = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("cassandra_street_no".equals(hColumn.getName())){
					street_no = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("LAC".equals(hColumn.getName())){
					String lacStr = hColumn.getValue()==null?"":hColumn.getValue();
					if(lacStr.indexOf("=")!=-1){
						lac = lacStr.substring(lacStr.indexOf("=")+1,lacStr.length());
					}else{
						lac = lacStr;
					}
				}else if("CID".equals(hColumn.getName())){
					String cidStr = hColumn.getValue()==null?"":hColumn.getValue();
					if(cidStr.indexOf("=")!=-1){
						cid = cidStr.substring(cidStr.indexOf("=")+1,cidStr.length());
					}else{
						cid = cidStr;
					}
				}else if("LAC/CID".equals(hColumn.getName())){
					String lacCid = hColumn.getValue()==null?"":hColumn.getValue();
					if(lacCid.indexOf("/")!=-1 || lacCid.indexOf("N/A")!=-1){
						String[] cellInfo = lacCid.split("/");
						lac = cellInfo[0];
						cid = cellInfo[1];
					}
				}else if("TAC".equals(hColumn.getName())){
					String tacStr = hColumn.getValue()==null?"":hColumn.getValue();
					if(tacStr.indexOf("=")!=-1){
						tac = tacStr.substring(tacStr.indexOf("=")+1,tacStr.length()-1);
					}else{
						tac = tacStr;
					}
				}else if("PCI".equals(hColumn.getName())){
					String pciStr = hColumn.getValue()==null?"":hColumn.getValue();
					if(pciStr.indexOf("=")!=-1){
						pci = pciStr.substring(pciStr.indexOf("=")+1,pciStr.length()-1);
					}else{
						pci = pciStr;
					}
				}else if("TAC/PCI".equals(hColumn.getName())){
					String tacPci = hColumn.getValue()==null?"":hColumn.getValue();
					if(tacPci.equals("N/A/N/A")){
						tac = "N/A";
						pci = "N/A";
					}else{
						if(tacPci.indexOf("/")!=-1 || tacPci.indexOf("N/A")!=-1){
							String[] cellInfo = tacPci.split("/");
							tac = cellInfo[0];
							pci = cellInfo[1];
						}
					}
				}else if("类型".equals(hColumn.getName()) || "Type".equals(hColumn.getName())){
					fileType = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("文件".equals(hColumn.getName()) || "File".equals(hColumn.getName())){
					destFile = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("文件大小".equals(hColumn.getName()) || "File Size".equals(hColumn.getName())){
					destFileSize = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("时长(s)".equals(hColumn.getName()) || "时长（s）".equals(hColumn.getName()) || "duration".equals(hColumn.getName())){
					time = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("网络类型".equals(hColumn.getName()) || "网络制式".equals(hColumn.getName()) || "Network Type".equals(hColumn.getName()) || "network type".equals(hColumn.getName())){
					netWorkType = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("SINR".equals(hColumn.getName())){
					sinr = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("网络(1)网络制式".equals(hColumn.getName()) || "Network(1) Type".equals(hColumn.getName()) || "Network (1) standard".equals(hColumn.getName())){
					netWorkType1 = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("网络(1)信号强度".equals(hColumn.getName()) || "Network(1) Signal Strength".equals(hColumn.getName())){
					signal1 = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("网络(1)小区信息".equals(hColumn.getName()) || "Network(1) Cell".equals(hColumn.getName()) || "Network(1)".equals(hColumn.getName())){
					cell1 = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("网络(1)SINR".equals(hColumn.getName()) || "Network(1) SINR".equals(hColumn.getName())){
					sinr1 = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("网络(2)网络制式".equals(hColumn.getName()) || "Network(2) Type".equals(hColumn.getName()) || "Network (2) standard".equals(hColumn.getName())){
					netWorkType2 = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("网络(2)信号强度".equals(hColumn.getName()) || "Network(2) Signal Strength".equals(hColumn.getName())){
					signal2 = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("网络(2)小区信息".equals(hColumn.getName()) || "Network(2) Cell".equals(hColumn.getName())){
					cell2 = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("网络(2)SINR".equals(hColumn.getName()) || "Network(2) SINR".equals(hColumn.getName())){
					sinr2 = hColumn.getValue()==null?"":hColumn.getValue();
				}else{
					myData.put(hColumn.getName(), hColumn.getValue());
				}
			}
//			if(speed!=null && !speed.equals("") && destFile!=null && !destFile.equals("") && destFileSize!=null && time!=null && !time.equals("") && fileType!=null){
				myData.put("target_address", destFile);
				if(destFileSize.equals("") || destFileSize.equals("--") || !destFileSize.contains(".")){
					myData.put("resource_size", destFileSize);	
				}else
				{
					myData.put("resource_size", destFileSize.substring(destFileSize.lastIndexOf("/")+1, destFileSize.lastIndexOf(".")));
				}
				myData.put("mean_rate", speed);
				if(fileType!=null && (fileType.equals("上传") || fileType.equals("Upload"))){
					myData.put("uploading_time", time);
				}else{
					myData.put("downloading_time", time);
				}
				lastKey = row.getKey();
//			}else{
//				continue;	
//			}
				String lac1 = "";
				String cid1 = "";
				if(cell1.indexOf("/")!=-1 || cell1.indexOf("N/A")!=-1){
					String[] cellInfo = cell1.split("/");
					if(cellInfo[0]!=null && !cellInfo[0].equals("")){
						lac1 = cellInfo[0];
					}
					if(tac!=null && !tac.equals("")){
						lac1 = tac;
					}
					if(cellInfo[1]!=null && !cellInfo[1].equals("")){
						cid1 = cellInfo[1];
					}
					if(pci!=null && !pci.equals("")){
						cid1 = pci;
					}
					if(tac!=null && !tac.equals("") && cellInfo[0]!=null && !cellInfo[0].equals("")){
						lac1 = cellInfo[0]+"|"+tac;
					}
					if(pci!=null && !pci.equals("") && cellInfo[1]!=null && !cellInfo[1].equals("")){
						cid1 = cellInfo[1]+"|"+pci;
					}
				}
				String lac2 = "";
				String cid2 = "";
				if(cell2.indexOf("/")!=-1 || cell2.indexOf("N/A")!=-1){
					String[] cellInfo = cell2.split("/");
					if(cellInfo[0]!=null && !cellInfo[0].equals("")){
						lac2 = cellInfo[0];
					}
					if(tac!=null && !tac.equals("")){
						lac2 = tac;
					}
					if(cellInfo[1]!=null && !cellInfo[1].equals("")){
						cid2 = cellInfo[1];
					}
					if(pci!=null && !pci.equals("")){
						cid2 = pci;
					}
					if(tac!=null && !tac.equals("") && cellInfo[0]!=null && !cellInfo[0].equals("")){
						lac2 = cellInfo[0]+"|"+tac;
					}
					if(pci!=null && !pci.equals("") && cellInfo[1]!=null && !cellInfo[1].equals("")){
						cid2 = cellInfo[1]+"|"+pci;
					}
				}
				//网络(1)信息存在时 当前数据返回的是网络(1)的信息
				if(flag1 && netWorkType1.equals(netWorkType)){
					myData.put("sinr", sinr1);
					myData.put("networktype", netWorkType1);
					myData.put("signal", signal1);
					myData.put("lac", lac1);
					myData.put("cid", cid1);
				}
				//网络(2)信息存在时 当前数据返回的是网络(2)的信息
				if(flag2 && netWorkType2.equals(netWorkType)){
					myData.put("sinr", sinr2);
					myData.put("networktype", netWorkType2);
					myData.put("signal", signal2);
					myData.put("lac", lac2);
					myData.put("cid", cid2);
				}
				if(!(flag1 && netWorkType.equals(netWorkType1)) && !(flag2 && netWorkType.equals(netWorkType2))){
					myData.put("sinr", sinr);
					myData.put("networktype", netWorkType);
					myData.put("signal", signal);
					if(lac!=null && !lac.equals("")){
						myData.put("lac", lac);
					}
					if(tac!=null && !tac.equals("")){
						lac2 = tac;
						myData.put("lac", tac);
					}
					if(cid!=null && !cid.equals("")){
						myData.put("cid", cid);
					}
					if(pci!=null && !pci.equals("")){
						cid2 = pci;
						myData.put("cid", pci);
					}
					if(tac!=null && !tac.equals("") && lac!=null && !lac.equals("")){
						lac2 = lac+"|"+tac;
						myData.put("lac", lac+"|"+tac);	
					}
					if(pci!=null && !pci.equals("") && cid!=null && !cid.equals("")){
						cid2 = cid+"|"+pci;
						myData.put("cid", cid+"|"+pci);	
					}
				}
				myData.put("sinr1", sinr1);
				myData.put("networktype1", netWorkType1);
				myData.put("signal1", signal1);
				myData.put("lac1", lac1);
				myData.put("cid1", cid1);
				
				myData.put("sinr2", sinr2);
				myData.put("networktype2", netWorkType2);
				myData.put("signal2", signal2);
				myData.put("lac2", lac2);
				myData.put("cid2", cid2);
			myData.put("GSMLAC", lac);
			myData.put("GSMCID", cid);
			myData.put("LTETAC", tac);
			myData.put("LTEPCI", pci);
			if(netType.indexOf("LTE")!=-1){
				myData.put("LTESignal", signal);
			}else{
				myData.put("GSMSignal", signal);
			}
			if(!lac.isEmpty() && !cid.isEmpty()){
				cell_info = lac+","+cid;
			}
			
			myData.put("cell_info", cell_info);
			myData.put("test_location", province+city+district+street+street_no);
			myData.put("testing_time", testTime);
			myData.put("terminal_model", deviceType);
			myData.put("software_edition",toolsVersion);
			myData.put("imei", imei);
			myData.put("testSpeed",speed);
			myData.put("file_size",fileSize);
			myData.put("file_name", fileName);
			myData.put("file_path", filePath);
			myData.put("file_lastModifies", fileLastModifies);
			detail.add(myData);
			count++;
			if(count>max){
				break;
			}
		}
		respResult.put("total",Integer.parseInt(limit));
		respResult.put("detail",detail);
		respResult.put("last_key",lastKey);
		return respResult;
	}
	/**
	 * PING 04002
	 * @author Ocean
	 * @param province
	 * @param city
	 * @param district
	 * @param orgs
	 * @param limit 
	 * @param testTypes
	 */
	public Map<String, Object> getTestDataByPING(String type, String menuType, String startKey, String orgs, Date startDate, Date endDate, String limit) throws Exception{
		Integer max = Integer.parseInt(limit);
		Map<String,Object> respResult = new HashMap<String,Object> ();
		ArrayList<HashMap<String, String>> detail = new ArrayList<HashMap<String, String>>();
		Keyspace keyspaceOperator = null;
		try{
			keyspaceOperator = HFactory.createKeyspace(orgs, KeySpaceFactory.cluster);
		}catch (Exception e){
			e.printStackTrace();
		}
		RangeSlicesQuery<String, String, String> rangeQuery = HFactory
				.createRangeSlicesQuery(keyspaceOperator, KeySpaceFactory.stringSerializer, KeySpaceFactory.stringSerializer, KeySpaceFactory.stringSerializer);
		
		rangeQuery.setColumnFamily(type);
		rangeQuery.setRowCount(200);
		if(startKey==null || startKey.equals("")){
			startKey = "";
		}
		rangeQuery.setKeys(startKey, "");
		rangeQuery.setColumnNames("网络类型","网络制式","Network Type","network type","小区信息","Network cell","SINR","网络(1)SINR","Network(1) SINR","网络(1)信号强度","Network(1) Signal Strength","网络(1)小区信息","Network(1) Cell","网络(1)网络制式","Network(1) Type","Network (1) standard","Network (1) standard","网络(2)SINR","Network(2) SINR","网络(2)信号强度","Network(2) Signal Strength","网络(2)小区信息","Network(2) Cell","网络(2)网络制式","Network(2) Type","Network (2) standard","信号强度","Signal Strength","终端类型","终端型号","Terminal Model","terminal model","Terminal Type","Device Type","Terminal Mode","Device Model","固件版本","软件版本号","OS version","data_time","平均速率(Kbps)","Avg Speed(Kbps)","imei","LAC","CID","LAC/CID","TAC","PCI","TAC/PCI","cassandra_province","cassandra_city","cassandra_district","cassandra_street","cassandra_street_number","fileName","fileSize","filePath","file_lastModifies","地址","Address","address","最大时延","Max Latency","Max Latency(ms)","最小时延","Min Latency","Min Latency(ms)","平均时延","最大时延(ms)","the maximum delay(ms)","最小时延(ms)","the minimum delay(ms)","平均时延(ms)","时延","时延(ms)","Avg Latency","Avg Time(ms)","Latency","Delay","Avg Latency(ms)","重复次数","测试次数","Repetition");
		QueryResult<OrderedRows<String, String, String>> result = rangeQuery
				.execute();
		OrderedRows<String, String, String> oRows = result.get();
		int count = 1;
		String lastKey = "";
		List<Row<String, String, String>> rowList = oRows.getList();
		for (Row<String, String, String> row : rowList) {
			if(row.getKey().equals(startKey)){
				continue;
			}
			HashMap<String, String> myData = new HashMap<String, String>();
			
			List<HColumn<String, String>> columns = row.getColumnSlice()
					.getColumns();
			String speed = "";

			String lac = "";
			String cid = "";
			String tac = "";
			String pci = "";
			String netType = "";
			String testTime = "";
			String deviceType = "";
			String toolsVersion = "";
			String imei = "";
			String cell_info = "";
			
			String province = "";
			String city = "";
			String district = "";
			String street = "";
			String street_no = "";
			
			String fileName = "";
			String filePath = "";
			String fileSize = "";
			String fileLastModifies = "";
			
			String min_delay = "";
			String max_delay = "";
			String avg_delay = "";
			String testCount = "";
			String address = "";
			String modelvalue = "";
			boolean flag1 = false;//判断小区1网络制式是否存在
			boolean flag2 = false;//小区2
			String sinr = "";//SINR
			String netWorkType = "";//网络类型 或者 网络制式
			String signal = "";//信号强度
			
			String sinr1 = "";
			String sinr2 = "";
			String netWorkType1 = "";
			String netWorkType2 = "";
			String cell1 = "";
			String cell2 = "";
			String signal1 = "";
			String signal2 = "";
			for (HColumn<String, String> hColumn : columns) {
				String name = hColumn.getName();
				if(name.contains("终端类型") || name.contains("终端型号") || "Device Type".equals(hColumn.getName()) || "Terminal Model".equals(hColumn.getName()) || "Terminal Type".equals(hColumn.getName()) || "Terminal Mode".equals(hColumn.getName()) || "Device Model".equals(hColumn.getName()) || "terminal model".equals(hColumn.getName())){
					modelvalue = hColumn.getValue();
				}
				if(name.contains("网络(1)网络制式") || name.contains("Network(1) Type") || name.contains("Network (1) standard")){
					flag1 = true;
				}
				if(name.contains("网络(2)网络制式") || name.contains("Network(2) Type") || name.contains("Network (2) standard")){
					flag2 = true;
				}
			}
			if(menuType.equals("ios") && modelvalue!=null && !modelvalue.toLowerCase().contains("iphone")){
				continue;
			}else if(menuType.equals("android") && modelvalue!=null && modelvalue.toLowerCase().contains("iphone")){
				continue;
			}
			for (HColumn<String, String> hColumn : columns) {
				if("平均速率(Kbps)".equals(hColumn.getName()) || "Avg Speed(Kbps)".equals(hColumn.getName())){
					speed = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("信号强度".equals(hColumn.getName()) || "Signal Strength".equals(hColumn.getName())){
					signal = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("data_time".equals(hColumn.getName())){
					testTime = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("终端类型".equals(hColumn.getName()) || "终端型号".equals(hColumn.getName()) || "Device Type".equals(hColumn.getName()) || "Terminal Model".equals(hColumn.getName()) || "Terminal Type".equals(hColumn.getName()) || "Terminal Mode".equals(hColumn.getName()) || "Device Model".equals(hColumn.getName()) || "terminal model".equals(hColumn.getName())){
					deviceType = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("固件版本".equals(hColumn.getName())||"软件版本号".equals(hColumn.getName()) || "OS version".equals(hColumn.getName())){
					toolsVersion = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("imei".equals(hColumn.getName())){
					imei = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("fileName".equals(hColumn.getName())){
					fileName = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("filePath".equals(hColumn.getName())){
					filePath = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("fileSize".equals(hColumn.getName())){
					fileSize = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("file_lastModifies".equals(hColumn.getName())){
					fileLastModifies = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("cassandra_province".equals(hColumn.getName())){
					province = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("cassandra_city".equals(hColumn.getName())){
					city = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("cassandra_district".equals(hColumn.getName())){
					district = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("cassandra_street".equals(hColumn.getName())){
					street = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("cassandra_street_no".equals(hColumn.getName())){
					street_no = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("LAC".equals(hColumn.getName())){
					String lacStr = hColumn.getValue()==null?"":hColumn.getValue();
					if(lacStr.indexOf("=")!=-1){
						lac = lacStr.substring(lacStr.indexOf("=")+1,lacStr.length());
					}else{
						lac = lacStr;
					}
				}else if("CID".equals(hColumn.getName())){
					String cidStr = hColumn.getValue()==null?"":hColumn.getValue();
					if(cidStr.indexOf("=")!=-1){
						cid = cidStr.substring(cidStr.indexOf("=")+1,cidStr.length());
					}else{
						cid = cidStr;
					}
				}else if("LAC/CID".equals(hColumn.getName())){
					String lacCid = hColumn.getValue()==null?"":hColumn.getValue();
					if(lacCid.indexOf("/")!=-1 || lacCid.indexOf("N/A")!=-1){
						String[] cellInfo = lacCid.split("/");
						lac = cellInfo[0];
						cid = cellInfo[1];
					}
				}else if("TAC".equals(hColumn.getName())){
					String tacStr = hColumn.getValue()==null?"":hColumn.getValue();
					if(tacStr.indexOf("=")!=-1){
						tac = tacStr.substring(tacStr.indexOf("=")+1,tacStr.length()-1);
					}else{
						tac = tacStr;
					}
				}else if("PCI".equals(hColumn.getName())){
					String pciStr = hColumn.getValue()==null?"":hColumn.getValue();
					if(pciStr.indexOf("=")!=-1){
						pci = pciStr.substring(pciStr.indexOf("=")+1,pciStr.length()-1);
					}else{
						pci = pciStr;
					}
				}else if("TAC/PCI".equals(hColumn.getName())){
					String tacPci = hColumn.getValue()==null?"":hColumn.getValue();
					if(tacPci.equals("N/A/N/A")){
						tac = "N/A";
						pci = "N/A";
					}else{
						if(tacPci.indexOf("/")!=-1 || tacPci.indexOf("N/A")!=-1){
							String[] cellInfo = tacPci.split("/");
							tac = cellInfo[0];
							pci = cellInfo[1];
						}
					}
				}else if("地址".equals(hColumn.getName()) || "Address".equals(hColumn.getName()) || "address".equals(hColumn.getName()) ){
					address = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("平均时延(ms)".equals(hColumn.getName())||"平均时延".equals(hColumn.getName()) || "Avg Latency(ms)".equals(hColumn.getName()) || "Avg Time(ms)".equals(hColumn.getName()) || "Avg Latency".equals(hColumn.getName())){
					avg_delay = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("最大时延(ms)".equals(hColumn.getName())||"最大时延".equals(hColumn.getName()) || "Max Latency(ms)".equals(hColumn.getName()) || "Max Latency".equals(hColumn.getName()) || "the maximum delay(ms)".equals(hColumn.getName())){
					max_delay = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("最小时延(ms)".equals(hColumn.getName())||"最小时延".equals(hColumn.getName()) || "Min Latency(ms)".equals(hColumn.getName()) || "Min Latency".equals(hColumn.getName()) || "the minimum delay(ms)".equals(hColumn.getName())){
					min_delay = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("测试次数".equals(hColumn.getName())||"重复次数".equals(hColumn.getName()) || "Repetition".equals(hColumn.getName())){
					testCount = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("网络类型".equals(hColumn.getName()) || "网络制式".equals(hColumn.getName()) || "Network Type".equals(hColumn.getName()) || "network type".equals(hColumn.getName())){
					netWorkType = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("SINR".equals(hColumn.getName())){
					sinr = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("网络(1)网络制式".equals(hColumn.getName()) || "Network(1) Type".equals(hColumn.getName()) || "Network (1) standard".equals(hColumn.getName())){
					netWorkType1 = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("网络(1)信号强度".equals(hColumn.getName()) || "Network(1) Signal Strength".equals(hColumn.getName())){
					signal1 = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("网络(1)小区信息".equals(hColumn.getName()) || "Network(1) Cell".equals(hColumn.getName()) || "Network(1)".equals(hColumn.getName())){
					cell1 = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("网络(1)SINR".equals(hColumn.getName()) || "Network(1) SINR".equals(hColumn.getName())){
					sinr1 = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("网络(2)网络制式".equals(hColumn.getName()) || "Network(2) Type".equals(hColumn.getName()) || "Network (2) standard".equals(hColumn.getName())){
					netWorkType2 = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("网络(2)信号强度".equals(hColumn.getName()) || "Network(2) Signal Strength".equals(hColumn.getName())){
					signal2 = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("网络(2)小区信息".equals(hColumn.getName()) || "Network(2) Cell".equals(hColumn.getName())){
					cell2 = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("网络(2)SINR".equals(hColumn.getName()) || "Network(2) SINR".equals(hColumn.getName())){
					sinr2 = hColumn.getValue()==null?"":hColumn.getValue();
				}else{
					myData.put(hColumn.getName(), hColumn.getValue());
				}
			}

//			if(address!=null && !address.equals("") && avg_delay!=null && !avg_delay.equals("") && max_delay!=null && !max_delay.equals("") && min_delay!=null && !min_delay.equals("") && testCount!=null && !testCount.equals("")){
				myData.put("domain_address", address);
				myData.put("max_time_delay", max_delay);
				myData.put("minimal_time_delay", min_delay);
				myData.put("average_time_delay", avg_delay);
				myData.put("count", testCount);
				lastKey = row.getKey();
//			}else{
//				continue;
//			}
			
				String lac1 = "";
				String cid1 = "";
				if(cell1.indexOf("/")!=-1 || cell1.indexOf("N/A")!=-1){
					String[] cellInfo = cell1.split("/");
					if(cellInfo[0]!=null && !cellInfo[0].equals("")){
						lac1 = cellInfo[0];
					}
					if(tac!=null && !tac.equals("")){
						lac1 = tac;
					}
					if(cellInfo[1]!=null && !cellInfo[1].equals("")){
						cid1 = cellInfo[1];
					}
					if(pci!=null && !pci.equals("")){
						cid1 = pci;
					}
					if(tac!=null && !tac.equals("") && cellInfo[0]!=null && !cellInfo[0].equals("")){
						lac1 = cellInfo[0]+"|"+tac;
					}
					if(pci!=null && !pci.equals("") && cellInfo[1]!=null && !cellInfo[1].equals("")){
						cid1 = cellInfo[1]+"|"+pci;
					}
				}
				String lac2 = "";
				String cid2 = "";
				if(cell2.indexOf("/")!=-1 || cell2.indexOf("N/A")!=-1){
					String[] cellInfo = cell2.split("/");
					if(cellInfo[0]!=null && !cellInfo[0].equals("")){
						lac2 = cellInfo[0];
					}
					if(tac!=null && !tac.equals("")){
						lac2 = tac;
					}
					if(cellInfo[1]!=null && !cellInfo[1].equals("")){
						cid2 = cellInfo[1];
					}
					if(pci!=null && !pci.equals("")){
						cid2 = pci;
					}
					if(tac!=null && !tac.equals("") && cellInfo[0]!=null && !cellInfo[0].equals("")){
						lac2 = cellInfo[0]+"|"+tac;
					}
					if(pci!=null && !pci.equals("") && cellInfo[1]!=null && !cellInfo[1].equals("")){
						cid2 = cellInfo[1]+"|"+pci;
					}
				}
				//网络(1)信息存在时 当前数据返回的是网络(1)的信息
				if(flag1 && netWorkType1.equals(netWorkType)){
					myData.put("sinr", sinr1);
					myData.put("networktype", netWorkType1);
					myData.put("signal", signal1);
					myData.put("lac", lac1);
					myData.put("cid", cid1);
				}
				//网络(2)信息存在时 当前数据返回的是网络(2)的信息
				if(flag2 && netWorkType2.equals(netWorkType)){
					myData.put("sinr", sinr2);
					myData.put("networktype", netWorkType2);
					myData.put("signal", signal2);
					myData.put("lac", lac2);
					myData.put("cid", cid2);
				}
				if(!(flag1 && netWorkType.equals(netWorkType1)) && !(flag2 && netWorkType.equals(netWorkType2))){
					myData.put("sinr", sinr);
					myData.put("networktype", netWorkType);
					myData.put("signal", signal);
					if(lac!=null && !lac.equals("")){
						myData.put("lac", lac);
					}
					if(tac!=null && !tac.equals("")){
						lac2 = tac;
						myData.put("lac", tac);
					}
					if(cid!=null && !cid.equals("")){
						myData.put("cid", cid);
					}
					if(pci!=null && !pci.equals("")){
						cid2 = pci;
						myData.put("cid", pci);
					}
					if(tac!=null && !tac.equals("") && lac!=null && !lac.equals("")){
						lac2 = lac+"|"+tac;
						myData.put("lac", lac+"|"+tac);	
					}
					if(pci!=null && !pci.equals("") && cid!=null && !cid.equals("")){
						cid2 = cid+"|"+pci;
						myData.put("cid", cid+"|"+pci);	
					}
				}
				myData.put("sinr1", sinr1);
				myData.put("networktype1", netWorkType1);
				myData.put("signal1", signal1);
				myData.put("lac1", lac1);
				myData.put("cid1", cid1);
				
				myData.put("sinr2", sinr2);
				myData.put("networktype2", netWorkType2);
				myData.put("signal2", signal2);
				myData.put("lac2", lac2);
				myData.put("cid2", cid2);
			myData.put("GSMLAC", lac);
			myData.put("GSMCID", cid);
			myData.put("LTETAC", tac);
			myData.put("LTEPCI", pci);
			if(netType.indexOf("LTE")!=-1){
				myData.put("LTESignal", signal);
			}else{
				myData.put("GSMSignal", signal);
			}
			if(!lac.isEmpty() && !cid.isEmpty()){
				cell_info = lac+","+cid;
			}
			
			myData.put("cell_info", cell_info);
			myData.put("test_location", province+city+district+street+street_no);
			myData.put("testing_time", testTime);
			myData.put("terminal_model", deviceType);
			myData.put("software_edition",toolsVersion);
			myData.put("imei", imei);
			myData.put("testSpeed",speed);
			myData.put("file_size",fileSize);
			myData.put("file_name", fileName);
			myData.put("file_path", filePath);
			myData.put("file_lastModifies", fileLastModifies);
			detail.add(myData);
			count++;
			if(count>max){
				break;
			}
		}
		respResult.put("total",Integer.parseInt(limit));
		respResult.put("detail",detail);
		respResult.put("last_key",lastKey);
		return respResult;
	}
	/**
	 * video 02011
	 * @author Ocean
	 * @param province
	 * @param city
	 * @param district
	 * @param orgs
	 * @param limit 
	 * @param testTypes
	 */
	public Map<String, Object> getTestDataByVideoTest(String type, String menuType, String startKey, String orgs, Date startDate, Date endDate, String limit) throws Exception{
		Integer max = Integer.parseInt(limit);
		Map<String,Object> respResult = new HashMap<String,Object> ();
		ArrayList<HashMap<String, String>> detail = new ArrayList<HashMap<String, String>>();
		Keyspace keyspaceOperator = null;
		try{
			keyspaceOperator = HFactory.createKeyspace(orgs, KeySpaceFactory.cluster);
		}catch (Exception e){
			e.printStackTrace();
		}
		RangeSlicesQuery<String, String, String> rangeQuery = HFactory
				.createRangeSlicesQuery(keyspaceOperator, KeySpaceFactory.stringSerializer, KeySpaceFactory.stringSerializer, KeySpaceFactory.stringSerializer);
		
		rangeQuery.setColumnFamily(type);
		rangeQuery.setRowCount(200);
		if(startKey==null || startKey.equals("")){
			startKey = "";
		}
		rangeQuery.setKeys(startKey, "");
		rangeQuery.setColumnNames("网络类型","网络制式","信号强度","小区信息","SINR","网络(1)SINR","网络(1)信号强度","网络(1)小区信息","网络(1)网络制式","网络(2)SINR","网络(2)信号强度","网络(2)小区信息","网络(2)网络制式","终端类型","终端型号","固件版本","软件版本号","data_time","平均速率(Kbps)","imei","LAC","CID","LAC/CID","TAC","PCI","TAC/PCI","cassandra_province","cassandra_city","cassandra_district","cassandra_street","cassandra_street_number","fileName","fileSize","filePath","file_lastModifies","测试用例","网址地址","缓冲总时长(s)","缓冲次数(次)","加载时长(ms)","加载速率(Kbps)","平均速率");
		QueryResult<OrderedRows<String, String, String>> result = rangeQuery
				.execute();
		OrderedRows<String, String, String> oRows = result.get();
		int count = 1;
		String lastKey = "";
		List<Row<String, String, String>> rowList = oRows.getList();
		for (Row<String, String, String> row : rowList) {
			if(row.getKey().equals(startKey)){
				continue;
			}
			HashMap<String, String> myData = new HashMap<String, String>();
			
			List<HColumn<String, String>> columns = row.getColumnSlice()
					.getColumns();
			String speed = "";

			String lac = "";
			String cid = "";
			String tac = "";
			String pci = "";
			String netType = "";
			String testTime = "";
			String deviceType = "";
			String toolsVersion = "";
			String imei = "";
			String cell_info = "";
			
			String province = "";
			String city = "";
			String district = "";
			String street = "";
			String street_no = "";
			
			String fileName = "";
			String filePath = "";
			String fileSize = "";
			String fileLastModifies = "";
			
			String accessAddress = "";
			String videoUrl = "";
			String bufferTime = "";
			String bufferCount = "";
			String loadTime = "";
			String referenceRate = "";
			String modelvalue = "";
			boolean flag1 = false;//判断小区1网络制式是否存在
			boolean flag2 = false;//小区2
			String sinr = "";//SINR
			String netWorkType = "";//网络类型 或者 网络制式
			String signal = "";//信号强度
			
			String sinr1 = "";
			String sinr2 = "";
			String netWorkType1 = "";
			String netWorkType2 = "";
			String cell1 = "";
			String cell2 = "";
			String signal1 = "";
			String signal2 = "";
			for (HColumn<String, String> hColumn : columns) {
				String name = hColumn.getName();
				if(name.contains("终端型号")){
					modelvalue = hColumn.getValue();
				}
				if(name.contains("终端类型")){
					modelvalue = hColumn.getValue();
				}
				if(name.contains("网络(1)网络制式")){
					flag1 = true;
				}
				if(name.contains("网络(2)网络制式")){
					flag2 = true;
				}
			}
			if(menuType.equals("ios") && modelvalue!=null && !modelvalue.toLowerCase().contains("iphone")){
				continue;
			}else if(menuType.equals("android") && modelvalue!=null && modelvalue.toLowerCase().contains("iphone")){
				continue;
			}
			for (HColumn<String, String> hColumn : columns) {
				if("平均速率(Kbps)".equals(hColumn.getName())){
					speed = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("信号强度".equals(hColumn.getName())){
					signal = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("data_time".equals(hColumn.getName())){
					testTime = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("终端类型".equals(hColumn.getName()) || "终端型号".equals(hColumn.getName())){
					deviceType = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("固件版本".equals(hColumn.getName())||"软件版本号".equals(hColumn.getName())){
					toolsVersion = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("imei".equals(hColumn.getName())){
					imei = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("fileName".equals(hColumn.getName())){
					fileName = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("filePath".equals(hColumn.getName())){
					filePath = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("fileSize".equals(hColumn.getName())){
					fileSize = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("file_lastModifies".equals(hColumn.getName())){
					fileLastModifies = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("cassandra_province".equals(hColumn.getName())){
					province = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("cassandra_city".equals(hColumn.getName())){
					city = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("cassandra_district".equals(hColumn.getName())){
					district = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("cassandra_street".equals(hColumn.getName())){
					street = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("cassandra_street_no".equals(hColumn.getName())){
					street_no = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("LAC".equals(hColumn.getName())){
					String lacStr = hColumn.getValue()==null?"":hColumn.getValue();
					if(lacStr.indexOf("=")!=-1){
						lac = lacStr.substring(lacStr.indexOf("=")+1,lacStr.length());
					}else{
						lac = lacStr;
					}
				}else if("CID".equals(hColumn.getName())){
					String cidStr = hColumn.getValue()==null?"":hColumn.getValue();
					if(cidStr.indexOf("=")!=-1){
						cid = cidStr.substring(cidStr.indexOf("=")+1,cidStr.length());
					}else{
						cid = cidStr;
					}
				}else if("LAC/CID".equals(hColumn.getName())){
					String lacCid = hColumn.getValue()==null?"":hColumn.getValue();
					if(lacCid.indexOf("/")!=-1 || lacCid.indexOf("N/A")!=-1){
						String[] cellInfo = lacCid.split("/");
						lac = cellInfo[0];
						cid = cellInfo[1];
					}
				}else if("TAC".equals(hColumn.getName())){
					String tacStr = hColumn.getValue()==null?"":hColumn.getValue();
					if(tacStr.indexOf("=")!=-1){
						tac = tacStr.substring(tacStr.indexOf("=")+1,tacStr.length()-1);
					}else{
						tac = tacStr;
					}
				}else if("PCI".equals(hColumn.getName())){
					String pciStr = hColumn.getValue()==null?"":hColumn.getValue();
					if(pciStr.indexOf("=")!=-1){
						pci = pciStr.substring(pciStr.indexOf("=")+1,pciStr.length()-1);
					}else{
						pci = pciStr;
					}
				}else if("TAC/PCI".equals(hColumn.getName())){
					String tacPci = hColumn.getValue()==null?"":hColumn.getValue();
					if(tacPci.equals("N/A/N/A")){
						tac = "N/A";
						pci = "N/A";
					}else{
						if(tacPci.indexOf("/")!=-1 || tacPci.indexOf("N/A")!=-1){
							String[] cellInfo = tacPci.split("/");
							tac = cellInfo[0];
							pci = cellInfo[1];
						}
					}
				}else if("测试用例".equals(hColumn.getName())){
					accessAddress = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("网址地址".equals(hColumn.getName())){
					videoUrl = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("缓冲总时长(s)".equals(hColumn.getName())){//"缓冲总时长(s)","缓冲次数(次)","加载时长(ms)","加载速率(Kbps)"
					bufferTime = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("缓冲次数(次)".equals(hColumn.getName())){
					bufferCount = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("加载时长(ms)".equals(hColumn.getName())){
					loadTime = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("加载速率(Kbps)".equals(hColumn.getName())||"平均速率".equals(hColumn.getName())){
					referenceRate = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("网络类型".equals(hColumn.getName()) || "网络制式".equals(hColumn.getName()) || "Network Type".equals(hColumn.getName()) || "network type".equals(hColumn.getName())){
					netWorkType = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("SINR".equals(hColumn.getName())){
					sinr = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("网络(1)网络制式".equals(hColumn.getName()) || "Network(1) Type".equals(hColumn.getName()) || "Network (1) standard".equals(hColumn.getName())){
					netWorkType1 = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("网络(1)信号强度".equals(hColumn.getName()) || "Network(1) Signal Strength".equals(hColumn.getName())){
					signal1 = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("网络(1)小区信息".equals(hColumn.getName()) || "Network(1) Cell".equals(hColumn.getName()) || "Network(1)".equals(hColumn.getName())){
					cell1 = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("网络(1)SINR".equals(hColumn.getName()) || "Network(1) SINR".equals(hColumn.getName())){
					sinr1 = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("网络(2)网络制式".equals(hColumn.getName()) || "Network(2) Type".equals(hColumn.getName()) || "Network (2) standard".equals(hColumn.getName())){
					netWorkType2 = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("网络(2)信号强度".equals(hColumn.getName()) || "Network(2) Signal Strength".equals(hColumn.getName())){
					signal2 = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("网络(2)小区信息".equals(hColumn.getName()) || "Network(2) Cell".equals(hColumn.getName())){
					cell2 = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("网络(2)SINR".equals(hColumn.getName()) || "Network(2) SINR".equals(hColumn.getName())){
					sinr2 = hColumn.getValue()==null?"":hColumn.getValue();
				}else{
					myData.put(hColumn.getName(), hColumn.getValue());
				}
			}

//			if(referenceRate!=null && !referenceRate.equals("") && accessAddress!=null && !accessAddress.equals("") && videoUrl!=null && !videoUrl.equals("") && bufferTime!=null && !bufferTime.equals("") && bufferCount!=null && !bufferCount.equals("") && loadTime!=null && !loadTime.equals("")){
				myData.put("access_address", accessAddress);
				myData.put("video_url", videoUrl);
				myData.put("buffering_time", bufferTime);
				myData.put("buffering_count", bufferCount);
				myData.put("80_loading_time", "");
				myData.put("100_loading_time", loadTime);
				myData.put("reference_rate", referenceRate);
				lastKey = row.getKey();
//			}else{
//				continue;
//			}
			
				String lac1 = "";
				String cid1 = "";
				if(cell1.indexOf("/")!=-1 || cell1.indexOf("N/A")!=-1){
					String[] cellInfo = cell1.split("/");
					if(cellInfo[0]!=null && !cellInfo[0].equals("")){
						lac1 = cellInfo[0];
					}
					if(tac!=null && !tac.equals("")){
						lac1 = tac;
					}
					if(cellInfo[1]!=null && !cellInfo[1].equals("")){
						cid1 = cellInfo[1];
					}
					if(pci!=null && !pci.equals("")){
						cid1 = pci;
					}
					if(tac!=null && !tac.equals("") && cellInfo[0]!=null && !cellInfo[0].equals("")){
						lac1 = cellInfo[0]+"|"+tac;
					}
					if(pci!=null && !pci.equals("") && cellInfo[1]!=null && !cellInfo[1].equals("")){
						cid1 = cellInfo[1]+"|"+pci;
					}
				}
				String lac2 = "";
				String cid2 = "";
				if(cell2.indexOf("/")!=-1 || cell2.indexOf("N/A")!=-1){
					String[] cellInfo = cell2.split("/");
					if(cellInfo[0]!=null && !cellInfo[0].equals("")){
						lac2 = cellInfo[0];
					}
					if(tac!=null && !tac.equals("")){
						lac2 = tac;
					}
					if(cellInfo[1]!=null && !cellInfo[1].equals("")){
						cid2 = cellInfo[1];
					}
					if(pci!=null && !pci.equals("")){
						cid2 = pci;
					}
					if(tac!=null && !tac.equals("") && cellInfo[0]!=null && !cellInfo[0].equals("")){
						lac2 = cellInfo[0]+"|"+tac;
					}
					if(pci!=null && !pci.equals("") && cellInfo[1]!=null && !cellInfo[1].equals("")){
						cid2 = cellInfo[1]+"|"+pci;
					}
				}
				//网络(1)信息存在时 当前数据返回的是网络(1)的信息
				if(flag1 && netWorkType1.equals(netWorkType)){
					myData.put("sinr", sinr1);
					myData.put("networktype", netWorkType1);
					myData.put("signal", signal1);
					myData.put("lac", lac1);
					myData.put("cid", cid1);
				}
				//网络(2)信息存在时 当前数据返回的是网络(2)的信息
				if(flag2 && netWorkType2.equals(netWorkType)){
					myData.put("sinr", sinr2);
					myData.put("networktype", netWorkType2);
					myData.put("signal", signal2);
					myData.put("lac", lac2);
					myData.put("cid", cid2);
				}
				if(!(flag1 && netWorkType.equals(netWorkType1)) && !(flag2 && netWorkType.equals(netWorkType2))){
					myData.put("sinr", sinr);
					myData.put("networktype", netWorkType);
					myData.put("signal", signal);
					if(lac!=null && !lac.equals("")){
						myData.put("lac", lac);
					}
					if(tac!=null && !tac.equals("")){
						lac2 = tac;
						myData.put("lac", tac);
					}
					if(cid!=null && !cid.equals("")){
						myData.put("cid", cid);
					}
					if(pci!=null && !pci.equals("")){
						cid2 = pci;
						myData.put("cid", pci);
					}
					if(tac!=null && !tac.equals("") && lac!=null && !lac.equals("")){
						lac2 = lac+"|"+tac;
						myData.put("lac", lac+"|"+tac);	
					}
					if(pci!=null && !pci.equals("") && cid!=null && !cid.equals("")){
						cid2 = cid+"|"+pci;
						myData.put("cid", cid+"|"+pci);	
					}
				}
				myData.put("sinr1", sinr1);
				myData.put("networktype1", netWorkType1);
				myData.put("signal1", signal1);
				myData.put("lac1", lac1);
				myData.put("cid1", cid1);
				
				myData.put("sinr2", sinr2);
				myData.put("networktype2", netWorkType2);
				myData.put("signal2", signal2);
				myData.put("lac2", lac2);
				myData.put("cid2", cid2);
			myData.put("GSMLAC", lac);
			myData.put("GSMCID", cid);
			myData.put("LTETAC", tac);
			myData.put("LTEPCI", pci);
			if(netType.indexOf("LTE")!=-1){
				myData.put("LTESignal", signal);
			}else{
				myData.put("GSMSignal", signal);
			}
			if(!lac.isEmpty() && !cid.isEmpty()){
				cell_info = lac+","+cid;
			}
			
			myData.put("cell_info", cell_info);
			myData.put("test_location", province+city+district+street+street_no);
			myData.put("testing_time", testTime);
			myData.put("terminal_model", deviceType);
			myData.put("software_edition",toolsVersion);
			myData.put("imei", imei);
			myData.put("testSpeed",speed);
			myData.put("file_size",fileSize);
			myData.put("file_name", fileName);
			myData.put("file_path", filePath);
			myData.put("file_lastModifies", fileLastModifies);
			detail.add(myData);
			count++;
			if(count>max){
				break;
			}
		}
		respResult.put("total",Integer.parseInt(limit));
		respResult.put("detail",detail);
		respResult.put("last_key",lastKey);
		return respResult;
	}
	/**
	 * 切换 reside 04006（networklingering）
	 * @author Ocean
	 * @param province
	 * @param city
	 * @param district
	 * @param orgs
	 * @param limit 
	 * @param testTypes
	 */
	public Map<String, Object> getTestDataByResideTest(String type, String menuType, String startKey, String orgs, Date startDate, Date endDate, String limit) throws Exception{
		Integer max = Integer.parseInt(limit);
		Map<String,Object> respResult = new HashMap<String,Object> ();
		ArrayList<HashMap<String, String>> detail = new ArrayList<HashMap<String, String>>();
		Keyspace keyspaceOperator = null;
		try{
			keyspaceOperator = HFactory.createKeyspace(orgs, KeySpaceFactory.cluster);
		}catch (Exception e){
			e.printStackTrace();
		}
		RangeSlicesQuery<String, String, String> rangeQuery = HFactory
				.createRangeSlicesQuery(keyspaceOperator, KeySpaceFactory.stringSerializer, KeySpaceFactory.stringSerializer, KeySpaceFactory.stringSerializer);
		
		rangeQuery.setColumnFamily(type);
		rangeQuery.setRowCount(200);
		if(startKey==null || startKey.equals("")){
			startKey = "";
		}
		rangeQuery.setKeys(startKey, "");
		//"time","terminal model","OS version","GPS position","GPS rate","network type","Network Name","server information","the terminal network address","terminal public address","terminal MAC address","signal strength","WIFI signal strength"
		rangeQuery.setColumnNames("网络类型","网络制式","Network Type","network type","小区信息","Network cell","SINR","网络(1)SINR","Network(1) SINR","网络(1)信号强度","Network(1) Signal Strength","网络(1)小区信息","Network(1) Cell","网络(1)网络制式","Network(1) Type","Network (1) standard","网络(2)SINR","Network(2) SINR","网络(2)信号强度","Network(2) Signal Strength","网络(2)小区信息","Network(2) Cell","网络(2)网络制式","Network(2) Type","Network (2) standard","信号强度","Signal Strength","终端类型","终端型号","Terminal Model","terminal model","Terminal Type","Device Type","Terminal Mode","Device Model","固件版本","软件版本号","OS version","data_time","平均速率(Kbps)","Avg Speed(Kbps)","imei","LAC","CID","LAC/CID","TAC","PCI","TAC/PCI","cassandra_province","cassandra_city","cassandra_district","cassandra_street","cassandra_street_number","fileName","fileSize","filePath","file_lastModifies","Handover Latency","平均时长(ms)","平均时延(ms)","平均时延","Avg Time(ms)","Latency","Delay","Avg Latency(ms)");
		QueryResult<OrderedRows<String, String, String>> result = rangeQuery
				.execute();
		OrderedRows<String, String, String> oRows = result.get();
		int count = 1;
		String lastKey = "";
		List<Row<String, String, String>> rowList = oRows.getList();
		for (Row<String, String, String> row : rowList) {
			if(row.getKey().equals(startKey)){
				continue;
			}
			System.out.println(row.getKey());
			HashMap<String, String> myData = new HashMap<String, String>();
			
			List<HColumn<String, String>> columns = row.getColumnSlice()
					.getColumns();
			String speed = "";

			String lac = "";
			String cid = "";
			String tac = "";
			String pci = "";
			String netType = "";
			String testTime = "";
			String deviceType = "";
			String toolsVersion = "";
			String imei = "";
			String cell_info = "";
			
			String province = "";
			String city = "";
			String district = "";
			String street = "";
			String street_no = "";
			
			String fileName = "";
			String filePath = "";
			String fileSize = "";
			String fileLastModifies = "";
			
			String modelvalue = "";
			boolean flag1 = false;//判断小区1网络制式是否存在
			boolean flag2 = false;//小区2
			String sinr = "";//SINR
			String netWorkType = "";//网络类型 或者 网络制式
			String signal = "";//信号强度
			
			String sinr1 = "";
			String sinr2 = "";
			String netWorkType1 = "";
			String netWorkType2 = "";
			String cell1 = "";
			String cell2 = "";
			String signal1 = "";
			String signal2 = "";
			
			String average_time_delay = "";//平均时延
			for (HColumn<String, String> hColumn : columns) {
				String name = hColumn.getName();
				if(name.contains("终端类型") || name.contains("终端型号") || "Device Type".equals(hColumn.getName()) || "Terminal Model".equals(hColumn.getName()) || "Terminal Type".equals(hColumn.getName()) || "Terminal Mode".equals(hColumn.getName()) || "Device Model".equals(hColumn.getName()) || "terminal model".equals(hColumn.getName())){
					modelvalue = hColumn.getValue();
				}
				if(name.contains("网络(1)网络制式") || name.contains("Network(1) Type") || name.contains("Network (1) standard")){
					flag1 = true;
				}
				if(name.contains("网络(2)网络制式") || name.contains("Network(2) Type") || name.contains("Network (2) standard")){
					flag2 = true;
				}
			}
			if(menuType.equals("ios") && modelvalue!=null && !modelvalue.toLowerCase().contains("iphone")){
				continue;
			}else if(menuType.equals("android") && modelvalue!=null && modelvalue.toLowerCase().contains("iphone")){
				continue;
			}
			for (HColumn<String, String> hColumn : columns) {
				if("平均速率(Kbps)".equals(hColumn.getName()) || "Avg Speed(Kbps)".equals(hColumn.getName())){
					speed = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("信号强度".equals(hColumn.getName()) || "Signal Strength".equals(hColumn.getName())){
					signal = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("data_time".equals(hColumn.getName())){
					testTime = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("终端类型".equals(hColumn.getName()) || "终端型号".equals(hColumn.getName()) || "Device Type".equals(hColumn.getName()) || "Terminal Model".equals(hColumn.getName()) || "Terminal Type".equals(hColumn.getName()) || "Terminal Mode".equals(hColumn.getName()) || "Device Model".equals(hColumn.getName()) || "terminal model".equals(hColumn.getName())){
					deviceType = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("固件版本".equals(hColumn.getName())||"软件版本号".equals(hColumn.getName()) || "OS version".equals(hColumn.getName())){
					toolsVersion = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("imei".equals(hColumn.getName())){
					imei = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("fileName".equals(hColumn.getName())){
					fileName = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("filePath".equals(hColumn.getName())){
					filePath = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("fileSize".equals(hColumn.getName())){
					fileSize = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("file_lastModifies".equals(hColumn.getName())){
					fileLastModifies = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("cassandra_province".equals(hColumn.getName())){
					province = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("cassandra_city".equals(hColumn.getName())){
					city = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("cassandra_district".equals(hColumn.getName())){
					district = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("cassandra_street".equals(hColumn.getName())){
					street = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("cassandra_street_no".equals(hColumn.getName())){
					street_no = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("LAC".equals(hColumn.getName())){
					String lacStr = hColumn.getValue()==null?"":hColumn.getValue();
					if(lacStr.indexOf("=")!=-1){
						lac = lacStr.substring(lacStr.indexOf("=")+1,lacStr.length());
					}else{
						lac = lacStr;
					}
				}else if("CID".equals(hColumn.getName())){
					String cidStr = hColumn.getValue()==null?"":hColumn.getValue();
					if(cidStr.indexOf("=")!=-1){
						cid = cidStr.substring(cidStr.indexOf("=")+1,cidStr.length());
					}else{
						cid = cidStr;
					}
				}else if("LAC/CID".equals(hColumn.getName())){
					String lacCid = hColumn.getValue()==null?"":hColumn.getValue();
					if(lacCid.indexOf("/")!=-1 || lacCid.indexOf("N/A")!=-1){
						String[] cellInfo = lacCid.split("/");
						lac = cellInfo[0];
						cid = cellInfo[1];
					}
				}else if("TAC".equals(hColumn.getName())){
					String tacStr = hColumn.getValue()==null?"":hColumn.getValue();
					if(tacStr.indexOf("=")!=-1){
						tac = tacStr.substring(tacStr.indexOf("=")+1,tacStr.length()-1);
					}else{
						tac = tacStr;
					}
				}else if("PCI".equals(hColumn.getName())){
					String pciStr = hColumn.getValue()==null?"":hColumn.getValue();
					if(pciStr.indexOf("=")!=-1){
						pci = pciStr.substring(pciStr.indexOf("=")+1,pciStr.length()-1);
					}else{
						pci = pciStr;
					}
				}else if("TAC/PCI".equals(hColumn.getName())){
					String tacPci = hColumn.getValue()==null?"":hColumn.getValue();
					if(tacPci.equals("N/A/N/A")){
						tac = "N/A";
						pci = "N/A";
					}else{
						if(tacPci.indexOf("/")!=-1 || tacPci.indexOf("N/A")!=-1){
							String[] cellInfo = tacPci.split("/");
							tac = cellInfo[0];
							pci = cellInfo[1];
						}
					}
				}else if("网络类型".equals(hColumn.getName()) || "网络制式".equals(hColumn.getName()) || "Network Type".equals(hColumn.getName()) || "network type".equals(hColumn.getName())){
					netWorkType = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("SINR".equals(hColumn.getName())){
					sinr = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("网络(1)网络制式".equals(hColumn.getName()) || "Network(1) Type".equals(hColumn.getName()) || "Network (1) standard".equals(hColumn.getName())){
					netWorkType1 = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("网络(1)信号强度".equals(hColumn.getName()) || "Network(1) Signal Strength".equals(hColumn.getName())){
					signal1 = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("网络(1)小区信息".equals(hColumn.getName()) || "Network(1) Cell".equals(hColumn.getName()) || "Network(1)".equals(hColumn.getName())){
					cell1 = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("网络(1)SINR".equals(hColumn.getName()) || "Network(1) SINR".equals(hColumn.getName())){
					sinr1 = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("网络(2)网络制式".equals(hColumn.getName()) || "Network(2) Type".equals(hColumn.getName()) || "Network (2) standard".equals(hColumn.getName())){
					netWorkType2 = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("网络(2)信号强度".equals(hColumn.getName()) || "Network(2) Signal Strength".equals(hColumn.getName())){
					signal2 = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("网络(2)小区信息".equals(hColumn.getName()) || "Network(2) Cell".equals(hColumn.getName())){
					cell2 = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("网络(2)SINR".equals(hColumn.getName()) || "Network(2) SINR".equals(hColumn.getName())){
					sinr2 = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("平均时长(ms)".equals(hColumn.getName()) || "平均时延".equals(hColumn.getName()) || "平均时延(ms)".equals(hColumn.getName()) || "Avg Latency(ms)".equals(hColumn.getName()) || "Avg Time(ms)".equals(hColumn.getName()) || "Latency".equals(hColumn.getName()) || "Delay".equals(hColumn.getName()) || "Handover Latency".equals(hColumn.getName())){
					average_time_delay = hColumn.getValue()==null?"":hColumn.getValue();
				}
				else{
					myData.put(hColumn.getName(), hColumn.getValue());
				}
			}
//			if(topSpeed!=null && downSpeed!=null && delay!=null && serverIp!=null && protocol!=null && topThread!=null && downThread!=null){
			myData.put("average_time_delay", average_time_delay);
			lastKey = row.getKey();
//			}else{
//				continue;
//			}
				String lac1 = "";
				String cid1 = "";
				if(cell1.indexOf("/")!=-1 || cell1.indexOf("N/A")!=-1){
					String[] cellInfo = cell1.split("/");
					if(cellInfo[0]!=null && !cellInfo[0].equals("")){
						lac1 = cellInfo[0];
					}
					if(tac!=null && !tac.equals("")){
						lac1 = tac;
					}
					if(cellInfo[1]!=null && !cellInfo[1].equals("")){
						cid1 = cellInfo[1];
					}
					if(pci!=null && !pci.equals("")){
						cid1 = pci;
					}
					if(tac!=null && !tac.equals("") && cellInfo[0]!=null && !cellInfo[0].equals("")){
						lac1 = cellInfo[0]+"|"+tac;
					}
					if(pci!=null && !pci.equals("") && cellInfo[1]!=null && !cellInfo[1].equals("")){
						cid1 = cellInfo[1]+"|"+pci;
					}
				}
				String lac2 = "";
				String cid2 = "";
				if(cell2.indexOf("/")!=-1 || cell2.indexOf("N/A")!=-1){
					String[] cellInfo = cell2.split("/");
					if(cellInfo[0]!=null && !cellInfo[0].equals("")){
						lac2 = cellInfo[0];
					}
					if(tac!=null && !tac.equals("")){
						lac2 = tac;
					}
					if(cellInfo[1]!=null && !cellInfo[1].equals("")){
						cid2 = cellInfo[1];
					}
					if(pci!=null && !pci.equals("")){
						cid2 = pci;
					}
					if(tac!=null && !tac.equals("") && cellInfo[0]!=null && !cellInfo[0].equals("")){
						lac2 = cellInfo[0]+"|"+tac;
					}
					if(pci!=null && !pci.equals("") && cellInfo[1]!=null && !cellInfo[1].equals("")){
						cid2 = cellInfo[1]+"|"+pci;
					}
				}
				//网络(1)信息存在时 当前数据返回的是网络(1)的信息
				if(flag1 && netWorkType1.equals(netWorkType)){
					myData.put("sinr", sinr1);
					myData.put("networktype", netWorkType1);
					myData.put("signal", signal1);
					myData.put("lac", lac1);
					myData.put("cid", cid1);
				}
				//网络(2)信息存在时 当前数据返回的是网络(2)的信息
				if(flag2 && netWorkType2.equals(netWorkType)){
					myData.put("sinr", sinr2);
					myData.put("networktype", netWorkType2);
					myData.put("signal", signal2);
					myData.put("lac", lac2);
					myData.put("cid", cid2);
				}
				if(!(flag1 && netWorkType.equals(netWorkType1)) && !(flag2 && netWorkType.equals(netWorkType2))){
					myData.put("sinr", sinr);
					myData.put("networktype", netWorkType);
					myData.put("signal", signal);
					if(lac!=null && !lac.equals("")){
						myData.put("lac", lac);
					}
					if(tac!=null && !tac.equals("")){
						lac2 = tac;
						myData.put("lac", tac);
					}
					if(cid!=null && !cid.equals("")){
						myData.put("cid", cid);
					}
					if(pci!=null && !pci.equals("")){
						cid2 = pci;
						myData.put("cid", pci);
					}
					if(tac!=null && !tac.equals("") && lac!=null && !lac.equals("")){
						lac2 = lac+"|"+tac;
						myData.put("lac", lac+"|"+tac);	
					}
					if(pci!=null && !pci.equals("") && cid!=null && !cid.equals("")){
						cid2 = cid+"|"+pci;
						myData.put("cid", cid+"|"+pci);	
					}
				}
				myData.put("sinr1", sinr1);
				myData.put("networktype1", netWorkType1);
				myData.put("signal1", signal1);
				myData.put("lac1", lac1);
				myData.put("cid1", cid1);
				
				myData.put("sinr2", sinr2);
				myData.put("networktype2", netWorkType2);
				myData.put("signal2", signal2);
				myData.put("lac2", lac2);
				myData.put("cid2", cid2);
			myData.put("GSMLAC", lac);
			myData.put("GSMCID", cid);
			myData.put("LTETAC", tac);
			myData.put("LTEPCI", pci);
			if(netType.indexOf("LTE")!=-1){
				myData.put("LTESignal", signal);
			}else{
				myData.put("GSMSignal", signal);
			}
			if(!lac.isEmpty() && !cid.isEmpty()){
				cell_info = lac+","+cid;
			}
			
			myData.put("cell_info", cell_info);
			myData.put("test_location", province+city+district+street+street_no);
			myData.put("testing_time", testTime);
			myData.put("terminal_model", deviceType);
			myData.put("software_edition",toolsVersion);
			myData.put("imei", imei);
			myData.put("testSpeed",speed);
			myData.put("file_size",fileSize);
			myData.put("file_name", fileName);
			myData.put("file_path", filePath);
			myData.put("file_lastModifies", fileLastModifies);
			detail.add(myData);
			count++;
			if(count>max){
				break;
			}
		}
		respResult.put("total",max);
		respResult.put("detail",detail);
		respResult.put("last_key",lastKey);
		return respResult;
	}
	/**
	 * 话音 dial 05001
	 * @author Ocean
	 * @param province
	 * @param city
	 * @param district
	 * @param orgs
	 * @param limit 
	 * @param testTypes
	 */
	public Map<String, Object> getTestDataByDial(String type, String menuType, String startKey, String orgs, Date startDate, Date endDate, String limit) throws Exception{
		Integer max = Integer.parseInt(limit);
		boolean flag = false;
		Map<String,Object> respResult = new HashMap<String,Object> ();
		ArrayList<HashMap<String, String>> detail = new ArrayList<HashMap<String, String>>();
		Keyspace keyspaceOperator = null;
		try{
			keyspaceOperator = HFactory.createKeyspace(orgs, KeySpaceFactory.cluster);
		}catch (Exception e){
			e.printStackTrace();
		}
		RangeSlicesQuery<String, String, String> rangeQuery = HFactory
				.createRangeSlicesQuery(keyspaceOperator, KeySpaceFactory.stringSerializer, KeySpaceFactory.stringSerializer, KeySpaceFactory.stringSerializer);
		
		if(type.equals("05001.C")){
			flag = true;
		}
		rangeQuery.setColumnFamily("05001");
		rangeQuery.setRowCount(200);
		if(startKey==null || startKey.equals("")){
			startKey = "";
		}
		rangeQuery.setKeys(startKey, "");
		//IOS 增加 接通时长(s)
		rangeQuery.setColumnNames("网络类型","网络制式","Network Type","network type","信号强度","Signal Strength","小区信息","Network Cell","SINR","网络(1)SINR","Network(1) SINR","网络(1)信号强度","Network(1) Signal Strength","网络(1)小区信息","Network(1) Cell","网络(1)网络制式","Network(1) Type","Network (1) standard","网络(2)SINR","Network(2) SINR","网络(2)信号强度","Network(2) Signal Strength","网络(2)小区信息","Network(2) Cell","网络(2)网络制式","Network(2) Type","Network (2) standard","终端类型","终端型号","Terminal Model","terminal model","terminal model","Terminal Type","Device Type","Terminal Mode","Device Model","固件版本","软件版本号","OS version","data_time","平均速率(Kbps)","Average Speed (Kbps)","imei","LAC","CID","LAC/CID","TAC","PCI","TAC/PCI","cassandra_province","cassandra_city","cassandra_district","cassandra_street","cassandra_street_number","fileName","fileSize","filePath","file_lastModifies","是否成功","Success or NOT","接续时长(ms)","Continuing Time(ms)","接通时长(ms)","Connecting time(ms)","接通时长(s)","Connected time(s)","是否录音","Call Recording","手机号码","Phone Number","网络","Network","fallbackToCs(ms)","FallBack To CS(s)","backToLte(ms)","Back To Lte(ms)");
		QueryResult<OrderedRows<String, String, String>> result = rangeQuery
				.execute();
		OrderedRows<String, String, String> oRows = result.get();
		int count = 1;
		String lastKey = "";
		List<Row<String, String, String>> rowList = oRows.getList();
		for (Row<String, String, String> row : rowList) {
			if(row.getKey().equals(startKey)){
				continue;
			}
			if(flag){
				if(!row.getKey().contains("05001.C")){
					continue;
				}
			}
//			if(!row.getKey().contains("1417074070360823_05001.1664_11977-2014_11_27_15_47_25_771.summary.csv")){
//				continue;
//			}
			HashMap<String, String> myData = new HashMap<String, String>();
			
			List<HColumn<String, String>> columns = row.getColumnSlice()
					.getColumns();
			String speed = "";

			String lac = "";
			String cid = "";
			String tac = "";
			String pci = "";
			String netType = "";
			String testTime = "";
			String deviceType = "";
			String toolsVersion = "";
			String imei = "";
			String cell_info = "";
			
			String province = "";
			String city = "";
			String district = "";
			String street = "";
			String street_no = "";
			
			String fileName = "";
			String filePath = "";
			String fileSize = "";
			String fileLastModifies = "";
			
			String success_failure = "";
			String access_time = "";
			String access_time1 = "";
			String access_time2 = "";
			String called_number = "";//被叫目标号码
			String caller_terminal_mode = "";//主叫型号
			String network_type = "";
			String recording = "";//是否录音
			String hand_over_situation = "";

			String fallbackToCs = "";
			String backToLte = "";
			
			String modelvalue = "";
			boolean flag1 = false;//判断小区1网络制式是否存在
			boolean flag2 = false;//小区2
			String sinr = "";//SINR
			String netWorkType = "";//网络类型 或者 网络制式
			String signal = "";//信号强度
			
			String sinr1 = "";
			String sinr2 = "";
			String netWorkType1 = "";
			String netWorkType2 = "";
			String cell1 = "";
			String cell2 = "";
			String signal1 = "";
			String signal2 = "";
			for (HColumn<String, String> hColumn : columns) {
				String name = hColumn.getName();
				if(name.contains("终端型号")  || "Device Type".equals(hColumn.getName()) || "Terminal Model".equals(hColumn.getName()) || "Terminal Type".equals(hColumn.getName()) || "Terminal Mode".equals(hColumn.getName()) || "Device Model".equals(hColumn.getName()) || "terminal model".equals(hColumn.getName())){
					modelvalue = hColumn.getValue();
				}
				if(name.contains("网络(1)网络制式") || name.contains("Network(1) Type") || name.contains("Network (1) standard")){
					flag1 = true;
				}
				if(name.contains("网络(2)网络制式") || name.contains("Network(2) Type") || name.contains("Network (2) standard")){
					flag2 = true;
				}
			}
			if(menuType.equals("ios") && modelvalue!=null && !modelvalue.toLowerCase().contains("iphone")){
				continue;
			}else if(menuType.equals("android") && modelvalue!=null && modelvalue.toLowerCase().contains("iphone")){
				continue;
			}
			for (HColumn<String, String> hColumn : columns) {
				if("平均速率(Kbps)".equals(hColumn.getName()) || "Average Speed (Kbps)".equals(hColumn.getName())){
					speed = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("信号强度".equals(hColumn.getName()) || "Signal Strength".equals(hColumn.getName())){
					signal = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("data_time".equals(hColumn.getName())){
					testTime = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("终端类型".equals(hColumn.getName()) || "终端型号".equals(hColumn.getName()) || "Device Type".equals(hColumn.getName()) || "Terminal Model".equals(hColumn.getName()) || "Terminal Type".equals(hColumn.getName()) || "Terminal Mode".equals(hColumn.getName()) || "Device Model".equals(hColumn.getName()) || "terminal model".equals(hColumn.getName())){
					deviceType = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("固件版本".equals(hColumn.getName())||"软件版本号".equals(hColumn.getName()) || "OS version".equals(hColumn.getName())){
					toolsVersion = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("imei".equals(hColumn.getName())){
					imei = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("fileName".equals(hColumn.getName())){
					fileName = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("filePath".equals(hColumn.getName())){
					filePath = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("fileSize".equals(hColumn.getName())){
					fileSize = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("file_lastModifies".equals(hColumn.getName())){
					fileLastModifies = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("cassandra_province".equals(hColumn.getName())){
					province = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("cassandra_city".equals(hColumn.getName())){
					city = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("cassandra_district".equals(hColumn.getName())){
					district = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("cassandra_street".equals(hColumn.getName())){
					street = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("cassandra_street_no".equals(hColumn.getName())){
					street_no = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("LAC".equals(hColumn.getName())){
					String lacStr = hColumn.getValue()==null?"":hColumn.getValue();
					if(lacStr.indexOf("=")!=-1){
						lac = lacStr.substring(lacStr.indexOf("=")+1,lacStr.length());
					}else{
						lac = lacStr;
					}
				}else if("CID".equals(hColumn.getName())){
					String cidStr = hColumn.getValue()==null?"":hColumn.getValue();
					if(cidStr.indexOf("=")!=-1){
						cid = cidStr.substring(cidStr.indexOf("=")+1,cidStr.length());
					}else{
						cid = cidStr;
					}
				}else if("LAC/CID".equals(hColumn.getName()) || "小区信息".equals(hColumn.getName())){
					String lacCid = hColumn.getValue()==null?"":hColumn.getValue();
					if(lacCid.indexOf("/")!=-1 || lacCid.indexOf("N/A")!=-1){
						String[] cellInfo = lacCid.split("/");
						lac = cellInfo[0];
						cid = cellInfo[1];
					}
				}else if("TAC".equals(hColumn.getName())){
					String tacStr = hColumn.getValue()==null?"":hColumn.getValue();
					if(tacStr.indexOf("=")!=-1){
						tac = tacStr.substring(tacStr.indexOf("=")+1,tacStr.length()-1);
					}else{
						tac = tacStr;
					}
				}else if("PCI".equals(hColumn.getName())){
					String pciStr = hColumn.getValue()==null?"":hColumn.getValue();
					if(pciStr.indexOf("=")!=-1){
						pci = pciStr.substring(pciStr.indexOf("=")+1,pciStr.length()-1);
					}else{
						pci = pciStr;
					}
				}else if("TAC/PCI".equals(hColumn.getName())){
					String tacPci = hColumn.getValue()==null?"":hColumn.getValue();
					if(tacPci.equals("N/A/N/A")){
						tac = "N/A";
						pci = "N/A";
					}else{
						if(tacPci.indexOf("/")!=-1 || tacPci.indexOf("N/A")!=-1){
							String[] cellInfo = tacPci.split("/");
							tac = cellInfo[0];
							pci = cellInfo[1];
						}
					}
				}else if("是否成功".equals(hColumn.getName()) || "Success or NOT".equals(hColumn.getName())){//"是否成功","通话时长","手机号码","请求类型","网络","fallbackToCs(ms)"
					success_failure = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("接续时长(ms)".equals(hColumn.getName()) || "Continuing Time(ms)".equals(hColumn.getName())){
					access_time = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("接通时长(ms)".equals(hColumn.getName()) || "Connecting time(ms)".equals(hColumn.getName())){
					access_time1 = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("接通时长(s)".equals(hColumn.getName()) || "Connected time(s)".equals(hColumn.getName())){
					access_time2 = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("手机号码".equals(hColumn.getName()) || "Phone Number".equals(hColumn.getName())){
					called_number = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("终端型号".equals(hColumn.getName()) || "terminal model".equals(hColumn.getName())){
					caller_terminal_mode = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("网络".equals(hColumn.getName()) || "Network".equals(hColumn.getName())){
					network_type = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("fallbackToCs(ms)".equals(hColumn.getName()) || "FallBack To CS(s)".equals(hColumn.getName())){
					hand_over_situation = hColumn.getValue()==null?"":hColumn.getValue();
					fallbackToCs = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("backToLte(ms)".equals(hColumn.getName()) || "Back To Lte(ms)".equals(hColumn.getName())){
					backToLte = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("是否录音".equals(hColumn.getName()) || "Call Recording".equals(hColumn.getName())){
					recording = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("网络类型".equals(hColumn.getName()) || "网络制式".equals(hColumn.getName()) || "Network Type".equals(hColumn.getName()) || "network type".equals(hColumn.getName())){
					netWorkType = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("SINR".equals(hColumn.getName())){
					sinr = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("网络(1)网络制式".equals(hColumn.getName()) || "Network(1) Type".equals(hColumn.getName()) || "Network (1) standard".equals(hColumn.getName())){
					netWorkType1 = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("网络(1)信号强度".equals(hColumn.getName()) || "Network(1) Signal Strength".equals(hColumn.getName())){
					signal1 = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("网络(1)小区信息".equals(hColumn.getName()) || "Network(1) Cell".equals(hColumn.getName()) || "Network(1)".equals(hColumn.getName())){
					cell1 = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("网络(1)SINR".equals(hColumn.getName()) || "Network(1) SINR".equals(hColumn.getName())){
					sinr1 = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("网络(2)网络制式".equals(hColumn.getName()) || "Network(2) Type".equals(hColumn.getName()) || "Network (2) standard".equals(hColumn.getName())){
					netWorkType2 = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("网络(2)信号强度".equals(hColumn.getName()) || "Network(2) Signal Strength".equals(hColumn.getName())){
					signal2 = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("网络(2)小区信息".equals(hColumn.getName()) || "Network(2) Cell".equals(hColumn.getName())){
					cell2 = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("网络(2)SINR".equals(hColumn.getName()) || "Network(2) SINR".equals(hColumn.getName())){
					sinr2 = hColumn.getValue()==null?"":hColumn.getValue();
				}else{
					myData.put(hColumn.getName(), hColumn.getValue());
				}
			}

//			if(success_failure!=null && !success_failure.equals("") && access_time!=null && access_time1!=null && called_number!=null && !called_number.equals("") && caller_terminal_mode!=null && !caller_terminal_mode.equals("") && network_type!=null && !network_type.equals("") && hand_over_situation!=null && !hand_over_situation.equals("")){
				myData.put("success_failure",success_failure);
				if(access_time!=null && access_time.equals("")||access_time.equals("0")||access_time.equals("null")){
					if(access_time1!=null && access_time1.equals("")||access_time1.equals("0")||access_time1.equals("null")){
						if(access_time2!=null && access_time1.equals("")||access_time1.equals("0")||access_time1.equals("null")){
							myData.put("access_time", access_time1);
						}else{
							myData.put("access_time", access_time2);	
						}
					}else{
						myData.put("access_time", access_time1);	
					}
				}else{
					myData.put("access_time", access_time);					
				}
				myData.put("called_number", called_number);
				myData.put("caller_terminal_model", caller_terminal_mode);
				myData.put("belonging_network_type", network_type);
				myData.put("hand_over_situation", hand_over_situation);
				myData.put("recording", recording);
				if(type.equals("05001.C")){
					myData.put("backToLte", backToLte);
					myData.put("fallbackToCs", fallbackToCs);
				}
				lastKey = row.getKey();
//			}else{
//				continue;
//			}
				String lac1 = "";
				String cid1 = "";
				if(cell1.indexOf("/")!=-1 || cell1.indexOf("N/A")!=-1){
					String[] cellInfo = cell1.split("/");
					if(cellInfo[0]!=null && !cellInfo[0].equals("")){
						lac1 = cellInfo[0];
					}
					if(tac!=null && !tac.equals("")){
						lac1 = tac;
					}
					if(cellInfo[1]!=null && !cellInfo[1].equals("")){
						cid1 = cellInfo[1];
					}
					if(pci!=null && !pci.equals("")){
						cid1 = pci;
					}
					if(tac!=null && !tac.equals("") && cellInfo[0]!=null && !cellInfo[0].equals("")){
						lac1 = cellInfo[0]+"|"+tac;
					}
					if(pci!=null && !pci.equals("") && cellInfo[1]!=null && !cellInfo[1].equals("")){
						cid1 = cellInfo[1]+"|"+pci;
					}
				}
				String lac2 = "";
				String cid2 = "";
				if(cell2.indexOf("/")!=-1 || cell2.indexOf("N/A")!=-1){
					String[] cellInfo = cell2.split("/");
					if(cellInfo[0]!=null && !cellInfo[0].equals("")){
						lac2 = cellInfo[0];
					}
					if(tac!=null && !tac.equals("")){
						lac2 = tac;
					}
					if(cellInfo[1]!=null && !cellInfo[1].equals("")){
						cid2 = cellInfo[1];
					}
					if(pci!=null && !pci.equals("")){
						cid2 = pci;
					}
					if(tac!=null && !tac.equals("") && cellInfo[0]!=null && !cellInfo[0].equals("")){
						lac2 = cellInfo[0]+"|"+tac;
					}
					if(pci!=null && !pci.equals("") && cellInfo[1]!=null && !cellInfo[1].equals("")){
						cid2 = cellInfo[1]+"|"+pci;
					}
				}
				//网络(1)信息存在时 当前数据返回的是网络(1)的信息
				if(flag1 && netWorkType1.equals(netWorkType)){
					myData.put("sinr", sinr1);
					myData.put("networktype", netWorkType1);
					myData.put("signal", signal1);
					myData.put("lac", lac1);
					myData.put("cid", cid1);
				}
				//网络(2)信息存在时 当前数据返回的是网络(2)的信息
				if(flag2 && netWorkType2.equals(netWorkType)){
					myData.put("sinr", sinr2);
					myData.put("networktype", netWorkType2);
					myData.put("signal", signal2);
					myData.put("lac", lac2);
					myData.put("cid", cid2);
				}
				if(!(flag1 && netWorkType.equals(netWorkType1)) && !(flag2 && netWorkType.equals(netWorkType2))){
					myData.put("sinr", sinr);
					myData.put("networktype", netWorkType);
					myData.put("signal", signal);
					if(lac!=null && !lac.equals("")){
						myData.put("lac", lac);
					}
					if(tac!=null && !tac.equals("")){
						lac2 = tac;
						myData.put("lac", tac);
					}
					if(cid!=null && !cid.equals("")){
						myData.put("cid", cid);
					}
					if(pci!=null && !pci.equals("")){
						cid2 = pci;
						myData.put("cid", pci);
					}
					if(tac!=null && !tac.equals("") && lac!=null && !lac.equals("")){
						lac2 = lac+"|"+tac;
						myData.put("lac", lac+"|"+tac);	
					}
					if(pci!=null && !pci.equals("") && cid!=null && !cid.equals("")){
						cid2 = cid+"|"+pci;
						myData.put("cid", cid+"|"+pci);	
					}
				}
				myData.put("sinr1", sinr1);
				myData.put("networktype1", netWorkType1);
				myData.put("signal1", signal1);
				myData.put("lac1", lac1);
				myData.put("cid1", cid1);
				
				myData.put("sinr2", sinr2);
				myData.put("networktype2", netWorkType2);
				myData.put("signal2", signal2);
				myData.put("lac2", lac2);
				myData.put("cid2", cid2);
			myData.put("GSMLAC", lac);
			myData.put("GSMCID", cid);
			myData.put("LTETAC", tac);
			myData.put("LTEPCI", pci);
			if(netType.indexOf("LTE")!=-1){
				myData.put("LTESignal", signal);
			}else{
				myData.put("GSMSignal", signal);
			}
			if(!lac.isEmpty() && !cid.isEmpty()){
				cell_info = lac+","+cid;
			}
			
			myData.put("cell_info", cell_info);
			myData.put("test_location", province+city+district+street+street_no);
			myData.put("testing_time", testTime);
			myData.put("terminal_model", deviceType);
			myData.put("software_edition",toolsVersion);
			myData.put("imei", imei);
			myData.put("testSpeed",speed);
			myData.put("file_size",fileSize);
			myData.put("file_name", fileName);
			myData.put("file_path", filePath);
			myData.put("file_lastModifies", fileLastModifies);
			detail.add(myData);
			count++;
			if(count>max){
				break;
			}
		}
		respResult.put("total",Integer.parseInt(limit));
		respResult.put("detail",detail);
		respResult.put("last_key",lastKey);
		return respResult;
	}
	/**
	 * FTP 03002
	 * @author Ocean
	 * @param province
	 * @param city
	 * @param district
	 * @param orgs
	 * @param limit 
	 * @param testTypes
	 */
	public Map<String, Object> getTestDataByFTP(String type, String menuType,String ftptype, String startKey, String orgs, Date startDate, Date endDate, String limit) throws Exception{
		Integer max = Integer.parseInt(limit);
		Map<String,Object> respResult = new HashMap<String,Object> ();
		ArrayList<HashMap<String, String>> detail = new ArrayList<HashMap<String, String>>();
		Keyspace keyspaceOperator = null;
		try{
			keyspaceOperator = HFactory.createKeyspace(orgs, KeySpaceFactory.cluster);
		}catch (Exception e){
			e.printStackTrace();
		}
		RangeSlicesQuery<String, String, String> rangeQuery = HFactory
				.createRangeSlicesQuery(keyspaceOperator, KeySpaceFactory.stringSerializer, KeySpaceFactory.stringSerializer, KeySpaceFactory.stringSerializer);
		
		rangeQuery.setColumnFamily(type);
		rangeQuery.setRowCount(200);
		if(startKey==null || startKey.equals("")){
			startKey = "";
		}
		rangeQuery.setKeys(startKey, "");
		rangeQuery.setColumnNames("网络类型","网络制式","Network Type","network type","小区信息","Network cell","SINR","网络(1)SINR","Network(1) SINR","网络(1)信号强度","Network(1) Signal Strength","网络(1)小区信息","Network(1) Cell","网络(1)网络制式","Network(1) Type","Network (1) standard","网络(2)SINR","Network(2) SINR","网络(2)信号强度","Network(2) Signal Strength","网络(2)小区信息","Network(2) Cell","网络(2)网络制式","Network(2) Type","Network (2) standard","信号强度","Signal Strength","终端类型","终端型号","Terminal Model","terminal model","Terminal Type","Device Type","Terminal Mode","Device Model","固件版本","软件版本号","OS version","data_time","平均速率(Kbps)","Avg Speed(Kbps)","imei","LAC","CID","LAC/CID","TAC","PCI","TAC/PCI","cassandra_province","cassandra_city","cassandra_district","cassandra_street","cassandra_street_number","fileName","fileSize","filePath","file_lastModifies","类型","Type","文件","File","时长(s)","Duration(s)","平均速率(Mbps)","平均速率(Kbps)","Average Speed","Average Speed(Kbps)","Average Speed(Mbps)");
		QueryResult<OrderedRows<String, String, String>> result = rangeQuery
				.execute();
		OrderedRows<String, String, String> oRows = result.get();
		int count = 1;
		String lastKey = "";
		List<Row<String, String, String>> rowList = oRows.getList();
		for (Row<String, String, String> row : rowList) {
			if(row.getKey().equals(startKey)){
				continue;
			}
			HashMap<String, String> myData = new HashMap<String, String>();
			
			List<HColumn<String, String>> columns = row.getColumnSlice()
					.getColumns();
			String speed = "";

			String lac = "";
			String cid = "";
			String tac = "";
			String pci = "";
			String netType = "";
			String testTime = "";
			String deviceType = "";
			String toolsVersion = "";
			String imei = "";
			String cell_info = "";
			
			String province = "";
			String city = "";
			String district = "";
			String street = "";
			String street_no = "";
			
			String fileName = "";
			String filePath = "";
			String fileSize = "";
			String fileLastModifies = "";
			
			String fileType = "";
			String destFile = "";
			String destFileSize = "";
			String time = "";
			String modelvalue = "";
			String ftpvalue = "";
			boolean flag1 = false;//判断小区1网络制式是否存在
			boolean flag2 = false;//小区2
			String sinr = "";//SINR
			String netWorkType = "";//网络类型 或者 网络制式
			String signal = "";//信号强度
			
			String sinr1 = "";
			String sinr2 = "";
			String netWorkType1 = "";
			String netWorkType2 = "";
			String cell1 = "";
			String cell2 = "";
			String signal1 = "";
			String signal2 = "";
			for (HColumn<String, String> hColumn : columns) {
				String name = hColumn.getName();
				if(name.contains("终端类型") || name.contains("终端型号") || "Device Type".equals(hColumn.getName()) || "Terminal Model".equals(hColumn.getName()) || "Terminal Type".equals(hColumn.getName()) || "Terminal Mode".equals(hColumn.getName()) || "Device Model".equals(hColumn.getName()) || "terminal model".equals(hColumn.getName())){
					modelvalue = hColumn.getValue();
				}
				if(name.contains("网络(1)网络制式") || name.contains("Network(1) Type") || name.contains("Network (1) standard")){
					flag1 = true;
				}
				if(name.contains("网络(2)网络制式") || name.contains("Network(2) Type") || name.contains("Network (2) standard")){
					flag2 = true;
				}
				if("类型".equals(name)){
					ftpvalue = hColumn.getValue();
				}
			}
			if(menuType.equals("ios") && modelvalue!=null && !modelvalue.toLowerCase().contains("iphone")){
				continue;
			}else if(menuType.equals("android") && modelvalue!=null && modelvalue.toLowerCase().contains("iphone")){
				continue;
			}else if(ftptype.equals("upload") && ftpvalue!=null && (ftpvalue.equals("下行") || ftpvalue.equals(""))){
				continue;
			}else if(ftptype.equals("download") && ftpvalue!=null && ftpvalue.equals("上行")){
				continue;
			}
			for (HColumn<String, String> hColumn : columns) {
				if("平均速率(Kbps)".equals(hColumn.getName()) || "Avg Speed(Kbps)".equals(hColumn.getName())){
					speed = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("信号强度".equals(hColumn.getName()) || "Signal Strength".equals(hColumn.getName())){
					signal = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("data_time".equals(hColumn.getName())){
					testTime = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("终端类型".equals(hColumn.getName()) || "终端型号".equals(hColumn.getName()) || "Device Type".equals(hColumn.getName()) || "Terminal Model".equals(hColumn.getName()) || "Terminal Type".equals(hColumn.getName()) || "Terminal Mode".equals(hColumn.getName()) || "Device Model".equals(hColumn.getName()) || "terminal model".equals(hColumn.getName())){
					deviceType = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("固件版本".equals(hColumn.getName())||"软件版本号".equals(hColumn.getName()) || "OS version".equals(hColumn.getName())){
					toolsVersion = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("imei".equals(hColumn.getName())){
					imei = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("fileName".equals(hColumn.getName())){
					fileName = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("filePath".equals(hColumn.getName())){
					filePath = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("fileSize".equals(hColumn.getName())){
					fileSize = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("file_lastModifies".equals(hColumn.getName())){
					fileLastModifies = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("cassandra_province".equals(hColumn.getName())){
					province = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("cassandra_city".equals(hColumn.getName())){
					city = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("cassandra_district".equals(hColumn.getName())){
					district = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("cassandra_street".equals(hColumn.getName())){
					street = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("cassandra_street_no".equals(hColumn.getName())){
					street_no = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("LAC".equals(hColumn.getName())){
					String lacStr = hColumn.getValue()==null?"":hColumn.getValue();
					if(lacStr.indexOf("=")!=-1){
						lac = lacStr.substring(lacStr.indexOf("=")+1,lacStr.length());
					}else{
						lac = lacStr;
					}
				}else if("CID".equals(hColumn.getName())){
					String cidStr = hColumn.getValue()==null?"":hColumn.getValue();
					if(cidStr.indexOf("=")!=-1){
						cid = cidStr.substring(cidStr.indexOf("=")+1,cidStr.length());
					}else{
						cid = cidStr;
					}
				}else if("LAC/CID".equals(hColumn.getName())){
					String lacCid = hColumn.getValue()==null?"":hColumn.getValue();
					if(lacCid.indexOf("/")!=-1 || lacCid.indexOf("N/A")!=-1){
						String[] cellInfo = lacCid.split("/");
						lac = cellInfo[0];
						cid = cellInfo[1];
					}
				}else if("TAC".equals(hColumn.getName())){
					String tacStr = hColumn.getValue()==null?"":hColumn.getValue();
					if(tacStr.indexOf("=")!=-1){
						tac = tacStr.substring(tacStr.indexOf("=")+1,tacStr.length()-1);
					}else{
						tac = tacStr;
					}
				}else if("PCI".equals(hColumn.getName())){
					String pciStr = hColumn.getValue()==null?"":hColumn.getValue();
					if(pciStr.indexOf("=")!=-1){
						pci = pciStr.substring(pciStr.indexOf("=")+1,pciStr.length()-1);
					}else{
						pci = pciStr;
					}
				}else if("TAC/PCI".equals(hColumn.getName())){
					String tacPci = hColumn.getValue()==null?"":hColumn.getValue();
					if(tacPci.equals("N/A/N/A")){
						tac = "N/A";
						pci = "N/A";
					}else{
						if(tacPci.indexOf("/")!=-1 || tacPci.indexOf("N/A")!=-1){
							String[] cellInfo = tacPci.split("/");
							tac = cellInfo[0];
							pci = cellInfo[1];
						}
					}
				}else if("类型".equals(hColumn.getName()) || "Type".equals(hColumn.getName())){
					fileType = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("文件".equals(hColumn.getName()) || "File".equals(hColumn.getName())){
					destFile = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("时长(s)".equals(hColumn.getName()) || "Duration(s)".equals(hColumn.getName())){
					time = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("网络类型".equals(hColumn.getName()) || "网络制式".equals(hColumn.getName()) || "Network Type".equals(hColumn.getName()) || "network type".equals(hColumn.getName())){
					netWorkType = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("SINR".equals(hColumn.getName())){
					sinr = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("网络(1)网络制式".equals(hColumn.getName()) || "Network(1) Type".equals(hColumn.getName()) || "Network (1) standard".equals(hColumn.getName())){
					netWorkType1 = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("网络(1)信号强度".equals(hColumn.getName()) || "Network(1) Signal Strength".equals(hColumn.getName())){
					signal1 = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("网络(1)小区信息".equals(hColumn.getName()) || "Network(1) Cell".equals(hColumn.getName()) || "Network(1)".equals(hColumn.getName())){
					cell1 = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("网络(1)SINR".equals(hColumn.getName()) || "Network(1) SINR".equals(hColumn.getName())){
					sinr1 = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("网络(2)网络制式".equals(hColumn.getName()) || "Network(2) Type".equals(hColumn.getName()) || "Network (2) standard".equals(hColumn.getName())){
					netWorkType2 = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("网络(2)信号强度".equals(hColumn.getName()) || "Network(2) Signal Strength".equals(hColumn.getName())){
					signal2 = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("网络(2)小区信息".equals(hColumn.getName()) || "Network(2) Cell".equals(hColumn.getName())){
					cell2 = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("网络(2)SINR".equals(hColumn.getName()) || "Network(2) SINR".equals(hColumn.getName())){
					sinr2 = hColumn.getValue()==null?"":hColumn.getValue();
				}else{
					myData.put(hColumn.getName(), hColumn.getValue());
				}
			}

//			if(speed!=null && !speed.equals("") && destFile!=null && !destFile.equals("") && time!=null && !time.equals("") && fileType!=null){
				myData.put("target_address", destFile);
				myData.put("mean_rate", speed);
				if(destFile==null)
				{
					destFile = "";
				}
				if(fileType!=null && (fileType.equals("上行") || fileType.equals("Upload"))){
					myData.put("uploading_time", time);
					if(destFile.equals("")){
						myData.put("resource_size", destFile);	
					}else
					{
						myData.put("resource_size", destFile.substring(destFile.lastIndexOf("/")+1, destFile.lastIndexOf(".")));
					}
				}else{
					myData.put("downloading_time", time);
					if(destFile.equals("")){
						myData.put("resource_size", destFile);	
					}else
					{
						myData.put("resource_size", destFile.substring(destFile.lastIndexOf("/")+1, destFile.lastIndexOf(".")));
					}
				}
				lastKey = row.getKey();
//			}else{
//				continue;
//			}
			
				String lac1 = "";
				String cid1 = "";
				if(cell1.indexOf("/")!=-1 || cell1.indexOf("N/A")!=-1){
					String[] cellInfo = cell1.split("/");
					if(cellInfo[0]!=null && !cellInfo[0].equals("")){
						lac1 = cellInfo[0];
					}
					if(tac!=null && !tac.equals("")){
						lac1 = tac;
					}
					if(cellInfo[1]!=null && !cellInfo[1].equals("")){
						cid1 = cellInfo[1];
					}
					if(pci!=null && !pci.equals("")){
						cid1 = pci;
					}
					if(tac!=null && !tac.equals("") && cellInfo[0]!=null && !cellInfo[0].equals("")){
						lac1 = cellInfo[0]+"|"+tac;
					}
					if(pci!=null && !pci.equals("") && cellInfo[1]!=null && !cellInfo[1].equals("")){
						cid1 = cellInfo[1]+"|"+pci;
					}
				}
				String lac2 = "";
				String cid2 = "";
				if(cell2.indexOf("/")!=-1 || cell2.indexOf("N/A")!=-1){
					String[] cellInfo = cell2.split("/");
					if(cellInfo[0]!=null && !cellInfo[0].equals("")){
						lac2 = cellInfo[0];
					}
					if(tac!=null && !tac.equals("")){
						lac2 = tac;
					}
					if(cellInfo[1]!=null && !cellInfo[1].equals("")){
						cid2 = cellInfo[1];
					}
					if(pci!=null && !pci.equals("")){
						cid2 = pci;
					}
					if(tac!=null && !tac.equals("") && cellInfo[0]!=null && !cellInfo[0].equals("")){
						lac2 = cellInfo[0]+"|"+tac;
					}
					if(pci!=null && !pci.equals("") && cellInfo[1]!=null && !cellInfo[1].equals("")){
						cid2 = cellInfo[1]+"|"+pci;
					}
				}
				//网络(1)信息存在时 当前数据返回的是网络(1)的信息
				if(flag1 && netWorkType1.equals(netWorkType)){
					myData.put("sinr", sinr1);
					myData.put("networktype", netWorkType1);
					myData.put("signal", signal1);
					myData.put("lac", lac1);
					myData.put("cid", cid1);
				}
				//网络(2)信息存在时 当前数据返回的是网络(2)的信息
				if(flag2 && netWorkType2.equals(netWorkType)){
					myData.put("sinr", sinr2);
					myData.put("networktype", netWorkType2);
					myData.put("signal", signal2);
					myData.put("lac", lac2);
					myData.put("cid", cid2);
				}
				if(!(flag1 && netWorkType.equals(netWorkType1)) && !(flag2 && netWorkType.equals(netWorkType2))){
					myData.put("sinr", sinr);
					myData.put("networktype", netWorkType);
					myData.put("signal", signal);
					if(lac!=null && !lac.equals("")){
						myData.put("lac", lac);
					}
					if(tac!=null && !tac.equals("")){
						lac2 = tac;
						myData.put("lac", tac);
					}
					if(cid!=null && !cid.equals("")){
						myData.put("cid", cid);
					}
					if(pci!=null && !pci.equals("")){
						cid2 = pci;
						myData.put("cid", pci);
					}
					if(tac!=null && !tac.equals("") && lac!=null && !lac.equals("")){
						lac2 = lac+"|"+tac;
						myData.put("lac", lac+"|"+tac);	
					}
					if(pci!=null && !pci.equals("") && cid!=null && !cid.equals("")){
						cid2 = cid+"|"+pci;
						myData.put("cid", cid+"|"+pci);	
					}
				}
				myData.put("sinr1", sinr1);
				myData.put("networktype1", netWorkType1);
				myData.put("signal1", signal1);
				myData.put("lac1", lac1);
				myData.put("cid1", cid1);
				
				myData.put("sinr2", sinr2);
				myData.put("networktype2", netWorkType2);
				myData.put("signal2", signal2);
				myData.put("lac2", lac2);
				myData.put("cid2", cid2);
			myData.put("GSMLAC", lac);
			myData.put("GSMCID", cid);
			myData.put("LTETAC", tac);
			myData.put("LTEPCI", pci);
			if(netType.indexOf("LTE")!=-1){
				myData.put("LTESignal", signal);
			}else{
				myData.put("GSMSignal", signal);
			}
			if(!lac.isEmpty() && !cid.isEmpty()){
				cell_info = lac+","+cid;
			}
			
			myData.put("cell_info", cell_info);
			myData.put("test_location", province+city+district+street+street_no);
			myData.put("testing_time", testTime);
			myData.put("terminal_model", deviceType);
			myData.put("software_edition",toolsVersion);
			myData.put("imei", imei);
			myData.put("testSpeed",speed);
			myData.put("file_size",fileSize);
			myData.put("file_name", fileName);
			myData.put("file_path", filePath);
			myData.put("file_lastModifies", fileLastModifies);
			detail.add(myData);
			count++;
			if(count>max){
				break;
			}
		}
		respResult.put("total",Integer.parseInt(limit));
		respResult.put("detail",detail);
		respResult.put("last_key",lastKey);
		return respResult;
	}
	/**
	 * answer 05004
	 * @author Ocean
	 * @param province
	 * @param city
	 * @param district
	 * @param orgs
	 * @param limit 
	 * @param testTypes
	 */
	public Map<String, Object> getTestDataByAnswer(String type, String menuType, String startKey, String orgs, Date startDate, Date endDate, String limit) throws Exception{
		AudioDao ad = new AudioDao();
		Integer max = Integer.parseInt(limit);
		Map<String,Object> respResult = new HashMap<String,Object> ();
		ArrayList<HashMap<String, String>> detail = new ArrayList<HashMap<String, String>>();
		Keyspace keyspaceOperator = null;
		try{
			keyspaceOperator = HFactory.createKeyspace(orgs, KeySpaceFactory.cluster);
		}catch (Exception e1){
			e1.printStackTrace();
		}
		RangeSlicesQuery<String, String, String> rangeQuery = HFactory
				.createRangeSlicesQuery(keyspaceOperator, KeySpaceFactory.stringSerializer, KeySpaceFactory.stringSerializer, KeySpaceFactory.stringSerializer);
		
		rangeQuery.setColumnFamily(type);
		rangeQuery.setRowCount(200);
		if(startKey==null || startKey.equals("")){
			startKey = "";
		}
		rangeQuery.setKeys(startKey, "");
		//IOS 增加 接通时长(s)
		rangeQuery.setColumnNames("网络类型","网络制式","Network Type","network type","小区信息","Network cell","SINR","网络(1)SINR","Network(1) SINR","网络(1)信号强度","Network(1) Signal Strength","网络(1)小区信息","Network(1) Cell","网络(1)网络制式","Network(1) Type","Network (1) standard","网络(2)SINR","Network(2) SINR","网络(2)信号强度","Network(2) Signal Strength","网络(2)小区信息","Network(2) Cell","网络(2)网络制式","Network(2) Type","Network (2) standard","信号强度","Signal Strength","终端类型","终端型号","Terminal Model","terminal model","Terminal Type","Device Type","Terminal Mode","Device Model","固件版本","软件版本号","OS version","data_time","平均速率(Kbps)","Avg Speed(Kbps)","imei","LAC","CID","LAC/CID","TAC","PCI","TAC/PCI","cassandra_province","cassandra_city","cassandra_district","cassandra_street","cassandra_street_number","fileName","fileSize","filePath","file_lastModifies","是否成功","成功率","接续时长(ms)","接通时长(ms)","接通时长(s)","是否录音","手机号码","终端型号","网络","fallbackToCs(ms)","FallBack To CS(s)");
		QueryResult<OrderedRows<String, String, String>> result = rangeQuery
				.execute();
		OrderedRows<String, String, String> oRows = result.get();
		int count = 1;
		String lastKey = "";
		List<Row<String, String, String>> rowList = oRows.getList();
		for (Row<String, String, String> row : rowList) {
			if(row.getKey().equals(startKey)){
				continue;
			}
			System.out.println(row.getKey());
			HashMap<String, String> myData = new HashMap<String, String>();
			
			List<HColumn<String, String>> columns = row.getColumnSlice()
					.getColumns();
			String speed = "";

			String lac = "";
			String cid = "";
			String tac = "";
			String pci = "";
			String netType = "";
			String testTime = "";
			String deviceType = "";
			String toolsVersion = "";
			String imei = "";
			String cell_info = "";
			
			String province = "";
			String city = "";
			String district = "";
			String street = "";
			String street_no = "";
			
			String fileName = "";
			String filePath = "";
			String fileSize = "";
			String fileLastModifies = "";
			
			String success_failure = "";
			String access_time = "";
			String access_time1 = "";
			String access_time2 = "";
			String called_number = "";//被叫目标号码
			String called_network_type = "";//被叫网络类型
			String caller_terminal_mode = "";//主叫型号
			String hand_over_situation = "";
			String recording = "";//是否录音
			
			String modelvalue = "";
			boolean flag1 = false;//判断小区1网络制式是否存在
			boolean flag2 = false;//小区2
			String sinr = "";//SINR
			String netWorkType = "";//网络类型 或者 网络制式
			String signal = "";//信号强度
			
			String sinr1 = "";
			String sinr2 = "";
			String netWorkType1 = "";
			String netWorkType2 = "";
			String cell1 = "";
			String cell2 = "";
			String signal1 = "";
			String signal2 = "";
			for (HColumn<String, String> hColumn : columns) {
				String name = hColumn.getName();
				if(name.contains("终端类型") || name.contains("终端型号") || "Device Type".equals(hColumn.getName()) || "Terminal Model".equals(hColumn.getName()) || "Terminal Type".equals(hColumn.getName()) || "Terminal Mode".equals(hColumn.getName()) || "Device Model".equals(hColumn.getName()) || "terminal model".equals(hColumn.getName())){
					modelvalue = hColumn.getValue();
				}
				if(name.contains("网络(1)网络制式") || name.contains("Network(1) Type") || name.contains("Network (1) standard")){
					flag1 = true;
				}
				if(name.contains("网络(2)网络制式") || name.contains("Network(2) Type") || name.contains("Network (2) standard")){
					flag2 = true;
				}
			}
			if(menuType.equals("ios") && modelvalue!=null && !modelvalue.toLowerCase().contains("iphone")){
				continue;
			}else if(menuType.equals("android") && modelvalue!=null && modelvalue.toLowerCase().contains("iphone")){
				continue;
			}
			for (HColumn<String, String> hColumn : columns) {
				if("平均速率(Kbps)".equals(hColumn.getName()) || "Avg Speed(Kbps)".equals(hColumn.getName())){
					speed = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("信号强度".equals(hColumn.getName()) || "Signal Strength".equals(hColumn.getName())){
					signal = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("data_time".equals(hColumn.getName())){
					testTime = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("终端类型".equals(hColumn.getName()) || "终端型号".equals(hColumn.getName()) || "Device Type".equals(hColumn.getName()) || "Terminal Model".equals(hColumn.getName()) || "Terminal Type".equals(hColumn.getName()) || "Terminal Mode".equals(hColumn.getName()) || "Device Model".equals(hColumn.getName()) || "terminal model".equals(hColumn.getName())){
					deviceType = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("固件版本".equals(hColumn.getName())||"软件版本号".equals(hColumn.getName()) || "OS version".equals(hColumn.getName())){
					toolsVersion = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("imei".equals(hColumn.getName())){
					imei = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("fileName".equals(hColumn.getName())){
					fileName = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("filePath".equals(hColumn.getName())){
					filePath = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("fileSize".equals(hColumn.getName())){
					fileSize = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("file_lastModifies".equals(hColumn.getName())){
					fileLastModifies = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("cassandra_province".equals(hColumn.getName())){
					province = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("cassandra_city".equals(hColumn.getName())){
					city = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("cassandra_district".equals(hColumn.getName())){
					district = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("cassandra_street".equals(hColumn.getName())){
					street = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("cassandra_street_no".equals(hColumn.getName())){
					street_no = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("LAC".equals(hColumn.getName())){
					String lacStr = hColumn.getValue()==null?"":hColumn.getValue();
					if(lacStr.indexOf("=")!=-1){
						lac = lacStr.substring(lacStr.indexOf("=")+1,lacStr.length());
					}else{
						lac = lacStr;
					}
				}else if("CID".equals(hColumn.getName())){
					String cidStr = hColumn.getValue()==null?"":hColumn.getValue();
					if(cidStr.indexOf("=")!=-1){
						cid = cidStr.substring(cidStr.indexOf("=")+1,cidStr.length());
					}else{
						cid = cidStr;
					}
				}else if("LAC/CID".equals(hColumn.getName())){
					String lacCid = hColumn.getValue()==null?"":hColumn.getValue();
					if(lacCid.indexOf("/")!=-1 || lacCid.indexOf("N/A")!=-1){
						String[] cellInfo = lacCid.split("/");
						lac = cellInfo[0];
						cid = cellInfo[1];
					}
				}else if("TAC".equals(hColumn.getName())){
					String tacStr = hColumn.getValue()==null?"":hColumn.getValue();
					if(tacStr.indexOf("=")!=-1){
						tac = tacStr.substring(tacStr.indexOf("=")+1,tacStr.length()-1);
					}else{
						tac = tacStr;
					}
				}else if("PCI".equals(hColumn.getName())){
					String pciStr = hColumn.getValue()==null?"":hColumn.getValue();
					if(pciStr.indexOf("=")!=-1){
						pci = pciStr.substring(pciStr.indexOf("=")+1,pciStr.length()-1);
					}else{
						pci = pciStr;
					}
				}else if("TAC/PCI".equals(hColumn.getName())){
					String tacPci = hColumn.getValue()==null?"":hColumn.getValue();
					if(tacPci.equals("N/A/N/A")){
						tac = "N/A";
						pci = "N/A";
					}else{
						if(tacPci.indexOf("/")!=-1 || tacPci.indexOf("N/A")!=-1){
							String[] cellInfo = tacPci.split("/");
							tac = cellInfo[0];
							pci = cellInfo[1];
						}
					}
				}else if("是否成功".equals(hColumn.getName()) || "成功率".equals(hColumn.getName())){//"是否成功","通话时长","手机号码","请求类型","网络","fallbackToCs(ms)"
					success_failure = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("接续时长(ms)".equals(hColumn.getName())){
					access_time = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("接通时长(ms)".equals(hColumn.getName())){
					access_time1 = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("接通时长(s)".equals(hColumn.getName())){
					access_time2 = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("手机号码".equals(hColumn.getName())){
					called_number = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("终端型号".equals(hColumn.getName())){
					caller_terminal_mode = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("网络".equals(hColumn.getName())){
					called_network_type = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("fallbackToCs(ms)".equals(hColumn.getName()) || "FallBack To CS(s)".equals(hColumn.getName())){
					hand_over_situation = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("是否录音".equals(hColumn.getName())){
					recording = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("网络类型".equals(hColumn.getName()) || "网络制式".equals(hColumn.getName()) || "Network Type".equals(hColumn.getName()) || "network type".equals(hColumn.getName())){
					netWorkType = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("SINR".equals(hColumn.getName())){
					sinr = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("网络(1)网络制式".equals(hColumn.getName()) || "Network(1) Type".equals(hColumn.getName()) || "Network (1) standard".equals(hColumn.getName())){
					netWorkType1 = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("网络(1)信号强度".equals(hColumn.getName()) || "Network(1) Signal Strength".equals(hColumn.getName())){
					signal1 = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("网络(1)小区信息".equals(hColumn.getName()) || "Network(1) Cell".equals(hColumn.getName()) || "Network(1)".equals(hColumn.getName())){
					cell1 = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("网络(1)SINR".equals(hColumn.getName()) || "Network(1) SINR".equals(hColumn.getName())){
					sinr1 = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("网络(2)网络制式".equals(hColumn.getName()) || "Network(2) Type".equals(hColumn.getName()) || "Network (2) standard".equals(hColumn.getName())){
					netWorkType2 = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("网络(2)信号强度".equals(hColumn.getName()) || "Network(2) Signal Strength".equals(hColumn.getName())){
					signal2 = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("网络(2)小区信息".equals(hColumn.getName()) || "Network(2) Cell".equals(hColumn.getName())){
					cell2 = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("网络(2)SINR".equals(hColumn.getName()) || "Network(2) SINR".equals(hColumn.getName())){
					sinr2 = hColumn.getValue()==null?"":hColumn.getValue();
				}else{
					myData.put(hColumn.getName(), hColumn.getValue());
				}
			}
			//查询mysql数据库中数据 
			String prefix = row.getKey().split("\\|")[1];
			prefix = prefix.substring(0,prefix.lastIndexOf("."));
			prefix = prefix.substring(0, prefix.lastIndexOf("."));
			String audio_name = "";
			String audio_path = "";
			String data_state = "";
			String store_path = "";
			String store_path_rela = "";
			String terminal_num = "";//PhoneNum
//			System.out.println(imei+","+prefix);
			List data_list = ad.query(imei, prefix);
			if(data_list!=null && data_list.size()>0){
				HashMap map =(HashMap)data_list.get(0);
				audio_name = map.get("name")==null?"":map.get("name").toString();
				audio_path = map.get("load_url")==null?"":map.get("load_url").toString();
				data_state = map.get("data_state")==null?"":map.get("data_state").toString();
				store_path = map.get("store_path")==null?"":map.get("store_path").toString();
				store_path_rela = map.get("store_path_rela")==null?"":map.get("store_path_rela").toString();
				terminal_num = map.get("device_no")==null?"":map.get("device_no").toString();
				myData.put("caller_number", terminal_num);
				myData.put("sample", audio_name);
				myData.put("sample_path",audio_path);
				if(data_state.equals("1")){
					myData.put("recording_path",store_path.contains(";")?store_path.split(";")[0]:store_path);
					myData.put("grade_path",store_path_rela.contains(";")?store_path_rela.split(";")[0]:store_path_rela);				
				}else{
					myData.put("recording_path",store_path.contains(";")?store_path.split(";")[0]:store_path);
				}
			}
		//			if(success_failure!=null && !success_failure.equals("") && access_time!=null && access_time1!=null && called_number!=null && !called_number.equals("") && caller_terminal_mode!=null && !caller_terminal_mode.equals("") && network_type!=null && !network_type.equals("") && hand_over_situation!=null && !hand_over_situation.equals("")){
				myData.put("success_failure",success_failure);
				if(access_time!=null && access_time.equals("")||access_time.equals("0")||access_time.equals("null")){
					if(access_time1!=null && access_time1.equals("")||access_time1.equals("0")||access_time1.equals("null")){
						if(access_time2!=null && access_time1.equals("")||access_time1.equals("0")||access_time1.equals("null")){
							myData.put("access_time", access_time1);
						}else{
							myData.put("access_time", access_time2);	
						}
					}else{
						myData.put("access_time", access_time1);	
					}
				}else{
					myData.put("access_time", access_time);					
				}
				myData.put("called_number", called_number);
				myData.put("called_terminal_model", caller_terminal_mode);
				myData.put("caller_terminal_model", "");
//				myData.put("belonging_network_type", called_network_type);
				myData.put("hand_over_situation", hand_over_situation);
				myData.put("recording", recording);
				lastKey = row.getKey();
//			}else{
//				continue;
//			}
			String lac1 = "";
			String cid1 = "";
			if(cell1.indexOf("/")!=-1 || cell1.indexOf("N/A")!=-1){
				String[] cellInfo = cell1.split("/");
				if(cellInfo[0]!=null && !cellInfo[0].equals("")){
					lac1 = cellInfo[0];
				}
				if(tac!=null && !tac.equals("")){
					lac1 = tac;
				}
				if(cellInfo[1]!=null && !cellInfo[1].equals("")){
					cid1 = cellInfo[1];
				}
				if(pci!=null && !pci.equals("")){
					cid1 = pci;
				}
				if(tac!=null && !tac.equals("") && cellInfo[0]!=null && !cellInfo[0].equals("")){
					lac1 = cellInfo[0]+"|"+tac;
				}
				if(pci!=null && !pci.equals("") && cellInfo[1]!=null && !cellInfo[1].equals("")){
					cid1 = cellInfo[1]+"|"+pci;
				}
			}
			String lac2 = "";
			String cid2 = "";
			if(cell2.indexOf("/")!=-1 || cell2.indexOf("N/A")!=-1){
				String[] cellInfo = cell2.split("/");
				if(cellInfo[0]!=null && !cellInfo[0].equals("")){
					lac2 = cellInfo[0];
				}
				if(tac!=null && !tac.equals("")){
					lac2 = tac;
				}
				if(cellInfo[1]!=null && !cellInfo[1].equals("")){
					cid2 = cellInfo[1];
				}
				if(pci!=null && !pci.equals("")){
					cid2 = pci;
				}
				if(tac!=null && !tac.equals("") && cellInfo[0]!=null && !cellInfo[0].equals("")){
					lac2 = cellInfo[0]+"|"+tac;
				}
				if(pci!=null && !pci.equals("") && cellInfo[1]!=null && !cellInfo[1].equals("")){
					cid2 = cellInfo[1]+"|"+pci;
				}
			}
			//网络(1)信息存在时 当前数据返回的是网络(1)的信息
			if(flag1 && netWorkType1.equals(netWorkType)){
				myData.put("sinr", sinr1);
				myData.put("belonging_network_type", netWorkType1);
				myData.put("signal", signal1);
				myData.put("lac", lac1);
				myData.put("cid", cid1);
			}
			//网络(2)信息存在时 当前数据返回的是网络(2)的信息
			if(flag2 && netWorkType2.equals(netWorkType)){
				myData.put("sinr", sinr2);
				myData.put("belonging_network_type", netWorkType2);
				myData.put("signal", signal2);
				myData.put("lac", lac2);
				myData.put("cid", cid2);
			}
			if(!(flag1 && netWorkType.equals(netWorkType1)) && !(flag2 && netWorkType.equals(netWorkType2))){
				myData.put("sinr", sinr);
				myData.put("belonging_network_type", netWorkType);
				myData.put("signal", signal);
				if(lac!=null && !lac.equals("")){
					myData.put("lac", lac);
				}
				if(tac!=null && !tac.equals("")){
					lac2 = tac;
					myData.put("lac", tac);
				}
				if(cid!=null && !cid.equals("")){
					myData.put("cid", cid);
				}
				if(pci!=null && !pci.equals("")){
					cid2 = pci;
					myData.put("cid", pci);
				}
				if(tac!=null && !tac.equals("") && lac!=null && !lac.equals("")){
					lac2 = lac+"|"+tac;
					myData.put("lac", lac+"|"+tac);	
				}
				if(pci!=null && !pci.equals("") && cid!=null && !cid.equals("")){
					cid2 = cid+"|"+pci;
					myData.put("cid", cid+"|"+pci);	
				}
			}
			myData.put("sinr1", sinr1);
			myData.put("networktype1", netWorkType1);
			myData.put("signal1", signal1);
			myData.put("lac1", lac1);
			myData.put("cid1", cid1);
			
			myData.put("sinr2", sinr2);
			myData.put("networktype2", netWorkType2);
			myData.put("signal2", signal2);
			myData.put("lac2", lac2);
			myData.put("cid2", cid2);
			
			myData.put("GSMLAC", lac);
			myData.put("GSMCID", cid);
			myData.put("LTETAC", tac);
			myData.put("LTEPCI", pci);
			if(netType.indexOf("LTE")!=-1){
				myData.put("LTESignal", signal);
			}else{
				myData.put("GSMSignal", signal);
			}
			if(!lac.isEmpty() && !cid.isEmpty()){
				cell_info = lac+","+cid;
			}
			
			myData.put("cell_info", cell_info);
			myData.put("test_location", province+city+district+street+street_no);
			myData.put("testing_time", testTime);
			myData.put("terminal_model", deviceType);
			myData.put("software_edition",toolsVersion);
			myData.put("imei", imei);
			myData.put("testSpeed",speed);
			myData.put("file_size",fileSize);
			myData.put("file_name", fileName);
			myData.put("file_path", filePath);
			myData.put("file_lastModifies", fileLastModifies);
			detail.add(myData);
			count++;
			if(count>max){
				break;
			}
		}
		respResult.put("total",Integer.parseInt(limit));
		respResult.put("detail",detail);
		respResult.put("last_key",lastKey);
		return respResult;
	}
	/**
	 * sms 05002
	 * @author Ocean
	 */
	public Map<String, Object> getTestDataBySMS(String type, String menuType, String startKey, String orgs, Date startDate, Date endDate, String limit) throws Exception{
		AudioDao ad = new AudioDao();
		Integer max = Integer.parseInt(limit);
		Map<String,Object> respResult = new HashMap<String,Object> ();
		ArrayList<HashMap<String, String>> detail = new ArrayList<HashMap<String, String>>();
		Keyspace keyspaceOperator = null;
		try{
			keyspaceOperator = HFactory.createKeyspace(orgs, KeySpaceFactory.cluster);
		}catch (Exception e1){
			e1.printStackTrace();
		}
		RangeSlicesQuery<String, String, String> rangeQuery = HFactory
				.createRangeSlicesQuery(keyspaceOperator, KeySpaceFactory.stringSerializer, KeySpaceFactory.stringSerializer, KeySpaceFactory.stringSerializer);
		
		rangeQuery.setColumnFamily(type);
		rangeQuery.setRowCount(200);
		if(startKey==null || startKey.equals("")){
			startKey = "";
		}
		rangeQuery.setKeys(startKey, "");
		//IOS 增加 接通时长(s)
		rangeQuery.setColumnNames("网络类型","网络制式","Network Type","network type","小区信息","Network cell","SINR","网络(1)SINR","Network(1) SINR","网络(1)信号强度","Network(1) Signal Strength","网络(1)小区信息","Network(1)","Network(1) Cell","网络(1)网络制式","Network(1) Type","Network (1) standard","Network (1) standard","网络(2)SINR","Network(2) SINR","网络(2)信号强度","Network(2) Signal Strength","网络(2)小区信息","Network(2)","Network(2) Cell","网络(2)网络制式","Network(2) Type","Network (2) standard","Network (2) standard","信号强度","Signal Strength","终端类型","终端型号"," Terminal Models","Terminal Model","terminal model","Terminal Type","Device Type","Terminal Mode","Device Model","固件版本","软件版本号","OS version","data_time","平均速率(Kbps)","Avg Speed(Kbps)","imei","LAC","CID","LAC/CID","TAC","PCI","TAC/PCI","cassandra_province","cassandra_city","cassandra_district","cassandra_street","cassandra_street_number","fileName","fileSize","filePath","file_lastModifies","Test times","Send SMS Counts","测试次数","Success Rate","成功率","Succ counts","成功次数","时延(ms)","手机号码","Delay(ms)","phone number ","Phone Number");
		QueryResult<OrderedRows<String, String, String>> result = rangeQuery
				.execute();
		OrderedRows<String, String, String> oRows = result.get();
		int count = 1;
		String lastKey = "";
		List<Row<String, String, String>> rowList = oRows.getList();
		for (Row<String, String, String> row : rowList) {
			if(row.getKey().equals(startKey)){
				continue;
			}
			System.out.println(row.getKey());
			HashMap<String, String> myData = new HashMap<String, String>();
			
			List<HColumn<String, String>> columns = row.getColumnSlice()
					.getColumns();
			String speed = "";

			String lac = "";
			String cid = "";
			String tac = "";
			String pci = "";
			String netType = "";
			String testTime = "";
			String deviceType = "";
			String toolsVersion = "";
			String imei = "";
			String cell_info = "";
			
			String province = "";
			String city = "";
			String district = "";
			String street = "";
			String street_no = "";
			
			String fileName = "";
			String filePath = "";
			String fileSize = "";
			String fileLastModifies = "";
			
			String testTimes = "";//测试次数
			String succTimes = "";//成功次数
			String succRate = "";//成功率
			String delay = "";//时延
			String target_number = "";
			
			String modelvalue = "";
			boolean flag1 = false;//判断小区1网络制式是否存在
			boolean flag2 = false;//小区2
			String sinr = "";//SINR
			String netWorkType = "";//网络类型 或者 网络制式
			String signal = "";//信号强度
			
			String sinr1 = "";
			String sinr2 = "";
			String netWorkType1 = "";
			String netWorkType2 = "";
			String cell1 = "";
			String cell2 = "";
			String signal1 = "";
			String signal2 = "";
			for (HColumn<String, String> hColumn : columns) {
				String name = hColumn.getName();
				if(name.contains("终端类型") || name.contains("终端型号") || " Terminal Models".equals(hColumn.getName()) ||"Device Type".equals(hColumn.getName()) || "Terminal Model".equals(hColumn.getName()) || "Terminal Type".equals(hColumn.getName()) || "Terminal Mode".equals(hColumn.getName()) || "Device Model".equals(hColumn.getName()) || "terminal model".equals(hColumn.getName())){
					modelvalue = hColumn.getValue();
				}
				if(name.contains("网络(1)网络制式") || name.contains("Network(1) Type") || name.contains("Network (1) standard")){
					flag1 = true;
				}
				if(name.contains("网络(2)网络制式") || name.contains("Network(2) Type") || name.contains("Network (2) standard")){
					flag2 = true;
				}
			}
			if(menuType.equals("ios") && modelvalue!=null && !modelvalue.toLowerCase().contains("iphone")){
				continue;
			}else if(menuType.equals("android") && modelvalue!=null && modelvalue.toLowerCase().contains("iphone")){
				continue;
			}
			for (HColumn<String, String> hColumn : columns) {
				if("平均速率(Kbps)".equals(hColumn.getName()) || "Avg Speed(Kbps)".equals(hColumn.getName())){
					speed = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("信号强度".equals(hColumn.getName()) || "Signal Strength".equals(hColumn.getName())){
					signal = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("data_time".equals(hColumn.getName())){
					testTime = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("终端类型".equals(hColumn.getName()) || "终端型号".equals(hColumn.getName()) || "Device Type".equals(hColumn.getName()) || "Terminal Model".equals(hColumn.getName()) || "Terminal Type".equals(hColumn.getName()) || "Terminal Mode".equals(hColumn.getName()) || "Device Model".equals(hColumn.getName()) || "terminal model".equals(hColumn.getName())){
					deviceType = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("固件版本".equals(hColumn.getName())||"软件版本号".equals(hColumn.getName()) || "OS version".equals(hColumn.getName())){
					toolsVersion = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("imei".equals(hColumn.getName())){
					imei = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("fileName".equals(hColumn.getName())){
					fileName = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("filePath".equals(hColumn.getName())){
					filePath = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("fileSize".equals(hColumn.getName())){
					fileSize = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("file_lastModifies".equals(hColumn.getName())){
					fileLastModifies = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("cassandra_province".equals(hColumn.getName())){
					province = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("cassandra_city".equals(hColumn.getName())){
					city = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("cassandra_district".equals(hColumn.getName())){
					district = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("cassandra_street".equals(hColumn.getName())){
					street = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("cassandra_street_no".equals(hColumn.getName())){
					street_no = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("LAC".equals(hColumn.getName())){
					String lacStr = hColumn.getValue()==null?"":hColumn.getValue();
					if(lacStr.indexOf("=")!=-1){
						lac = lacStr.substring(lacStr.indexOf("=")+1,lacStr.length());
					}else{
						lac = lacStr;
					}
				}else if("CID".equals(hColumn.getName())){
					String cidStr = hColumn.getValue()==null?"":hColumn.getValue();
					if(cidStr.indexOf("=")!=-1){
						cid = cidStr.substring(cidStr.indexOf("=")+1,cidStr.length());
					}else{
						cid = cidStr;
					}
				}else if("LAC/CID".equals(hColumn.getName())){
					String lacCid = hColumn.getValue()==null?"":hColumn.getValue();
					if(lacCid.indexOf("/")!=-1 || lacCid.indexOf("N/A")!=-1){
						String[] cellInfo = lacCid.split("/");
						lac = cellInfo[0];
						cid = cellInfo[1];
					}
				}else if("TAC".equals(hColumn.getName())){
					String tacStr = hColumn.getValue()==null?"":hColumn.getValue();
					if(tacStr.indexOf("=")!=-1){
						tac = tacStr.substring(tacStr.indexOf("=")+1,tacStr.length()-1);
					}else{
						tac = tacStr;
					}
				}else if("PCI".equals(hColumn.getName())){
					String pciStr = hColumn.getValue()==null?"":hColumn.getValue();
					if(pciStr.indexOf("=")!=-1){
						pci = pciStr.substring(pciStr.indexOf("=")+1,pciStr.length()-1);
					}else{
						pci = pciStr;
					}
				}else if("TAC/PCI".equals(hColumn.getName())){
					String tacPci = hColumn.getValue()==null?"":hColumn.getValue();
					if(tacPci.equals("N/A/N/A")){
						tac = "N/A";
						pci = "N/A";
					}else{
						if(tacPci.indexOf("/")!=-1 || tacPci.indexOf("N/A")!=-1){
							String[] cellInfo = tacPci.split("/");
							tac = cellInfo[0];
							pci = cellInfo[1];
						}
					}
				}else if("时延(ms)".equals(hColumn.getName()) || "Delay(ms)".equals(hColumn.getName())){
					delay = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("手机号码".equals(hColumn.getName()) || "phone number".equals(hColumn.getName()) || "Phone Number".equals(hColumn.getName())){
					target_number = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("测试次数".equals(hColumn.getName()) || "Test times ".equals(hColumn.getName()) || "Send SMS Counts".equals(hColumn.getName())){
					testTimes = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("成功率".equals(hColumn.getName()) || "Success Rate".equals(hColumn.getName())){
					succRate = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("成功次数".equals(hColumn.getName()) || "Succ counts".equals(hColumn.getName())){
					succTimes = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("网络类型".equals(hColumn.getName()) || "网络制式".equals(hColumn.getName()) || "Network Type".equals(hColumn.getName()) || "network type".equals(hColumn.getName())){
					netWorkType = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("SINR".equals(hColumn.getName())){
					sinr = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("网络(1)网络制式".equals(hColumn.getName()) || "Network(1) Type".equals(hColumn.getName()) || "Network (1) standard".equals(hColumn.getName())){
					netWorkType1 = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("网络(1)信号强度".equals(hColumn.getName()) || "Network(1) Signal Strength".equals(hColumn.getName())){
					signal1 = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("网络(1)小区信息".equals(hColumn.getName()) || "Network(1) Cell".equals(hColumn.getName()) || "Network(1)".equals(hColumn.getName())){
					cell1 = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("网络(1)SINR".equals(hColumn.getName()) || "Network(1) SINR".equals(hColumn.getName())){
					sinr1 = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("网络(2)网络制式".equals(hColumn.getName()) || "Network(2) Type".equals(hColumn.getName()) || "Network (2) standard".equals(hColumn.getName())){
					netWorkType2 = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("网络(2)信号强度".equals(hColumn.getName()) || "Network(2) Signal Strength".equals(hColumn.getName())){
					signal2 = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("网络(2)小区信息".equals(hColumn.getName()) || "Network(2) Cell".equals(hColumn.getName()) || "Network(2)".equals(hColumn.getName())){
					cell2 = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("网络(2)SINR".equals(hColumn.getName()) || "Network(2) SINR".equals(hColumn.getName())){
					sinr2 = hColumn.getValue()==null?"":hColumn.getValue();
				}else{
					myData.put(hColumn.getName(), hColumn.getValue());
				}
			}

			lastKey = row.getKey();
			String lac1 = "";
			String cid1 = "";
			if(cell1.indexOf("/")!=-1 || cell1.indexOf("N/A")!=-1){
				String[] cellInfo = cell1.split("/");
				if(cellInfo[0]!=null && !cellInfo[0].equals("")){
					lac1 = cellInfo[0];
				}
				if(tac!=null && !tac.equals("")){
					lac1 = tac;
				}
				if(cellInfo[1]!=null && !cellInfo[1].equals("")){
					cid1 = cellInfo[1];
				}
				if(pci!=null && !pci.equals("")){
					cid1 = pci;
				}
				if(tac!=null && !tac.equals("") && cellInfo[0]!=null && !cellInfo[0].equals("")){
					lac1 = cellInfo[0]+"|"+tac;
				}
				if(pci!=null && !pci.equals("") && cellInfo[1]!=null && !cellInfo[1].equals("")){
					cid1 = cellInfo[1]+"|"+pci;
				}
			}
			String lac2 = "";
			String cid2 = "";
			if(cell2.indexOf("/")!=-1 || cell2.indexOf("N/A")!=-1){
				String[] cellInfo = cell2.split("/");
				if(cellInfo[0]!=null && !cellInfo[0].equals("")){
					lac2 = cellInfo[0];
				}
				if(tac!=null && !tac.equals("")){
					lac2 = tac;
				}
				if(cellInfo[1]!=null && !cellInfo[1].equals("")){
					cid2 = cellInfo[1];
				}
				if(pci!=null && !pci.equals("")){
					cid2 = pci;
				}
				if(tac!=null && !tac.equals("") && cellInfo[0]!=null && !cellInfo[0].equals("")){
					lac2 = cellInfo[0]+"|"+tac;
				}
				if(pci!=null && !pci.equals("") && cellInfo[1]!=null && !cellInfo[1].equals("")){
					cid2 = cellInfo[1]+"|"+pci;
				}
			}
			//网络(1)信息存在时 当前数据返回的是网络(1)的信息
			if(flag1 && netWorkType1.equals(netWorkType)){
				myData.put("sinr", sinr1);
				myData.put("networktype", netWorkType);
				myData.put("signal", signal1);
				myData.put("lac", lac1);
				myData.put("cid", cid1);
			}
			//网络(2)信息存在时 当前数据返回的是网络(2)的信息
			if(flag2 && netWorkType2.equals(netWorkType)){
				myData.put("sinr", sinr2);
				myData.put("networktype", netWorkType);
				myData.put("signal", signal2);
				myData.put("lac", lac2);
				myData.put("cid", cid2);
			}
			if(!(flag1 && netWorkType.equals(netWorkType1)) && !(flag2 && netWorkType.equals(netWorkType2))){
				myData.put("sinr", sinr);
				myData.put("networktype", netWorkType);
				myData.put("signal", signal);
				if(lac!=null && !lac.equals("")){
					myData.put("lac", lac);
				}
				if(tac!=null && !tac.equals("")){
					lac2 = tac;
					myData.put("lac", tac);
				}
				if(cid!=null && !cid.equals("")){
					myData.put("cid", cid);
				}
				if(pci!=null && !pci.equals("")){
					cid2 = pci;
					myData.put("cid", pci);
				}
				if(tac!=null && !tac.equals("") && lac!=null && !lac.equals("")){
					lac2 = lac+"|"+tac;
					myData.put("lac", lac+"|"+tac);	
				}
				if(pci!=null && !pci.equals("") && cid!=null && !cid.equals("")){
					cid2 = cid+"|"+pci;
					myData.put("cid", cid+"|"+pci);	
				}
			}
			myData.put("sinr1", sinr1);
			myData.put("networktype1", netWorkType1);
			myData.put("signal1", signal1);
			myData.put("lac1", lac1);
			myData.put("cid1", cid1);
			
			myData.put("sinr2", sinr2);
			myData.put("networktype2", netWorkType2);
			myData.put("signal2", signal2);
			myData.put("lac2", lac2);
			myData.put("cid2", cid2);
			
			myData.put("GSMLAC", lac);
			myData.put("GSMCID", cid);
			myData.put("LTETAC", tac);
			myData.put("LTEPCI", pci);
			if(netType.indexOf("LTE")!=-1){
				myData.put("LTESignal", signal);
			}else{
				myData.put("GSMSignal", signal);
			}
			if(!lac.isEmpty() && !cid.isEmpty()){
				cell_info = lac+","+cid;
			}
			
			myData.put("delay", delay);
			myData.put("target_number", target_number);
			myData.put("test_times", testTimes);
			myData.put("success_times", succTimes);
			myData.put("success_rate", succRate);
			
			myData.put("cell_info", cell_info);
			myData.put("test_location", province+city+district+street+street_no);
			myData.put("testing_time", testTime);
			myData.put("terminal_model", deviceType);
			myData.put("software_edition",toolsVersion);
			myData.put("imei", imei);
			myData.put("testSpeed",speed);
			myData.put("file_size",fileSize);
			myData.put("file_name", fileName);
			myData.put("file_path", filePath);
			myData.put("file_lastModifies", fileLastModifies);
			detail.add(myData);
			count++;
			if(count>max){
				break;
			}
		}
		respResult.put("total",Integer.parseInt(limit));
		respResult.put("detail",detail);
		respResult.put("last_key",lastKey);
		return respResult;
	}
	/**
	 * 返回起始时间所在时间段的天数 yyyyMMdd
	 * @param startDate
	 * @param endDate
	 * @return
	 */
	private List<String> parseDays(Date startDate, Date endDate) throws Exception{
		List<String> days = new ArrayList<String>();
		SimpleDateFormat dateFormat = new SimpleDateFormat(
				"yyyyMMdd");
		String startStr = dateFormat.format(startDate);
		long startDay = (dateFormat.parse(startStr)).getTime();

		String endStr = dateFormat.format(endDate);
		long endDay = (dateFormat.parse(endStr)).getTime();
		
		
		if((endDay-startDay)<24*3600*1000){
			days.add(startStr);
		}else if((endDay-startDay)<=24*3600*1000){
			days.add(startStr);
			days.add(endStr);
		}else{
			while(startDay<endDay){
				String day = dateFormat.format(startDay);
				days.add(day);
				startDay += 24*3600*1000;
			}
			days.add(endStr);
		}
		return days;
	} 
	/*
	public static void main(String[] args) {
//		String startTime = "2014-11-13 00:00:00";
//		String endTime = "2014-11-14 00:00:00";
//		SimpleDateFormat dateFormat = new SimpleDateFormat(
//				"yyyy-MM-dd HH:mm:ss");
//		Date startDate = null;
//		try {
//			startDate = dateFormat.parse(startTime);
//		} catch (ParseException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		Date endDate = null;
//		try {
//			endDate = dateFormat.parse(endTime);
//		} catch (ParseException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		if((startDate.getTime()-endDate.getTime())>0){
//			System.out.println("ERROR");
//		}
//		TestDataAnalyze myTDA = new TestDataAnalyze();
//		try {
//			List<String> myDays = myTDA.parseDays(startDate, endDate);
//			for(int i=0; i<myDays.size(); i++){
//				System.out.println(myDays.get(i));
//			}
//		} catch (Exception e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		TestDataChase testData = new TestDataChase();
		JSONObject jsonObject = new JSONObject();
		jsonObject.put("province", "北京市");
		jsonObject.put("start_time", "2014-10-24 00:00:00");
		jsonObject.put("end_time", "2014-10-24 21:00:00");
		JSONArray jsonarray = new JSONArray();
		jsonarray.add(0,"CMCC_CMRI_DEPT_CESHI_YEWUSHI");
		jsonObject.put("orgs", jsonarray);
		JSONArray jsonarray1 = new JSONArray();
//		jsonarray1.add(0,"signal_strength_3G"); //3G制式的为-85标准
		jsonarray1.add(0,"02001");//GSM制式的为-90标准  无省份的过滤才能通过
		
		jsonObject.put("test_types", jsonarray1);
		try {
			List list = testData.getTestDataByLocation(jsonObject);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	*/
	/**
	 * GPS坐标转换
	 * @param meta
	 * @return
	 */
	private static String[] transGpsPoint(String meta){

		String[] result = new String[2];
		String[] info = null;
		if(meta!=null && !"".equals(meta)){
			info = meta.split(" ");
		}else{
			return null;
		}
		
		String latitude = "";
		String longitude = "";
		
		String gpsPoint = "";
		
		for(int i=0; i<info.length&&i<2; i++){
			if(info[i].contains("°")){
				String degrees = info[i].substring(0,info[i].lastIndexOf("°"));
				String minutes = info[i].substring(info[i].lastIndexOf("°")+1,info[i].lastIndexOf("′"));
				String seconds = info[i].substring(info[i].lastIndexOf("′")+1,info[i].lastIndexOf("″"));
				
				//Long gpsLong = Long.parseLong(degrees)+Long.parseLong(minutes)/60+Long.parseLong(seconds)/3600;
				float gpsLong = Float.parseFloat(degrees)+Float.parseFloat(minutes)/60+Float.parseFloat(seconds)/3600;
				
				DecimalFormat decimalFormat=new DecimalFormat(".0000000");
				gpsPoint = decimalFormat.format(gpsLong);
				if(info[i].contains("E")){
					longitude = gpsPoint;
				}else if(info[i].contains("N")){
					latitude = gpsPoint;
				}else if(gpsLong>80.0){
					longitude = gpsPoint;
				}else{
					latitude = gpsPoint;
				}
			}else{
				if(info[i].contains("E")){
					gpsPoint = info[i].substring(0,info[i].lastIndexOf("E"));
					float gpsLong = Float.valueOf(gpsPoint);
					DecimalFormat decimalFormat=new DecimalFormat(".0000000");
					gpsPoint = decimalFormat.format(gpsLong);
					longitude = gpsPoint;
				}else if(info[i].contains("N")){
					gpsPoint = info[i].substring(0,info[i].lastIndexOf("N"));
					float gpsLong = Float.valueOf(gpsPoint);
					DecimalFormat decimalFormat=new DecimalFormat(".0000000");
					gpsPoint = decimalFormat.format(gpsLong);
					latitude = gpsPoint;
				}else{
					gpsPoint = info[i];
					float gpsLong = Float.valueOf(gpsPoint);
					DecimalFormat decimalFormat=new DecimalFormat(".0000000");
					gpsPoint = decimalFormat.format(gpsLong);
					if(gpsLong>80.0){
						longitude = gpsPoint;
					}else{
						latitude = gpsPoint;
					}
				}
			}
		}
		result[0] = longitude;
		result[1] = latitude;
		return result;
	}
	public List<Map<String, String>> getTestDataByLocation(JSONObject jsonObject) throws Exception{
		String province = "";
		String city = "";
		String district = "";
		String startTime = "";
		String endTime = "";
		JSONArray orgs = null;
		JSONArray testTypes = null;
		
			province = (String)jsonObject.get("province");
			city = (String)jsonObject.get("city");
			district = (String)jsonObject.get("district");
			startTime = (String)jsonObject.get("start_time");
			endTime = (String)jsonObject.get("end_time");
			orgs = jsonObject.getJSONArray("orgs");
			testTypes = jsonObject.getJSONArray("test_types");
		if(province!=null && !"".equals(province)){
				SimpleDateFormat dateFormat = new SimpleDateFormat(
						"yyyy-MM-dd HH:mm:ss");
				Date startDate = null;
				Date endDate = null;
				try {
					startDate = dateFormat.parse(startTime);
					endDate = dateFormat.parse(endTime);
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				return this.getTestDataByLocationProvince1(province, orgs, testTypes, startDate, endDate);
		}
		return null;
	}
	public List<Map<String, String>> getTestDataByLocationProvince1(String province, JSONArray orgs, JSONArray testTypes, Date startDate, Date endDate) throws Exception{
		List<ArrayList<QueryExpression>> qes = new ArrayList<ArrayList<QueryExpression>>();
		long startTime = startDate.getTime();
		long endTime = endDate.getTime();
		SimpleDateFormat dateFormat = new SimpleDateFormat(
				"yyyyMMdd");
		String startStr = dateFormat.format(startDate);
		String endStr = dateFormat.format(endDate);
		
		List<String> days = this.parseDays(startDate, endDate);
		if(days.size()==1){
			ArrayList<QueryExpression> qe = new ArrayList<QueryExpression>();
			QueryExpression myQEDay = new QueryExpression("year_month_day",IndexOperator.EQ,startStr);
			QueryExpression myQETime = new QueryExpression("data_time",IndexOperator.GTE,""+startTime);
//			QueryExpression myQEProvince = new QueryExpression("cassandra_province",IndexOperator.EQ,""+province);
			qe.add(myQEDay);
			qe.add(myQETime);
//			qe.add(myQEProvince);
			qes.add(qe);
		}else if(days.size()==2){
			ArrayList<QueryExpression> qe = new ArrayList<QueryExpression>();
			QueryExpression myQEDay = new QueryExpression("year_month_day",IndexOperator.EQ,startStr);
			QueryExpression myQETime = new QueryExpression("data_time",IndexOperator.GTE,""+startTime);
			QueryExpression myQEProvince = new QueryExpression("cassandra_province",IndexOperator.EQ,""+province);
			qe.add(myQEDay);
			qe.add(myQETime);
			qe.add(myQEProvince);
			qes.add(qe);
			
			ArrayList<QueryExpression> qe1 = new ArrayList<QueryExpression>();
			QueryExpression myQEDay1 = new QueryExpression("year_month_day",IndexOperator.EQ,endStr);
			QueryExpression myQETime1 = new QueryExpression("data_time",IndexOperator.LTE,""+endTime);
			QueryExpression myQEProvince1 = new QueryExpression("cassandra_province",IndexOperator.EQ,""+province);
			qe1.add(myQEDay1);
			qe1.add(myQETime1);
			qe1.add(myQEProvince1);
			qes.add(qe1);
		}else{
			for(int i=0; i<days.size(); i++){
				if(days.get(i).equals(startStr)){
					ArrayList<QueryExpression> qe = new ArrayList<QueryExpression>();
					QueryExpression myQEDay = new QueryExpression("year_month_day",IndexOperator.EQ,startStr);
					QueryExpression myQETime = new QueryExpression("data_time",IndexOperator.GTE,""+startTime);
					QueryExpression myQEProvince = new QueryExpression("cassandra_province",IndexOperator.EQ,""+province);
					qe.add(myQEDay);
					qe.add(myQETime);
					qe.add(myQEProvince);
					qes.add(qe);
				}else if(days.get(i).equals(endStr)){
					ArrayList<QueryExpression> qe = new ArrayList<QueryExpression>();
					QueryExpression myQEDay = new QueryExpression("year_month_day",IndexOperator.EQ,endStr);
					QueryExpression myQETime = new QueryExpression("data_time",IndexOperator.LTE,""+endTime);
					QueryExpression myQEProvince = new QueryExpression("cassandra_province",IndexOperator.EQ,""+province);
					qe.add(myQEDay);
					qe.add(myQETime);
					qe.add(myQEProvince);
					qes.add(qe);
				}else{
					long timeTemp = (dateFormat.parse(days.get(i))).getTime();
					ArrayList<QueryExpression> qe = new ArrayList<QueryExpression>();
					QueryExpression myQEDay = new QueryExpression("year_month_day",IndexOperator.EQ,days.get(i));
					QueryExpression myQETime = new QueryExpression("data_time",IndexOperator.GTE,""+timeTemp);
					QueryExpression myQEProvince = new QueryExpression("cassandra_province",IndexOperator.EQ,""+province);
					qe.add(myQEDay);
					qe.add(myQETime);
					qe.add(myQEProvince);
					qes.add(qe);
				}
			}
		}
		return getNetWorkData(orgs,testTypes,qes,-1);
	}
	// 网页浏览 统计网站地址 加载次数 平均时延
	public static List<Map<String, String>> getNetWorkData(JSONArray keyspaces, JSONArray columnFamilies, List<ArrayList<QueryExpression>> qes, int count){
		List<Map<String, String>> result = new ArrayList<Map<String, String>> ();
		int defaultStep = 200;
		int total = 0;
		boolean limit = false;
		if(count>-1){
			limit = true;
			if(defaultStep>=count){
				defaultStep = count;
			}
		}
		Map netLocationiMap = new HashMap();
		int netLocationCount = 0;
		for(int x=0; x<qes.size(); x++){
			List<QueryExpression> qe = qes.get(x);
			for(int i=0; i<keyspaces.size(); i++){
				//EQ代表“=”，GTE代表“>=”，GT代表“>”，LTE代表“<=”，LT代表“<”
				if((String)keyspaces.get(i)==null || "".equals((String)keyspaces.get(i))){
					continue;
				}
				Keyspace keyspaceOperator = null;
				try{
					keyspaceOperator = HFactory.createKeyspace((String)keyspaces.get(i),
							KeySpaceFactory.cluster);
				}catch (Exception e){
					e.printStackTrace();
					continue;
				}
				
				for(int j=0; j<columnFamilies.size(); j++){
					ColumnFamilyTemplate<String, String> columnFamilyTemplate = new ThriftColumnFamilyTemplate<String, String>(
							keyspaceOperator, (String)columnFamilies.get(j), KeySpaceFactory.stringSerializer, KeySpaceFactory.stringSerializer);
					IndexedSlicesPredicate<String, String, String> dataPredicate = new IndexedSlicesPredicate<String, String, String>(
							KeySpaceFactory.stringSerializer, KeySpaceFactory.stringSerializer, KeySpaceFactory.stringSerializer);
					
					for(int k=0; k<qe.size(); k++){
						QueryExpression myQueryExpression = qe.get(k);
						if(myQueryExpression!=null){
							if(myQueryExpression.getColumnName()!=null && !"".equals(myQueryExpression.getColumnName()) && myQueryExpression.getOp()!=null && myQueryExpression.getValue()!=null && !"".equals(myQueryExpression.getValue())){
								dataPredicate.addExpression(myQueryExpression.getColumnName(), myQueryExpression.getOp(), myQueryExpression.getValue());
							}
						}
					}

					int stepCount = defaultStep;
					String lastKey = "";
					
					while(stepCount>=(defaultStep-1) && ((limit==true && total<count)||limit==false)){
						stepCount = 0;
						dataPredicate.count(defaultStep).startKey(lastKey);
						ColumnFamilyResult<String, String> list = null;
						try{
							list = columnFamilyTemplate
									.queryColumns(dataPredicate);
						}catch (Exception e){
							e.printStackTrace();
							continue;
						}
						while (list.hasResults()) {
							if(lastKey.equals(list.getKey())){
								continue;
							}
							lastKey = list.getKey();
							stepCount++;
							total++;
							if(total==count){
								break;
							}
							Map<String, String> myData = new HashMap<String, String>();
							Collection<String> element = list.getColumnNames();
							Object[] columns = element.toArray();
							String netLocation = ""; //测试地址
							String net_delay = "";
							int netCount = 0;
							for(int r=0; r<columns.length; r++){
								myData.put((String)columns[r], list.getString(""+columns[r]));
								result.add(myData);
								if(columns[r].toString().contains("时长")||columns[r].toString().contains("时延")){
									System.out.println(list.getString(columns[r].toString()));
								}
//								System.out.println(""+columns[r]+" : "+list.getString(""+columns[r]));
							}
							if(myData.containsKey("网址")||myData.containsKey("地址")){
								netLocation = myData.get("网址");
								if(netLocation==null || netLocation.trim().equals("")){
									netLocation = myData.get("地址");
									if(netLocation!=null && !netLocation.trim().equals("")){
										if(netLocationiMap.containsKey(netLocation)){
											netLocationiMap.put(netLocation, Integer.parseInt((String)netLocationiMap.get(netLocation))+1);
										}else{
											netLocationiMap.put(netLocation, 1);
										}
									}
								}else{
									if(netLocationiMap.containsKey(netLocation)){
										netLocationiMap.put(netLocation, (Integer.parseInt(netLocationiMap.get(netLocation)+"")+1));
									}else{
										netLocationiMap.put(netLocation, 1);
									}
								}
								if(myData.containsKey("次数")){
									try {
										netCount = Integer.parseInt(myData.get("次数"));
										netLocationiMap.put(netLocation+"_count", netCount);
									} catch (Exception e) {
									}
								}
								if(myData.containsKey("平均时延") || myData.containsKey("平均时长(ms)") || myData.containsKey("超时时长") || myData.containsKey("时长(ms)")){
									net_delay = myData.get("平均时延");
									if(net_delay!=null && net_delay.trim().equals("")){
										net_delay = myData.get("平均时长(ms)");
										if(net_delay!=null && net_delay.trim().equals("")){
											netLocationiMap.put(netLocation+"_delay"+netCount, "");
										}else{
											net_delay = myData.get("超时时长");
											if(net_delay!=null && net_delay.trim().equals("")){
												netLocationiMap.put(netLocation+"_delay"+netCount, "");
											}else{
												net_delay = myData.get("时长(ms)");
												if(net_delay!=null && net_delay.trim().equals("")){
													netLocationiMap.put(netLocation+"_delay"+netCount, "");
												}else{
													netLocationiMap.put(netLocation+"_delay"+netCount, "0");
												}
											}
										}
									}
								}
							}
							if(list.hasNext()){
								list.next();
							}else{
								break;
							}
						}
					}

				}
			}
		}
		Set set = netLocationiMap.keySet();
		Iterator iter = set.iterator();
		while(iter.hasNext()){
			String name = (String)iter.next();
			System.out.println(name+","+netLocationiMap.get(name));
		}
		
		System.out.println("总处理数据量"+total);
		return result;
	}
	
	/**
	 public List<Map<String, String>> getTestDataByLocation(){
		String province = "";
		String city = "";
		String district = "";
		if(requestBody!=null){
			province = (String)requestBody.get("province");
			city = (String)requestBody.get("city");
			district = (String)requestBody.get("district");
		}
		if(province!=null && !"".equals(province)){
			if(city!=null && !"".equals(city)){
				if(district!=null && !"".equals(district)){
					return this.getTestDataByLocationDistrict();
				}else{
					return this.getTestDataByLocationCity();
				}
			}else{
				return this.getTestDataByLocationProvince();
			}
		}
		return null;
	}
	public List<Map<String, String>> getTestDataByLocationProvince(){
		
		return null;
	}
	public List<Map<String, String>> getTestDataByLocationCity(){
		return null;
	}
	public List<Map<String, String>> getTestDataByLocationDistrict(){
		return null;
	}
	 */
	
}
