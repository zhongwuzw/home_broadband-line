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

import org.apache.cassandra.thrift.IndexOperator;

import com.opencassandra.v01.dao.factory.KeySpaceFactory;
import com.opencassandra.v01.dao.factory.QueryExpression;

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

public class TestDataChase {
	JSONObject requestBody = null;
	
	public TestDataChase(JSONObject requestBody){
		this.requestBody = requestBody;
	} 
	public TestDataChase(){
	} 
//	public static void main(String[] args) {
//		org.json.simple.JSONObject json = new org.json.simple.JSONObject();
//		json.put("org", "CMCC_SUZHOU_PINGZHICESHE");
//		json.put("last_key", "");
//		json.put("type", "05001");
//		TestDataChase tdc = new TestDataChase();
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
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//	}
	public Map<String, Object> getTestDataByOrg() throws Exception{
		String startTime = "";
		String endTime = "";
		String type = "";
		String lastKey = "";
		String org = null;
		
		Map<String,Object> result = new HashMap<String,Object>();
		if(requestBody!=null){
			startTime = (String)requestBody.get("start_time");
			endTime = (String)requestBody.get("end_time");
			org = (String)requestBody.get("org");
			type = (String)requestBody.get("type");
			lastKey = (String)requestBody.get("last_key");
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
					result = this.getTestDataByOrgSpeedtest(type, lastKey, org, startDate, endDate);
				}else if(type.equals("02001")){//网页浏览
					result = this.getTestDataByOrg(type, lastKey, org, startDate, endDate);
				}else if(type.equals("03001")){//HTTP文件上传或者下载
					result = this.getTestDataByHttp(type, lastKey, org, startDate, endDate);
				}else if(type.equals("04002")){//PING
					result = this.getTestDataByPING(type, lastKey, org, startDate, endDate);
				}else if(type.equals("02011")){//视频
					result = this.getTestDataByVideoTest(type, lastKey, org, startDate, endDate);
				}else if(type.equals("04006")){//切换时延
					result = this.getTestDataByResideTest(type, lastKey, org, startDate, endDate);
				}else if(type.equals("05001")){//话音
					result = this.getTestDataByDial(type, lastKey, org, startDate, endDate);
				}
				result.put("status", "获取数据成功");
			} catch (Exception e) {
				result.put("status", "本次查询为空");
				e.printStackTrace();
			}
			return result;
			
		}
		
		return null;
	}
	
	/**
	 * 网页浏览02001
	 * @author Ocean
	 * @param province
	 * @param city
	 * @param district
	 * @param orgs
	 * @param testTypes
	 */
	public Map<String, Object> getTestDataByOrg(String type, String startKey, String orgs, Date startDate, Date endDate) throws Exception{
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
		rangeQuery.setColumnNames("GPS位置","LAC/CID","TAC/PCI","网络类型","信号强度","测试时间","cassandra_province","cassandra_city","cassandra_district","cassandra_street","cassandra_street_number","平均速率(Kbps)","平均时长(ms)","时长(ms)","imei","fileName","fileSize","filePath","file_lastModifies");
		QueryResult<OrderedRows<String, String, String>> result = rangeQuery
				.execute();
		OrderedRows<String, String, String> oRows = result.get();
		int count = 1;
		String lastKey = "";
		List<Row<String, String, String>> rowList = oRows.getList();
		for (Row<String, String, String> row : rowList) {
			if(startKey.equals(row.getKey())){
				continue;
			}
			HashMap<String, String> myData = new HashMap<String, String>();
			
			List<HColumn<String, String>> columns = row.getColumnSlice()
					.getColumns();
			String speed = "";
			String term = "";
			String term1 = "";

			String gpsLocation = "";
			String lac = "";
			String cid = "";
			String tac = "";
			String pci = "";
			String netType = "";
			String signal = "";
			String sinr = "";
			String testTime = "";
			
			String province = "";
			String city = "";
			String district = "";
			String street = "";
			String street_no = "";
			
			String fileName = "";
			String filePath = "";
			String fileSize = "";
			String fileLastModifies = "";
			for (HColumn<String, String> hColumn : columns) {
				if("平均速率(Kbps)".equals(hColumn.getName())){
					speed = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("平均时长(ms)".equals(hColumn.getName())){
					term = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("时长(ms)".equals(hColumn.getName())){
					term1 = hColumn.getValue()==null?"":hColumn.getValue();
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
				}else if("LAC/CID".equals(hColumn.getName())){
					String lacCid = hColumn.getValue()==null?"":hColumn.getValue();
					if(lacCid.indexOf("/")!=-1 || lacCid.indexOf("N/A")!=-1){
						String[] cellInfo = lacCid.split("/");
						lac = cellInfo[0];
						cid = cellInfo[1];
					}
				}else if("TAC/PCI".equals(hColumn.getName())){
					String tacPci = hColumn.getValue()==null?"":hColumn.getValue();
					if(tacPci.indexOf("/")!=-1 || tacPci.indexOf("N/A")!=-1){
						String[] cellInfo = tacPci.split("/");
						tac = cellInfo[0];
						pci = cellInfo[1];
					}
				}else if("GPS位置".equals(hColumn.getName())){
					gpsLocation = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("网络类型".equals(hColumn.getName())){
					netType = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("信号强度".equals(hColumn.getName())){
					signal = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("测试时间".equals(hColumn.getName())){
					testTime = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("SINR".equals(hColumn.getName())){
					sinr = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("fileName".equals(hColumn.getName())){
					fileName = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("filePath".equals(hColumn.getName())){
					filePath = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("fileSize".equals(hColumn.getName())){
					fileSize = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("file_lastModifies".equals(hColumn.getName())){
					fileLastModifies = hColumn.getValue()==null?"":hColumn.getValue();
				}else{
					myData.put(hColumn.getName(), hColumn.getValue());
				}
			}

			if(term!=null && !term.equals("") && speed!=null && !speed.equals("")){
				myData.put("loadTime", term);
				myData.put("loadSpeed", speed);
				lastKey = row.getKey();
			}else if(term1!=null && !term1.equals("") && speed!=null && !speed.equals("")){
				myData.put("loadTime", term1);
				myData.put("loadSpeed", speed);
				lastKey = row.getKey();
			}else{
				continue;
			}

			myData.put("reportLocation", province+city+district+street+street_no);
			myData.put("GSMLAC", lac);
			myData.put("GSMCID", cid);
			myData.put("LTETAC", tac);
			myData.put("LTEPCI", pci);
			if(netType.indexOf("LTE")!=-1){
				myData.put("LTESignal", signal);
			}else{
				myData.put("GSMSignal", signal);
			}
			myData.put("LTESINR", sinr);
			myData.put("testTime", testTime);
			myData.put("GPSLocation", gpsLocation);
			detail.add(myData);
			if(count>=20){
				break;
			}
			count++;
		}
		respResult.put("total",count);
		respResult.put("detail",detail);
		respResult.put("last_key",lastKey);
		return respResult;
	}
	
	/**
	 * speedTest01001 测速
	 * @author Ocean
	 * @param province
	 * @param city
	 * @param district
	 * @param orgs
	 * @param testTypes
	 */
	public Map<String, Object> getTestDataByOrgSpeedtest(String type, String startKey, String orgs, Date startDate, Date endDate) throws Exception{
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
		rangeQuery.setColumnNames("GPS位置","LAC/CID","TAC/PCI","网络类型","信号强度","测试时间","cassandra_province","cassandra_city","cassandra_district","cassandra_street","cassandra_street_number","时长(ms)","imei","下行速率(Mbps)","上行速率(Mbps)","上行速率","下行速率","时延","时延(ms)","fileName","fileSize","filePath","file_lastModifies");
		QueryResult<OrderedRows<String, String, String>> result = rangeQuery
				.execute();
		OrderedRows<String, String, String> oRows = result.get();
		int count = 1;
		String lastKey = "";
		List<Row<String, String, String>> rowList = oRows.getList();
		for (Row<String, String, String> row : rowList) {
			if(startKey.equals(row.getKey())){
				continue;
			}
			HashMap<String, String> myData = new HashMap<String, String>();
			
			List<HColumn<String, String>> columns = row.getColumnSlice()
					.getColumns();
			String speed = "";
			String term = "";
			String term1 = "";

			String gpsLocation = "";
			String lac = "";
			String cid = "";
			String tac = "";
			String pci = "";
			String netType = "";
			String signal = "";
			String sinr = "";
			String testTime = "";
			
			String topSpeed = "";//上行速率
			String downSpeed = "";//下行速率
			String delay = "";//时延
			
			String province = "";
			String city = "";
			String district = "";
			String street = "";
			String street_no = "";
			
			String fileName = "";
			String filePath = "";
			String fileSize = "";
			String fileLastModifies = "";
			
			for (HColumn<String, String> hColumn : columns) {
				if("平均速率(Kbps)".equals(hColumn.getName())){
					speed = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("平均时长(ms)".equals(hColumn.getName())){
					term = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("时长(ms)".equals(hColumn.getName())){
					term1 = hColumn.getValue()==null?"":hColumn.getValue();
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
				}else if("LAC/CID".equals(hColumn.getName())){
					String lacCid = hColumn.getValue()==null?"":hColumn.getValue();
					if(lacCid.indexOf("/")!=-1 || lacCid.indexOf("N/A")!=-1){
						String[] cellInfo = lacCid.split("/");
						lac = cellInfo[0];
						cid = cellInfo[1];
					}
				}else if("TAC/PCI".equals(hColumn.getName())){
					String tacPci = hColumn.getValue()==null?"":hColumn.getValue();
					if(tacPci.indexOf("/")!=-1 || tacPci.indexOf("N/A")!=-1){
						String[] cellInfo = tacPci.split("/");
						tac = cellInfo[0];
						pci = cellInfo[1];
					}
				}else if("GPS位置".equals(hColumn.getName())){
					gpsLocation = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("网络类型".equals(hColumn.getName())){
					netType = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("信号强度".equals(hColumn.getName())){
					signal = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("测试时间".equals(hColumn.getName())){
					testTime = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("SINR".equals(hColumn.getName())){
					sinr = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("上行速率".equals(hColumn.getName())||"上行速率(Mbps)".equals(hColumn.getName())){
					topSpeed = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("下行速率".equals(hColumn.getName())||"下行速率(Mbps)".equals(hColumn.getName())){
					downSpeed = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("时延".equals(hColumn.getName())||"时延(ms)".equals(hColumn.getName())){
					delay = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("fileName".equals(hColumn.getName())){
					fileName = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("filePath".equals(hColumn.getName())){
					filePath = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("fileSize".equals(hColumn.getName())){
					fileSize = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("file_lastModifies".equals(hColumn.getName())){
					fileLastModifies = hColumn.getValue()==null?"":hColumn.getValue();
				}else{
					myData.put(hColumn.getName(), hColumn.getValue());
				}
			}

			if(topSpeed!=null && !topSpeed.equals("") && downSpeed!=null && !downSpeed.equals("") && delay!=null && !delay.equals("")){
				myData.put("up_avg_speed", topSpeed);
				myData.put("down_avg_speed", downSpeed);
				myData.put("delay", delay);
				lastKey = row.getKey();
			}else{
				continue;
			}

			myData.put("reportLocation", province+city+district+street+street_no);
			myData.put("GSMLAC", lac);
			myData.put("GSMCID", cid);
			myData.put("LTETAC", tac);
			myData.put("LTEPCI", pci);
			if(netType.indexOf("LTE")!=-1){
				myData.put("LTESignal", signal);
			}else{
				myData.put("GSMSignal", signal);
			}
			myData.put("LTESINR", sinr);
			myData.put("testTime", testTime);
			myData.put("GPSLocation", gpsLocation);
			detail.add(myData);
			if(count>=20){
				break;
			}
			count++;
		}
		respResult.put("total",count);
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
	 * @param testTypes
	 */
	public Map<String, Object> getTestDataByHttp(String type, String startKey, String orgs, Date startDate, Date endDate) throws Exception{
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
		rangeQuery.setColumnNames("GPS位置","LAC/CID","TAC/PCI","网络类型","信号强度","测试时间","cassandra_province","cassandra_city","cassandra_district","cassandra_street","cassandra_street_number","平均速率(Mbps)","平均时长(ms)","时长(ms)","imei","速率","类型","fileName","fileSize","filePath","file_lastModifies");
		QueryResult<OrderedRows<String, String, String>> result = rangeQuery
				.execute();
		OrderedRows<String, String, String> oRows = result.get();
		int count = 1;
		String lastKey = "";
		List<Row<String, String, String>> rowList = oRows.getList();
		for (Row<String, String, String> row : rowList) {
			if(startKey.equals(row.getKey())){
				continue;
			}
			HashMap<String, String> myData = new HashMap<String, String>();
			
			List<HColumn<String, String>> columns = row.getColumnSlice()
					.getColumns();
			String speed = "";

			String gpsLocation = "";
			String lac = "";
			String cid = "";
			String tac = "";
			String pci = "";
			String netType = "";
			String signal = "";
			String sinr = "";
			String testTime = "";
			
			String province = "";
			String city = "";
			String district = "";
			String street = "";
			String street_no = "";
			
			String fileName = "";
			String filePath = "";
			String fileSize = "";
			String fileLastModifies = "";
			for (HColumn<String, String> hColumn : columns) {
				if("平均速率(Mbps)".equals(hColumn.getName())){
					speed = hColumn.getValue()==null?"":hColumn.getValue();
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
				}else if("LAC/CID".equals(hColumn.getName())){
					String lacCid = hColumn.getValue()==null?"":hColumn.getValue();
					if(lacCid.indexOf("/")!=-1 || lacCid.indexOf("N/A")!=-1){
						String[] cellInfo = lacCid.split("/");
						lac = cellInfo[0];
						cid = cellInfo[1];
					}
				}else if("TAC/PCI".equals(hColumn.getName())){
					String tacPci = hColumn.getValue()==null?"":hColumn.getValue();
					if(tacPci.indexOf("/")!=-1 || tacPci.indexOf("N/A")!=-1){
						String[] cellInfo = tacPci.split("/");
						tac = cellInfo[0];
						pci = cellInfo[1];
					}
				}else if("GPS位置".equals(hColumn.getName())){
					gpsLocation = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("网络类型".equals(hColumn.getName())){
					netType = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("信号强度".equals(hColumn.getName())){
					signal = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("测试时间".equals(hColumn.getName())){
					testTime = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("SINR".equals(hColumn.getName())){
					sinr = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("类型".equals(hColumn.getName())){
					
				}else if("fileName".equals(hColumn.getName())){
					fileName = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("filePath".equals(hColumn.getName())){
					filePath = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("fileSize".equals(hColumn.getName())){
					fileSize = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("file_lastModifies".equals(hColumn.getName())){
					fileLastModifies = hColumn.getValue()==null?"":hColumn.getValue();
				}else{
					myData.put(hColumn.getName(), hColumn.getValue());
				}
			}

			if(speed!=null && !speed.equals("")){
				myData.put("speed", speed);
				lastKey = row.getKey();
			}else{
				continue;
			}

			myData.put("reportLocation", province+city+district+street+street_no);
			myData.put("GSMLAC", lac);
			myData.put("GSMCID", cid);
			myData.put("LTETAC", tac);
			myData.put("LTEPCI", pci);
			if(netType.indexOf("LTE")!=-1){
				myData.put("LTESignal", signal);
			}else{
				myData.put("GSMSignal", signal);
			}
			myData.put("LTESINR", sinr);
			myData.put("testTime", testTime);
			myData.put("GPSLocation", gpsLocation);
			detail.add(myData);
			if(count>=20){
				break;
			}
			count++;
		}
		respResult.put("total",count);
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
	 * @param testTypes
	 */
	public Map<String, Object> getTestDataByPING(String type, String startKey, String orgs, Date startDate, Date endDate) throws Exception{
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
		rangeQuery.setColumnNames("GPS位置","LAC/CID","TAC/PCI","网络类型","信号强度","测试时间","cassandra_province","cassandra_city","cassandra_district","cassandra_street","cassandra_street_number","平均速率(Kbps)","平均时长(ms)","时长(ms)","imei","平均时延","平均时延(ms)","fileName","fileSize","filePath","file_lastModifies");
		QueryResult<OrderedRows<String, String, String>> result = rangeQuery
				.execute();
		OrderedRows<String, String, String> oRows = result.get();
		int count = 1;
		String lastKey = "";
		List<Row<String, String, String>> rowList = oRows.getList();
		for (Row<String, String, String> row : rowList) {
			if(startKey.equals(row.getKey())){
				continue;
			}
			HashMap<String, String> myData = new HashMap<String, String>();
			
			List<HColumn<String, String>> columns = row.getColumnSlice()
					.getColumns();

			String gpsLocation = "";
			String lac = "";
			String cid = "";
			String tac = "";
			String pci = "";
			String netType = "";
			String signal = "";
			String sinr = "";
			String testTime = "";
			
			String delay = "";//时延
			
			String province = "";
			String city = "";
			String district = "";
			String street = "";
			String street_no = "";
			
			String fileName = "";
			String filePath = "";
			String fileSize = "";
			String fileLastModifies = "";
			for (HColumn<String, String> hColumn : columns) {
				if("cassandra_province".equals(hColumn.getName())){
					province = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("cassandra_city".equals(hColumn.getName())){
					city = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("cassandra_district".equals(hColumn.getName())){
					district = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("cassandra_street".equals(hColumn.getName())){
					street = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("cassandra_street_no".equals(hColumn.getName())){
					street_no = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("LAC/CID".equals(hColumn.getName())){
					String lacCid = hColumn.getValue()==null?"":hColumn.getValue();
					if(lacCid.indexOf("/")!=-1 || lacCid.indexOf("N/A")!=-1){
						String[] cellInfo = lacCid.split("/");
						lac = cellInfo[0];
						cid = cellInfo[1];
					}
				}else if("TAC/PCI".equals(hColumn.getName())){
					String tacPci = hColumn.getValue()==null?"":hColumn.getValue();
					if(tacPci.indexOf("/")!=-1 || tacPci.indexOf("N/A")!=-1){
						String[] cellInfo = tacPci.split("/");
						tac = cellInfo[0];
						pci = cellInfo[1];
					}
				}else if("GPS位置".equals(hColumn.getName())){
					gpsLocation = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("网络类型".equals(hColumn.getName())){
					netType = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("信号强度".equals(hColumn.getName())){
					signal = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("测试时间".equals(hColumn.getName())){
					testTime = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("SINR".equals(hColumn.getName())){
					sinr = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("平均时延".equals(hColumn.getName()) || "平均时延(ms)".equals(hColumn.getName())){
					delay = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("fileName".equals(hColumn.getName())){
					fileName = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("filePath".equals(hColumn.getName())){
					filePath = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("fileSize".equals(hColumn.getName())){
					fileSize = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("file_lastModifies".equals(hColumn.getName())){
					fileLastModifies = hColumn.getValue()==null?"":hColumn.getValue();
				}else{
					myData.put(hColumn.getName(), hColumn.getValue());
				}
			}

			if(delay!=null && !delay.equals("")){
				myData.put("delay", delay);
				lastKey = row.getKey();
			}else{
				continue;
			}

			myData.put("reportLocation", province+city+district+street+street_no);
			myData.put("GSMLAC", lac);
			myData.put("GSMCID", cid);
			myData.put("LTETAC", tac);
			myData.put("LTEPCI", pci);
			if(netType.indexOf("LTE")!=-1){
				myData.put("LTESignal", signal);
			}else{
				myData.put("GSMSignal", signal);
			}
			myData.put("LTESINR", sinr);
			myData.put("testTime", testTime);
			myData.put("GPSLocation", gpsLocation);
			
			detail.add(myData);
			if(count>=20){
				break;
			}
			count++;
		}
		respResult.put("total",count);
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
	 * @param testTypes
	 */
	public Map<String, Object> getTestDataByVideoTest(String type, String startKey, String orgs, Date startDate, Date endDate) throws Exception{
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
		rangeQuery.setColumnNames("GPS位置","LAC/CID","TAC/PCI","网络类型","信号强度","测试时间","cassandra_province","cassandra_city","cassandra_district","cassandra_street","cassandra_street_number","平均速率(Kbps)","平均时长(ms)","时长(ms)","imei","加载时长(ms)","加载速率(Kbps)","缓冲次数(次)","缓冲总时长(s)","fileName","fileSize","filePath","file_lastModifies");
		QueryResult<OrderedRows<String, String, String>> result = rangeQuery
				.execute();
		OrderedRows<String, String, String> oRows = result.get();
		int count = 1;
		String lastKey = "";
		List<Row<String, String, String>> rowList = oRows.getList();
		for (Row<String, String, String> row : rowList) {
			if(startKey.equals(row.getKey())){
				continue;
			}
			HashMap<String, String> myData = new HashMap<String, String>();
			
			List<HColumn<String, String>> columns = row.getColumnSlice()
					.getColumns();
			String speed = "";

			String gpsLocation = "";
			String lac = "";
			String cid = "";
			String tac = "";
			String pci = "";
			String netType = "";
			String signal = "";
			String sinr = "";
			String testTime = "";
			
			String load_delay = "";//页面加载时延
			String down_max_speed = "";//下行最大速率
			String break_times = "";//中断次数
			String break_sum_delay = "";//缓冲总时长(s)
			String break_avg_delay = "";//平均中断时延
			
			String province = "";
			String city = "";
			String district = "";
			String street = "";
			String street_no = "";
			
			String fileName = "";
			String filePath = "";
			String fileSize = "";
			String fileLastModifies = "";
			for (HColumn<String, String> hColumn : columns) {
				if("平均速率(Kbps)".equals(hColumn.getName())){
					speed = hColumn.getValue()==null?"":hColumn.getValue();
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
				}else if("LAC/CID".equals(hColumn.getName())){
					String lacCid = hColumn.getValue()==null?"":hColumn.getValue();
					if(lacCid.indexOf("/")!=-1 || lacCid.indexOf("N/A")!=-1){
						String[] cellInfo = lacCid.split("/");
						lac = cellInfo[0];
						cid = cellInfo[1];
					}
				}else if("TAC/PCI".equals(hColumn.getName())){
					String tacPci = hColumn.getValue()==null?"":hColumn.getValue();
					if(tacPci.indexOf("/")!=-1 || tacPci.indexOf("N/A")!=-1){
						String[] cellInfo = tacPci.split("/");
						tac = cellInfo[0];
						pci = cellInfo[1];
					}
				}else if("GPS位置".equals(hColumn.getName())){
					gpsLocation = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("网络类型".equals(hColumn.getName())){
					netType = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("信号强度".equals(hColumn.getName())){
					signal = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("测试时间".equals(hColumn.getName())){
					testTime = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("SINR".equals(hColumn.getName())){
					sinr = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("加载时长(ms)".equals(hColumn.getName())){
					load_delay = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("加载速率(Kbps)".equals(hColumn.getName())){
					down_max_speed = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("缓冲次数(次)".equals(hColumn.getName())){
					break_times = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("缓冲总时长(s)".equals(hColumn.getName())){
					break_sum_delay = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("fileName".equals(hColumn.getName())){
					fileName = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("filePath".equals(hColumn.getName())){
					filePath = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("fileSize".equals(hColumn.getName())){
					fileSize = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("file_lastModifies".equals(hColumn.getName())){
					fileLastModifies = hColumn.getValue()==null?"":hColumn.getValue();
				}else{
					myData.put(hColumn.getName(), hColumn.getValue());
				}
			}

			if(load_delay!=null && !load_delay.equals("") && down_max_speed!=null && !load_delay.equals("") && break_times!=null && !load_delay.equals("")){
				myData.put("load_delay", load_delay);
				myData.put("down_max_speed",down_max_speed);
				myData.put("break_times", break_times);
				if(!break_times.equals("0") && break_sum_delay!=null && !break_sum_delay.trim().equals(""))
				{
					break_avg_delay = Double.parseDouble(break_sum_delay)/Integer.parseInt(break_times) + "";
				}else
				{
					break_avg_delay = "";
				}
				myData.put("break_avg_delay",break_avg_delay);
				lastKey = row.getKey();
			}else{
				continue;
			}

			myData.put("reportLocation", province+city+district+street+street_no);
			myData.put("GSMLAC", lac);
			myData.put("GSMCID", cid);
			myData.put("LTETAC", tac);
			myData.put("LTEPCI", pci);
			if(netType.indexOf("LTE")!=-1){
				myData.put("LTESignal", signal);
			}else{
				myData.put("GSMSignal", signal);
			}
			myData.put("LTESINR", sinr);
			myData.put("testTime", testTime);
			myData.put("GPSLocation", gpsLocation);
			myData.put("speed", speed);
			
			detail.add(myData);
			if(count>=20){
				break;
			}
			count++;
		}
		respResult.put("total",count);
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
	 * @param testTypes
	 */
	public Map<String, Object> getTestDataByResideTest(String type, String startKey, String orgs, Date startDate, Date endDate) throws Exception{
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
		rangeQuery.setColumnNames("GPS位置","LAC/CID","TAC/PCI","网络类型","信号强度","测试时间","cassandra_province","cassandra_city","cassandra_district","cassandra_street","cassandra_street_number","平均速率(Kbps)","平均时长(ms)","时长(ms)","imei","切换重选时延","切换时延(s)","fileName","fileSize","filePath","file_lastModifies");
		QueryResult<OrderedRows<String, String, String>> result = rangeQuery
				.execute();
		OrderedRows<String, String, String> oRows = result.get();
		int count = 1;
		String lastKey = "";
		List<Row<String, String, String>> rowList = oRows.getList();
		for (Row<String, String, String> row : rowList) {
			if(startKey.equals(row.getKey())){
				continue;
			}
			HashMap<String, String> myData = new HashMap<String, String>();
			
			List<HColumn<String, String>> columns = row.getColumnSlice()
					.getColumns();
			String gpsLocation = "";
			String lac = "";
			String cid = "";
			String tac = "";
			String pci = "";
			String netType = "";
			String signal = "";
			String sinr = "";
			String testTime = "";
			
			String delay = "";//时延
			
			String province = "";
			String city = "";
			String district = "";
			String street = "";
			String street_no = "";
			
			String fileName = "";
			String filePath = "";
			String fileSize = "";
			String fileLastModifies = "";
			for (HColumn<String, String> hColumn : columns) {
				if("cassandra_province".equals(hColumn.getName())){
					province = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("cassandra_city".equals(hColumn.getName())){
					city = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("cassandra_district".equals(hColumn.getName())){
					district = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("cassandra_street".equals(hColumn.getName())){
					street = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("cassandra_street_no".equals(hColumn.getName())){
					street_no = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("LAC/CID".equals(hColumn.getName())){
					String lacCid = hColumn.getValue()==null?"":hColumn.getValue();
					if(lacCid.indexOf("/")!=-1 || lacCid.indexOf("N/A")!=-1){
						String[] cellInfo = lacCid.split("/");
						lac = cellInfo[0];
						cid = cellInfo[1];
					}
				}else if("TAC/PCI".equals(hColumn.getName())){
					String tacPci = hColumn.getValue()==null?"":hColumn.getValue();
					if(tacPci.indexOf("/")!=-1 || tacPci.indexOf("N/A")!=-1){
						String[] cellInfo = tacPci.split("/");
						tac = cellInfo[0];
						pci = cellInfo[1];
					}
				}else if("GPS位置".equals(hColumn.getName())){
					gpsLocation = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("网络类型".equals(hColumn.getName())){
					netType = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("信号强度".equals(hColumn.getName())){
					signal = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("测试时间".equals(hColumn.getName())){
					testTime = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("SINR".equals(hColumn.getName())){
					sinr = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("切换时延(s)".equals(hColumn.getName()) || "切换重选时延".equals(hColumn.getName())){
					delay = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("fileName".equals(hColumn.getName())){
					fileName = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("filePath".equals(hColumn.getName())){
					filePath = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("fileSize".equals(hColumn.getName())){
					fileSize = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("file_lastModifies".equals(hColumn.getName())){
					fileLastModifies = hColumn.getValue()==null?"":hColumn.getValue();
				}else{
					myData.put(hColumn.getName(), hColumn.getValue());
				}
			}

			if(delay!=null && !delay.equals("")){
				myData.put("delay", delay);
				lastKey = row.getKey();
			}else{
				continue;
			}

			myData.put("reportLocation", province+city+district+street+street_no);
			myData.put("GSMLAC", lac);
			myData.put("GSMCID", cid);
			myData.put("LTETAC", tac);
			myData.put("LTEPCI", pci);
			if(netType.indexOf("LTE")!=-1){
				myData.put("LTESignal", signal);
			}else{
				myData.put("GSMSignal", signal);
			}
			myData.put("LTESINR", sinr);
			myData.put("testTime", testTime);
			myData.put("GPSLocation", gpsLocation);
			
			detail.add(myData);
			if(count>=20){
				break;
			}
			count++;
		}
		respResult.put("total",count);
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
	 * @param testTypes
	 */
	public Map<String, Object> getTestDataByDial(String type, String startKey, String orgs, Date startDate, Date endDate) throws Exception{
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
		rangeQuery.setColumnNames("GPS位置","LAC/CID","TAC/PCI","网络类型","信号强度","测试时间","cassandra_province","cassandra_city","cassandra_district","cassandra_street","cassandra_street_number","平均速率(Kbps)","平均时长(ms)","时长(ms)","imei","接续时长(ms)","fallbackToCs(ms)","backToLte(ms)","fileName","fileSize","filePath","file_lastModifies");
		QueryResult<OrderedRows<String, String, String>> result = rangeQuery
				.execute();
		OrderedRows<String, String, String> oRows = result.get();
		int count = 1;
		String lastKey = "";
		List<Row<String, String, String>> rowList = oRows.getList();
		for (Row<String, String, String> row : rowList) {
			if(startKey.equals(row.getKey())){
				continue;
			}
			
			HashMap<String, String> myData = new HashMap<String, String>();
			
			List<HColumn<String, String>> columns = row.getColumnSlice()
					.getColumns();
			String gpsLocation = "";
			String lac = "";
			String cid = "";
			String tac = "";
			String pci = "";
			String netType = "";
			String signal = "";
			String sinr = "";
			String testTime = "";
			
			String follow_delay = "";
			String csfb_fall_delay = "";
			String csfb_return_delay = "";

			String province = "";
			String city = "";
			String district = "";
			String street = "";
			String street_no = "";
			
			String fileName = "";
			String filePath = "";
			String fileSize = "";
			String fileLastModifies = "";
			for (HColumn<String, String> hColumn : columns) {
				if("cassandra_province".equals(hColumn.getName())){
					province = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("cassandra_city".equals(hColumn.getName())){
					city = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("cassandra_district".equals(hColumn.getName())){
					district = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("cassandra_street".equals(hColumn.getName())){
					street = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("cassandra_street_no".equals(hColumn.getName())){
					street_no = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("LAC/CID".equals(hColumn.getName())){
					String lacCid = hColumn.getValue()==null?"":hColumn.getValue();
					if(lacCid.indexOf("/")!=-1 || lacCid.indexOf("N/A")!=-1){
						String[] cellInfo = lacCid.split("/");
						lac = cellInfo[0];
						cid = cellInfo[1];
					}
				}else if("TAC/PCI".equals(hColumn.getName())){
					String tacPci = hColumn.getValue()==null?"":hColumn.getValue();
					if(tacPci.indexOf("/")!=-1 || tacPci.indexOf("N/A")!=-1){
						String[] cellInfo = tacPci.split("/");
						tac = cellInfo[0];
						pci = cellInfo[1];
					}
				}else if("GPS位置".equals(hColumn.getName())){
					gpsLocation = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("网络类型".equals(hColumn.getName())){
					netType = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("信号强度".equals(hColumn.getName())){
					signal = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("测试时间".equals(hColumn.getName())){
					testTime = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("SINR".equals(hColumn.getName())){
					sinr = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("接续时长(ms)".equals(hColumn.getName())){
					follow_delay = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("fallbackToCs(ms)".equals(hColumn.getName())){
					csfb_fall_delay = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("backToLte(ms)".equals(hColumn.getName())){
					csfb_return_delay = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("fileName".equals(hColumn.getName())){
					fileName = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("filePath".equals(hColumn.getName())){
					filePath = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("fileSize".equals(hColumn.getName())){
					fileSize = hColumn.getValue()==null?"":hColumn.getValue();
				}else if("file_lastModifies".equals(hColumn.getName())){
					fileLastModifies = hColumn.getValue()==null?"":hColumn.getValue();
				}else{
					myData.put(hColumn.getName(), hColumn.getValue());
				}
			}

			if(follow_delay!=null && !follow_delay.equals("") && csfb_fall_delay!=null && !csfb_fall_delay.equals("") && csfb_return_delay!=null && !csfb_return_delay.equals("")){
				myData.put("follow_delay", follow_delay);
				myData.put("csfb_fall_delay", csfb_fall_delay);
				myData.put("cfsb_return_delay", csfb_return_delay);
				lastKey = row.getKey();
			}else{
				continue;
			}

			myData.put("reportLocation", province+city+district+street+street_no);
			myData.put("GSMLAC", lac);
			myData.put("GSMCID", cid);
			myData.put("LTETAC", tac);
			myData.put("LTEPCI", pci);
			if(netType.indexOf("LTE")!=-1){
				myData.put("LTESignal", signal);
			}else{
				myData.put("GSMSignal", signal);
			}
			myData.put("LTESINR", sinr);
			myData.put("testTime", testTime);
			myData.put("GPSLocation", gpsLocation);
			detail.add(myData);
			if(count>=20){
				break;
			}
			count++;
		}
		respResult.put("total",count);
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
