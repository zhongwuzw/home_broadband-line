package com.opencassandra.v01.dao.impl;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
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
import java.util.regex.Pattern;

import me.prettyprint.cassandra.service.template.ColumnFamilyResult;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.IndexedSlicesPredicate;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.RangeSlicesQuery;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.apache.cassandra.thrift.IndexOperator;
import org.apache.log4j.Logger;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;
import com.opencassandra.descfile.ConfParser;
import com.opencassandra.v01.dao.factory.KeySpaceFactory;
import com.opencassandra.v01.dao.factory.QueryExpression;

public class TestDataAnalyze {
	JSONObject requestBody = null;
	static Logger logger = Logger.getLogger(TestDataAnalyze.class);
	private String[] dateStr = new String[] { "yyyy", "MM", "dd", "HH", "mm", "ss", "SSS" };
	public TestDataAnalyze(JSONObject requestBody){
		this.requestBody = requestBody;
	} 
	public TestDataAnalyze(){
	} 
	public List<Map<String, String>> getTestDataByLocation() throws Exception{
		String province = "";
		String city = "";
		String district = "";
		String startTime = "";
		String endTime = "";
		JSONArray orgs = null;
		JSONArray testTypes = null;
		
		if(requestBody!=null){
			province = (String)requestBody.get("province");
			city = (String)requestBody.get("city");
			district = (String)requestBody.get("district");
			startTime = (String)requestBody.get("start_time");
			endTime = (String)requestBody.get("end_time");
			orgs = requestBody.getJSONArray("orgs");
			testTypes = requestBody.getJSONArray("test_types");
		}
		if(province!=null && !"".equals(province)){
			if(city!=null && !"".equals(city)){
				if(district!=null && !"".equals(district)){
					return this.getTestDataByLocationDistrict(province, city, district, orgs, testTypes);
				}else{
					return this.getTestDataByLocationCity(province, city, district, orgs, testTypes);
				}
			}else{
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
				return this.getTestDataByLocationProvince(province, orgs, testTypes, startDate, endDate);
			}
		}
		return null;
	}
	
	public Map<String, Object> getTestDataByOrg() throws Exception{
		String startTime = "";
		String endTime = "";
		String type = "";
		String lastKey = "";
		String org = null;
		
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
			return this.getTestDataByOrg(type, lastKey, org, startDate, endDate);
		}
		
		return null;
	}
	
	public List<Map<String, String>> getGpsDataByLocation() throws Exception{
		String province = "";
		String city = "";
		String district = "";
		String startTime = "";
		String endTime = "";
		JSONArray orgs = null;
		JSONArray testTypes = null;
		
		if(requestBody!=null){
			province = (String)requestBody.get("province");
			city = (String)requestBody.get("city");
			district = (String)requestBody.get("district");
			startTime = (String)requestBody.get("start_time");
			endTime = (String)requestBody.get("end_time");
			orgs = requestBody.getJSONArray("orgs");
			testTypes = requestBody.getJSONArray("test_types");
		}
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
		
		if(province!=null && !"".equals(province)){
			if(city!=null && !"".equals(city)){
				if(district!=null && !"".equals(district)){
					return this.getGpsDataByLocationDistrict(province, city, district, orgs, testTypes, startDate, endDate);
				}else{
					return this.getGpsDataByLocationCity(province, city, orgs, testTypes, startDate, endDate);
				}
			}else{
				return this.getGpsDataByLocationProvince(province, orgs, testTypes, startDate, endDate);
			}
		}
		return null;
	}
	

	public Map<String, Object> getReportDataByLocation() throws Exception{
		String province = "";
		String city = "";
		String district = "";
		String startTime = "";
		String endTime = "";
		JSONArray orgs = null;
		JSONArray testTypes = null;
		
		if(requestBody!=null){
			province = (String)requestBody.get("province");
			city = (String)requestBody.get("city");
			district = (String)requestBody.get("district");
			startTime = (String)requestBody.get("start_time");
			endTime = (String)requestBody.get("end_time");
			orgs = requestBody.getJSONArray("orgs");
			testTypes = requestBody.getJSONArray("test_types");
		}
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
		
		if(province!=null && !"".equals(province)){
			if(city!=null && !"".equals(city)){
				if(district!=null && !"".equals(district)){
					return this.getReportDataByLocationDistrict("total_report", province, city, district, orgs, testTypes, startDate, endDate);
				}else{
					return this.getReportDataByLocationCity("total_report", province, city, orgs, testTypes, startDate, endDate);
				}
			}else{
				return this.getReportDataByLocationProvince("total_report", province, orgs, testTypes, startDate, endDate);
			}
		}
		return null;
	}
	
	public Map<String, Object> getReportGpsDataByLocation() throws Exception{
		String province = "";
		String city = "";
		String district = "";
		String startTime = "";
		String endTime = "";
		JSONArray orgs = null;
		JSONArray testTypes = null;
		
		if(requestBody!=null){
			province = (String)requestBody.get("province");
			city = (String)requestBody.get("city");
			district = (String)requestBody.get("district");
			startTime = (String)requestBody.get("start_time");
			endTime = (String)requestBody.get("end_time");
			orgs = requestBody.getJSONArray("orgs");
			testTypes = requestBody.getJSONArray("test_types");
		}
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
		
		if(province!=null && !"".equals(province)){
			if(city!=null && !"".equals(city)){
				if(district!=null && !"".equals(district)){
					return this.getReportDataByLocationDistrict("total_gps", province, city, district, orgs, testTypes, startDate, endDate);
				}else{
					return this.getReportDataByLocationCity("total_gps", province, city, orgs, testTypes, startDate, endDate);
				}
			}else{
				return this.getReportDataByLocationProvince("total_gps", province, orgs, testTypes, startDate, endDate);
			}
		}
		return null;
	}
	
	public Map<String, Object> getSignalDataByLocation() throws Exception{
		String province = "";
		String city = "";
		String district = "";
		String startTime = "";
		String endTime = "";
		String type = "";
		String isDetail = "";
		JSONArray orgs = null;
		JSONArray testTypes = null;
		
		if(requestBody!=null){
			province = (String)requestBody.get("province");
			city = (String)requestBody.get("city");
			district = (String)requestBody.get("district");
			startTime = (String)requestBody.get("start_time");
			endTime = (String)requestBody.get("end_time");
			orgs = requestBody.getJSONArray("orgs");
			testTypes = requestBody.getJSONArray("test_types");
			type = (String)requestBody.get("net_type");
			isDetail = (String)requestBody.get("is_detail");
		}
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
		
		if(isDetail!=null && !"".equals(isDetail) && "1".equals(isDetail)){
			if(type!=null && province!=null && !"".equals(province)){
				if(city!=null && !"".equals(city)){
					if(district!=null && !"".equals(district)){
						return this.getSignalDataByLocationDistrict(type, province, city, district, orgs, testTypes, startDate, endDate);
					}else{
						return this.getSignalDataByLocationCity(type, province, city, orgs, testTypes, startDate, endDate);
					}
				}else{
					return this.getSignalDataByLocationProvince(type, province, orgs, testTypes, startDate, endDate);
				}
			}
		}else{
			if(type!=null && province!=null && !"".equals(province)){
				if(city!=null && !"".equals(city)){
					if(district!=null && !"".equals(district)){
						return this.getSignalAnalyzeByLocationDistrict(type, province, city, district, orgs, testTypes, startDate, endDate);
					}else{
						return this.getSignalAnalyzeByLocationCity(type, province, city, orgs, testTypes, startDate, endDate);
					}
				}else{
					return this.getSignalAnalyzeByLocationProvince(type, province, orgs, testTypes, startDate, endDate);
				}
			}
		}
		
		return null;
	}
	
	public Map<String, Object> getSpeedtestDataByLocation() throws Exception{
		String province = "";
		String city = "";
		String district = "";
		String startTime = "";
		String endTime = "";
		String type = "";
		String isAnalyzed = "";
		JSONArray orgs = null;
		JSONArray testTypes = null;
		
		if(requestBody!=null){
			province = (String)requestBody.get("province");
			city = (String)requestBody.get("city");
			district = (String)requestBody.get("district");
			startTime = (String)requestBody.get("start_time");
			endTime = (String)requestBody.get("end_time");
			orgs = requestBody.getJSONArray("orgs");
			type = (String)requestBody.get("type");
			isAnalyzed = (String)requestBody.get("is_analyzed");
		}
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
		
		if(isAnalyzed!=null && !"".equals(isAnalyzed) && "0".equals(isAnalyzed)){
			if(type!=null && province!=null && !"".equals(province)){
				if(city!=null && !"".equals(city)){
					if(district!=null && !"".equals(district)){
						return this.getSpeedtestDataByLocationDistrict(type, province, city, district, orgs, startDate, endDate);
					}else{
						return this.getSpeedtestDataByLocationCity(type, province, city, orgs, startDate, endDate);
					}
				}else{
					return this.getSpeedtestDataByLocationProvince(type, province, orgs, startDate, endDate);
				}
			}
		}else{
			if(province!=null && !"".equals(province)){
				if(city!=null && !"".equals(city)){
					if(district!=null && !"".equals(district)){
						return this.getSpeedtestAnalyzeDataByLocationDistrict(province, city, district, orgs, startDate, endDate);
					}else{
						return this.getSpeedtestAnalyzeDataByLocationCity(province, city, orgs, startDate, endDate);
					}
				}else{
					return this.getSpeedtestAnalyzeDataByLocationProvince(province, orgs, startDate, endDate);
				}
			}
		}
		
		return null;
	}
	
	public Map<String, Object> getSpeedtestAnalyzeDataByLocation() throws Exception{
		String province = "";
		String city = "";
		String district = "";
		String startTime = "";
		String endTime = "";
		String type = "";
		String isAnalyzed = "";
		JSONArray orgs = null;
		JSONArray testTypes = null;
		
		if(requestBody!=null){
			province = (String)requestBody.get("province");
			city = (String)requestBody.get("city");
			district = (String)requestBody.get("district");
			startTime = (String)requestBody.get("start_time");
			endTime = (String)requestBody.get("end_time");
			orgs = requestBody.getJSONArray("orgs");
			type = (String)requestBody.get("type");
			isAnalyzed = (String)requestBody.get("is_analyzed");
		}
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
		
		if(isAnalyzed!=null && !"".equals(isAnalyzed) && "0".equals(isAnalyzed)){
			if(type!=null && province!=null && !"".equals(province)){
				if(city!=null && !"".equals(city)){
					if(district!=null && !"".equals(district)){
						return this.getSpeedtestDataByLocationDistrict(type, province, city, district, orgs, startDate, endDate);
					}else{
						return this.getSpeedtestDataByLocationCity(type, province, city, orgs, startDate, endDate);
					}
				}else{
					return this.getSpeedtestDataByLocationProvince(type, province, orgs, startDate, endDate);
				}
			}
		}else{
			if(type!=null && province!=null && !"".equals(province)){
				if(city!=null && !"".equals(city)){
					if(district!=null && !"".equals(district)){
						return this.getSignalAnalyzeByLocationDistrict(type, province, city, district, orgs, testTypes, startDate, endDate);
					}else{
						return this.getSignalAnalyzeByLocationCity(type, province, city, orgs, testTypes, startDate, endDate);
					}
				}else{
					return this.getSignalAnalyzeByLocationProvince(type, province, orgs, testTypes, startDate, endDate);
				}
			}
		}
		
		return null;
	}
	
	public Map<String, Object> getHttpTestDataByLocation() throws Exception{
		String province = "";
		String city = "";
		String district = "";
		String startTime = "";
		String endTime = "";
		String type = "";
		JSONArray orgs = null;
		
		if(requestBody!=null){
			province = (String)requestBody.get("province");
			city = (String)requestBody.get("city");
			district = (String)requestBody.get("district");
			startTime = (String)requestBody.get("start_time");
			endTime = (String)requestBody.get("end_time");
			orgs = requestBody.getJSONArray("orgs");
			type = (String)requestBody.get("type");
		}
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
		
		if(type!=null && province!=null && !"".equals(province)){
			if(city!=null && !"".equals(city)){
				if(district!=null && !"".equals(district)){
					return this.getHttpTestDataByLocationDistrict(type, province, city, district, orgs, startDate, endDate);
				}else{
					return this.getHttpTestDataByLocationCity(type, province, city, orgs, startDate, endDate);
				}
			}else{
				return this.getHttpTestDataByLocationProvince(type, province, orgs, startDate, endDate);
			}
		}
		
		return null;
	}
	
	public Map<String, Object> getBrowseTestDataByLocation() throws Exception{
		String province = "";
		String city = "";
		String district = "";
		String startTime = "";
		String endTime = "";
		JSONArray orgs = null;
		
		if(requestBody!=null){
			province = (String)requestBody.get("province");
			city = (String)requestBody.get("city");
			district = (String)requestBody.get("district");
			startTime = (String)requestBody.get("start_time");
			endTime = (String)requestBody.get("end_time");
			orgs = requestBody.getJSONArray("orgs");
		}
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
		
		if(province!=null && !"".equals(province)){
			if(city!=null && !"".equals(city)){
				if(district!=null && !"".equals(district)){
					return this.getBrowseTestDataByLocationDistrict(province, city, district, orgs, startDate, endDate);
				}else{
					return this.getBrowseTestDataByLocationCity(province, city, orgs, startDate, endDate);
				}
			}else{
				return this.getBrowseTestDataByLocationProvince(province, orgs, startDate, endDate);
			}
		}
		
		return null;
	}
	
	/**
	 * 根据省份获取测试数据
	 * @author Ocean
	 * @param province
	 * @param city
	 * @param district
	 * @param orgs
	 * @param testTypes
	 */
	public List<Map<String, String>> getTestDataByLocationProvince(String province, JSONArray orgs, JSONArray testTypes, Date startDate, Date endDate) throws Exception{
		List<ArrayList<QueryExpression>> qes = new ArrayList<ArrayList<QueryExpression>>();
		long startTime = startDate.getTime();
		long endTime = endDate.getTime();
		SimpleDateFormat dateFormat = new SimpleDateFormat(
				"yyyyMMdd");
		String startStr = dateFormat.format(startDate);
		String endStr = dateFormat.format(endDate);
		
		List<String> days = this.parseDays(startDate, endDate);
		if(days!=null && days.size()!=0){
			if(days.size()==1){
				ArrayList<QueryExpression> qe = new ArrayList<QueryExpression>();
				QueryExpression myQEDay = new QueryExpression("year_month_day",IndexOperator.EQ,startStr);
				QueryExpression myQETimeStart = new QueryExpression("data_time",IndexOperator.GTE,""+startTime);
				QueryExpression myQETimeEnd = new QueryExpression("data_time",IndexOperator.LTE,""+endTime);
				QueryExpression myQEProvince = new QueryExpression("cassandra_province",IndexOperator.EQ,""+province);
				qe.add(myQEDay);
				qe.add(myQETimeStart);
				qe.add(myQETimeEnd);
				qe.add(myQEProvince);
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
			return getTestData(orgs,testTypes,qes,"",-1);
		}
		return null;
	}

	public List<Map<String, String>> getTestDataByLocationCity(String province, String city, String district, JSONArray orgs, JSONArray testTypes){
		return null;
	}
	public List<Map<String, String>> getTestDataByLocationDistrict(String province, String city, String district, JSONArray orgs, JSONArray testTypes){
		return null;
	}
	
	/**
	 * 根据省份获取测试数据
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
		rangeQuery.setColumnNames("GPS位置","LAC/CID","TAC/PCI","网络类型","信号强度","测试时间","cassandra_province","cassandra_city","cassandra_district","cassandra_street","cassandra_street_number","平均速率(Kbps)","平均时长(ms)","时长(ms)","imei");
		QueryResult<OrderedRows<String, String, String>> result = rangeQuery
				.execute();
		OrderedRows<String, String, String> oRows = result.get();
		int count = 0;
		String lastKey = "";
		List<Row<String, String, String>> rowList = oRows.getList();
		for (Row<String, String, String> row : rowList) {
			
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
			if(count>20){
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
	 * 根据省份获取测试数据
	 * @author Ocean
	 * @param province
	 * @param city
	 * @param district
	 * @param orgs
	 * @param testTypes
	 */
	public Map<String, Object> getReportDataByLocationProvince(String type, String province, JSONArray orgs, JSONArray testTypes, Date startDate, Date endDate) throws Exception{
		List<ArrayList<QueryExpression>> qes = new ArrayList<ArrayList<QueryExpression>>();
		
		List<String> days = this.parseDays(startDate, endDate);
		if(days!=null && days.size()!=0){
			for(int i=0; i<days.size(); i++){
				ArrayList<QueryExpression> qe = new ArrayList<QueryExpression>();
				QueryExpression myQEDay = new QueryExpression("year_month_day",IndexOperator.EQ,days.get(i));
				QueryExpression myQEProvince = new QueryExpression("province",IndexOperator.EQ,""+province);
				qe.add(myQEDay);
				qe.add(myQEProvince);
				qes.add(qe);
			}
			return getReportData(orgs,type,testTypes,qes,"");
		}
		return null;
	}

	/**
	 * 根据省份获取测试数据
	 * @author Ocean
	 * @param province
	 * @param city
	 * @param district
	 * @param orgs
	 * @param testTypes
	 */
	public Map<String, Object> getReportDataByLocationCity(String type, String province, String city, JSONArray orgs, JSONArray testTypes, Date startDate, Date endDate) throws Exception{
		List<ArrayList<QueryExpression>> qes = new ArrayList<ArrayList<QueryExpression>>();
		
		List<String> days = this.parseDays(startDate, endDate);
		if(days!=null && days.size()!=0){
			for(int i=0; i<days.size(); i++){
				ArrayList<QueryExpression> qe = new ArrayList<QueryExpression>();
				QueryExpression myQEDay = new QueryExpression("year_month_day",IndexOperator.EQ,days.get(i));
				QueryExpression myQEProvince = new QueryExpression("province",IndexOperator.EQ,""+province);
				QueryExpression myQECity = new QueryExpression("city",IndexOperator.EQ,""+city);
				qe.add(myQEDay);
				qe.add(myQEProvince);
				qe.add(myQECity);
				qes.add(qe);
			}
			return getReportData(orgs,type,testTypes,qes,"");
		}
		return null;
	}
	
	/**
	 * 根据省份获取测试数据
	 * @author Ocean
	 * @param province
	 * @param city
	 * @param district
	 * @param orgs
	 * @param testTypes
	 */
	public Map<String, Object> getReportDataByLocationDistrict(String type, String province, String city, String district, JSONArray orgs, JSONArray testTypes, Date startDate, Date endDate) throws Exception{
		List<ArrayList<QueryExpression>> qes = new ArrayList<ArrayList<QueryExpression>>();
		
		List<String> days = this.parseDays(startDate, endDate);
		if(days!=null && days.size()!=0){
			for(int i=0; i<days.size(); i++){
				ArrayList<QueryExpression> qe = new ArrayList<QueryExpression>();
				QueryExpression myQEDay = new QueryExpression("year_month_day",IndexOperator.EQ,days.get(i));
				QueryExpression myQEProvince = new QueryExpression("province",IndexOperator.EQ,""+province);
				QueryExpression myQECity = new QueryExpression("city",IndexOperator.EQ,""+city);
				QueryExpression myQEDistrict = new QueryExpression("district",IndexOperator.EQ,""+district);
				qe.add(myQEDay);
				qe.add(myQEProvince);
				qe.add(myQECity);
				qe.add(myQEDistrict);
				qes.add(qe);
			}
			return getReportData(orgs,type,testTypes,qes,"");
		}
		return null;
	}
	
	/**
	 * 根据省份获取测试数据
	 * @author Ocean
	 * @param province
	 * @param city
	 * @param district
	 * @param orgs
	 * @param testTypes
	 */
	public Map<String, Object> getSignalDataByLocationProvince(String type, String province, JSONArray orgs, JSONArray testTypes, Date startDate, Date endDate) throws Exception{
		List<ArrayList<QueryExpression>> qes = new ArrayList<ArrayList<QueryExpression>>();
		long startTime = startDate.getTime();
		long endTime = endDate.getTime();
		SimpleDateFormat dateFormat = new SimpleDateFormat(
				"yyyyMMdd");
		String startStr = dateFormat.format(startDate);
		String endStr = dateFormat.format(endDate);
		
		List<String> days = this.parseDays(startDate, endDate);
		if(days!=null && days.size()!=0){
			if(days.size()==1){
				ArrayList<QueryExpression> qe = new ArrayList<QueryExpression>();
				QueryExpression myQEDay = new QueryExpression("year_month_day",IndexOperator.EQ,startStr);
				QueryExpression myQETimeStart = new QueryExpression("data_time",IndexOperator.GTE,""+startTime);
				QueryExpression myQETimeEnd = new QueryExpression("data_time",IndexOperator.LTE,""+endTime);
				QueryExpression myQEProvince = new QueryExpression("cassandra_province",IndexOperator.EQ,""+province);
				qe.add(myQEDay);
				qe.add(myQETimeStart);
				qe.add(myQETimeEnd);
				qe.add(myQEProvince);
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
			return getSignalData(type,orgs,testTypes,qes,"",-1,true);
		}
		return null;
	}

	/**
	 * 根据省份获取测试数据
	 * @author Ocean
	 * @param province
	 * @param city
	 * @param district
	 * @param orgs
	 * @param testTypes
	 */
	public Map<String, Object> getSignalDataByLocationCity(String type, String province, String city, JSONArray orgs, JSONArray testTypes, Date startDate, Date endDate) throws Exception{
		List<ArrayList<QueryExpression>> qes = new ArrayList<ArrayList<QueryExpression>>();
		long startTime = startDate.getTime();
		long endTime = endDate.getTime();
		SimpleDateFormat dateFormat = new SimpleDateFormat(
				"yyyyMMdd");
		String startStr = dateFormat.format(startDate);
		String endStr = dateFormat.format(endDate);
		
		List<String> days = this.parseDays(startDate, endDate);
		if(days!=null && days.size()!=0){
			if(days.size()==1){
				ArrayList<QueryExpression> qe = new ArrayList<QueryExpression>();
				QueryExpression myQEDay = new QueryExpression("year_month_day",IndexOperator.EQ,startStr);
				QueryExpression myQETimeStart = new QueryExpression("data_time",IndexOperator.GTE,""+startTime);
				QueryExpression myQETimeEnd = new QueryExpression("data_time",IndexOperator.LTE,""+endTime);
				QueryExpression myQEProvince = new QueryExpression("cassandra_province",IndexOperator.EQ,""+province);
				QueryExpression myQECity = new QueryExpression("cassandra_city",IndexOperator.EQ,""+city);
				qe.add(myQEDay);
				qe.add(myQETimeStart);
				qe.add(myQETimeEnd);
				qe.add(myQEProvince);
				qe.add(myQECity);
				qes.add(qe);
			}else if(days.size()==2){
				ArrayList<QueryExpression> qe = new ArrayList<QueryExpression>();
				QueryExpression myQEDay = new QueryExpression("year_month_day",IndexOperator.EQ,startStr);
				QueryExpression myQETime = new QueryExpression("data_time",IndexOperator.GTE,""+startTime);
				QueryExpression myQEProvince = new QueryExpression("cassandra_province",IndexOperator.EQ,""+province);
				QueryExpression myQECity = new QueryExpression("cassandra_city",IndexOperator.EQ,""+city);
				qe.add(myQEDay);
				qe.add(myQETime);
				qe.add(myQEProvince);
				qe.add(myQECity);
				qes.add(qe);
				
				ArrayList<QueryExpression> qe1 = new ArrayList<QueryExpression>();
				QueryExpression myQEDay1 = new QueryExpression("year_month_day",IndexOperator.EQ,endStr);
				QueryExpression myQETime1 = new QueryExpression("data_time",IndexOperator.LTE,""+endTime);
				QueryExpression myQEProvince1 = new QueryExpression("cassandra_province",IndexOperator.EQ,""+province);
				QueryExpression myQECity1 = new QueryExpression("cassandra_city",IndexOperator.EQ,""+city);
				qe.add(myQEDay1);
				qe.add(myQETime1);
				qe.add(myQEProvince1);
				qe.add(myQECity1);
				qes.add(qe1);
			}else{
				for(int i=0; i<days.size(); i++){
					if(days.get(i).equals(startStr)){
						ArrayList<QueryExpression> qe = new ArrayList<QueryExpression>();
						QueryExpression myQEDay = new QueryExpression("year_month_day",IndexOperator.EQ,startStr);
						QueryExpression myQETime = new QueryExpression("data_time",IndexOperator.GTE,""+startTime);
						QueryExpression myQEProvince = new QueryExpression("cassandra_province",IndexOperator.EQ,""+province);
						QueryExpression myQECity = new QueryExpression("cassandra_city",IndexOperator.EQ,""+city);
						qe.add(myQEDay);
						qe.add(myQETime);
						qe.add(myQEProvince);
						qe.add(myQECity);
						qes.add(qe);
					}else if(days.get(i).equals(endStr)){
						ArrayList<QueryExpression> qe = new ArrayList<QueryExpression>();
						QueryExpression myQEDay = new QueryExpression("year_month_day",IndexOperator.EQ,endStr);
						QueryExpression myQETime = new QueryExpression("data_time",IndexOperator.LTE,""+endTime);
						QueryExpression myQEProvince = new QueryExpression("cassandra_province",IndexOperator.EQ,""+province);
						QueryExpression myQECity = new QueryExpression("cassandra_city",IndexOperator.EQ,""+city);
						qe.add(myQEDay);
						qe.add(myQETime);
						qe.add(myQEProvince);
						qe.add(myQECity);
						qes.add(qe);
					}else{
						long timeTemp = (dateFormat.parse(days.get(i))).getTime();
						ArrayList<QueryExpression> qe = new ArrayList<QueryExpression>();
						QueryExpression myQEDay = new QueryExpression("year_month_day",IndexOperator.EQ,days.get(i));
						QueryExpression myQETime = new QueryExpression("data_time",IndexOperator.GTE,""+timeTemp);
						QueryExpression myQEProvince = new QueryExpression("cassandra_province",IndexOperator.EQ,""+province);
						QueryExpression myQECity = new QueryExpression("cassandra_city",IndexOperator.EQ,""+city);
						qe.add(myQEDay);
						qe.add(myQETime);
						qe.add(myQEProvince);
						qe.add(myQECity);
						qes.add(qe);
					}
				}
			}
			return getSignalData(type,orgs,testTypes,qes,"",-1,true);
		}
		return null;
	}
	
	/**
	 * 根据省份获取测试数据
	 * @author Ocean
	 * @param province
	 * @param city
	 * @param district
	 * @param orgs
	 * @param testTypes
	 */
	public Map<String, Object> getSignalDataByLocationDistrict(String type, String province, String city, String district, JSONArray orgs, JSONArray testTypes, Date startDate, Date endDate) throws Exception{
		List<ArrayList<QueryExpression>> qes = new ArrayList<ArrayList<QueryExpression>>();
		long startTime = startDate.getTime();
		long endTime = endDate.getTime();
		SimpleDateFormat dateFormat = new SimpleDateFormat(
				"yyyyMMdd");
		String startStr = dateFormat.format(startDate);
		String endStr = dateFormat.format(endDate);
		
		List<String> days = this.parseDays(startDate, endDate);
		if(days!=null && days.size()!=0){
			if(days.size()==1){
				ArrayList<QueryExpression> qe = new ArrayList<QueryExpression>();
				QueryExpression myQEDay = new QueryExpression("year_month_day",IndexOperator.EQ,startStr);
				QueryExpression myQETimeStart = new QueryExpression("data_time",IndexOperator.GTE,""+startTime);
				QueryExpression myQETimeEnd = new QueryExpression("data_time",IndexOperator.LTE,""+endTime);
				QueryExpression myQEProvince = new QueryExpression("cassandra_province",IndexOperator.EQ,""+province);
				QueryExpression myQECity = new QueryExpression("cassandra_city",IndexOperator.EQ,""+city);
				QueryExpression myQEDistrict = new QueryExpression("cassandra_district",IndexOperator.EQ,""+district);
				qe.add(myQEDay);
				qe.add(myQETimeStart);
				qe.add(myQETimeEnd);
				qe.add(myQEProvince);
				qe.add(myQECity);
				qe.add(myQEDistrict);
				qes.add(qe);
			}else if(days.size()==2){
				ArrayList<QueryExpression> qe = new ArrayList<QueryExpression>();
				QueryExpression myQEDay = new QueryExpression("year_month_day",IndexOperator.EQ,startStr);
				QueryExpression myQETime = new QueryExpression("data_time",IndexOperator.GTE,""+startTime);
				QueryExpression myQEProvince = new QueryExpression("cassandra_province",IndexOperator.EQ,""+province);
				QueryExpression myQECity = new QueryExpression("cassandra_city",IndexOperator.EQ,""+city);
				QueryExpression myQEDistrict = new QueryExpression("cassandra_district",IndexOperator.EQ,""+district);
				qe.add(myQEDay);
				qe.add(myQETime);
				qe.add(myQEProvince);
				qe.add(myQECity);
				qe.add(myQEDistrict);
				qes.add(qe);
				
				ArrayList<QueryExpression> qe1 = new ArrayList<QueryExpression>();
				QueryExpression myQEDay1 = new QueryExpression("year_month_day",IndexOperator.EQ,endStr);
				QueryExpression myQETime1 = new QueryExpression("data_time",IndexOperator.LTE,""+endTime);
				QueryExpression myQEProvince1 = new QueryExpression("cassandra_province",IndexOperator.EQ,""+province);
				QueryExpression myQECity1 = new QueryExpression("cassandra_city",IndexOperator.EQ,""+city);
				QueryExpression myQEDistrict1 = new QueryExpression("cassandra_district",IndexOperator.EQ,""+district);
				qe.add(myQEDay1);
				qe.add(myQETime1);
				qe.add(myQEProvince1);
				qe.add(myQECity1);
				qe.add(myQEDistrict1);
				qes.add(qe1);
			}else{
				for(int i=0; i<days.size(); i++){
					if(days.get(i).equals(startStr)){
						ArrayList<QueryExpression> qe = new ArrayList<QueryExpression>();
						QueryExpression myQEDay = new QueryExpression("year_month_day",IndexOperator.EQ,startStr);
						QueryExpression myQETime = new QueryExpression("data_time",IndexOperator.GTE,""+startTime);
						QueryExpression myQEProvince = new QueryExpression("cassandra_province",IndexOperator.EQ,""+province);
						QueryExpression myQECity = new QueryExpression("cassandra_city",IndexOperator.EQ,""+city);
						QueryExpression myQEDistrict = new QueryExpression("cassandra_district",IndexOperator.EQ,""+district);
						qe.add(myQEDay);
						qe.add(myQETime);
						qe.add(myQEProvince);
						qe.add(myQECity);
						qe.add(myQEDistrict);
						qes.add(qe);
					}else if(days.get(i).equals(endStr)){
						ArrayList<QueryExpression> qe = new ArrayList<QueryExpression>();
						QueryExpression myQEDay = new QueryExpression("year_month_day",IndexOperator.EQ,endStr);
						QueryExpression myQETime = new QueryExpression("data_time",IndexOperator.LTE,""+endTime);
						QueryExpression myQEProvince = new QueryExpression("cassandra_province",IndexOperator.EQ,""+province);
						QueryExpression myQECity = new QueryExpression("cassandra_city",IndexOperator.EQ,""+city);
						QueryExpression myQEDistrict = new QueryExpression("cassandra_district",IndexOperator.EQ,""+district);
						qe.add(myQEDay);
						qe.add(myQETime);
						qe.add(myQEProvince);
						qe.add(myQECity);
						qe.add(myQEDistrict);
						qes.add(qe);
					}else{
						long timeTemp = (dateFormat.parse(days.get(i))).getTime();
						ArrayList<QueryExpression> qe = new ArrayList<QueryExpression>();
						QueryExpression myQEDay = new QueryExpression("year_month_day",IndexOperator.EQ,days.get(i));
						QueryExpression myQETime = new QueryExpression("data_time",IndexOperator.GTE,""+timeTemp);
						QueryExpression myQEProvince = new QueryExpression("cassandra_province",IndexOperator.EQ,""+province);
						QueryExpression myQECity = new QueryExpression("cassandra_city",IndexOperator.EQ,""+city);
						QueryExpression myQEDistrict = new QueryExpression("cassandra_district",IndexOperator.EQ,""+district);
						qe.add(myQEDay);
						qe.add(myQETime);
						qe.add(myQEProvince);
						qe.add(myQECity);
						qe.add(myQEDistrict);
						qes.add(qe);
					}
				}
			}
			return getSignalData(type,orgs,testTypes,qes,"",-1,true);
		}
		return null;
	}
	
	/**
	 * 根据省份获取测试数据
	 * @author Ocean
	 * @param province
	 * @param city
	 * @param district
	 * @param orgs
	 * @param testTypes
	 */
	public Map<String, Object> getSignalAnalyzeByLocationProvince(String type, String province, JSONArray orgs, JSONArray testTypes, Date startDate, Date endDate) throws Exception{
		List<ArrayList<QueryExpression>> qes = new ArrayList<ArrayList<QueryExpression>>();
		long startTime = startDate.getTime();
		long endTime = endDate.getTime();
		SimpleDateFormat dateFormat = new SimpleDateFormat(
				"yyyyMMdd");
		String startStr = dateFormat.format(startDate);
		String endStr = dateFormat.format(endDate);
		
		List<String> days = this.parseDays(startDate, endDate);
		if(days!=null && days.size()!=0){
			if(days.size()==1){
				ArrayList<QueryExpression> qe = new ArrayList<QueryExpression>();
				QueryExpression myQEDay = new QueryExpression("year_month_day",IndexOperator.EQ,startStr);
				QueryExpression myQETimeStart = new QueryExpression("data_time",IndexOperator.GTE,""+startTime);
				QueryExpression myQETimeEnd = new QueryExpression("data_time",IndexOperator.LTE,""+endTime);
				QueryExpression myQEProvince = new QueryExpression("cassandra_province",IndexOperator.EQ,""+province);
				qe.add(myQEDay);
				qe.add(myQETimeStart);
				qe.add(myQETimeEnd);
				qe.add(myQEProvince);
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
			return getSignalData(type,orgs,testTypes,qes,"",-1,false);
		}
		return null;
	}

	/**
	 * 根据省份获取测试数据
	 * @author Ocean
	 * @param province
	 * @param city
	 * @param district
	 * @param orgs
	 * @param testTypes
	 */
	public Map<String, Object> getSignalAnalyzeByLocationCity(String type, String province, String city, JSONArray orgs, JSONArray testTypes, Date startDate, Date endDate) throws Exception{
		List<ArrayList<QueryExpression>> qes = new ArrayList<ArrayList<QueryExpression>>();
		long startTime = startDate.getTime();
		long endTime = endDate.getTime();
		SimpleDateFormat dateFormat = new SimpleDateFormat(
				"yyyyMMdd");
		String startStr = dateFormat.format(startDate);
		String endStr = dateFormat.format(endDate);
		
		List<String> days = this.parseDays(startDate, endDate);
		if(days!=null && days.size()!=0){
			if(days.size()==1){
				ArrayList<QueryExpression> qe = new ArrayList<QueryExpression>();
				QueryExpression myQEDay = new QueryExpression("year_month_day",IndexOperator.EQ,startStr);
				QueryExpression myQETimeStart = new QueryExpression("data_time",IndexOperator.GTE,""+startTime);
				QueryExpression myQETimeEnd = new QueryExpression("data_time",IndexOperator.LTE,""+endTime);
				QueryExpression myQEProvince = new QueryExpression("cassandra_province",IndexOperator.EQ,""+province);
				QueryExpression myQECity = new QueryExpression("cassandra_city",IndexOperator.EQ,""+city);
				qe.add(myQEDay);
				qe.add(myQETimeStart);
				qe.add(myQETimeEnd);
				qe.add(myQEProvince);
				qe.add(myQECity);
				qes.add(qe);
			}else if(days.size()==2){
				ArrayList<QueryExpression> qe = new ArrayList<QueryExpression>();
				QueryExpression myQEDay = new QueryExpression("year_month_day",IndexOperator.EQ,startStr);
				QueryExpression myQETime = new QueryExpression("data_time",IndexOperator.GTE,""+startTime);
				QueryExpression myQEProvince = new QueryExpression("cassandra_province",IndexOperator.EQ,""+province);
				QueryExpression myQECity = new QueryExpression("cassandra_city",IndexOperator.EQ,""+city);
				qe.add(myQEDay);
				qe.add(myQETime);
				qe.add(myQEProvince);
				qe.add(myQECity);
				qes.add(qe);
				
				ArrayList<QueryExpression> qe1 = new ArrayList<QueryExpression>();
				QueryExpression myQEDay1 = new QueryExpression("year_month_day",IndexOperator.EQ,endStr);
				QueryExpression myQETime1 = new QueryExpression("data_time",IndexOperator.LTE,""+endTime);
				QueryExpression myQEProvince1 = new QueryExpression("cassandra_province",IndexOperator.EQ,""+province);
				QueryExpression myQECity1 = new QueryExpression("cassandra_city",IndexOperator.EQ,""+city);
				qe.add(myQEDay1);
				qe.add(myQETime1);
				qe.add(myQEProvince1);
				qe.add(myQECity1);
				qes.add(qe1);
			}else{
				for(int i=0; i<days.size(); i++){
					if(days.get(i).equals(startStr)){
						ArrayList<QueryExpression> qe = new ArrayList<QueryExpression>();
						QueryExpression myQEDay = new QueryExpression("year_month_day",IndexOperator.EQ,startStr);
						QueryExpression myQETime = new QueryExpression("data_time",IndexOperator.GTE,""+startTime);
						QueryExpression myQEProvince = new QueryExpression("cassandra_province",IndexOperator.EQ,""+province);
						QueryExpression myQECity = new QueryExpression("cassandra_city",IndexOperator.EQ,""+city);
						qe.add(myQEDay);
						qe.add(myQETime);
						qe.add(myQEProvince);
						qe.add(myQECity);
						qes.add(qe);
					}else if(days.get(i).equals(endStr)){
						ArrayList<QueryExpression> qe = new ArrayList<QueryExpression>();
						QueryExpression myQEDay = new QueryExpression("year_month_day",IndexOperator.EQ,endStr);
						QueryExpression myQETime = new QueryExpression("data_time",IndexOperator.LTE,""+endTime);
						QueryExpression myQEProvince = new QueryExpression("cassandra_province",IndexOperator.EQ,""+province);
						QueryExpression myQECity = new QueryExpression("cassandra_city",IndexOperator.EQ,""+city);
						qe.add(myQEDay);
						qe.add(myQETime);
						qe.add(myQEProvince);
						qe.add(myQECity);
						qes.add(qe);
					}else{
						long timeTemp = (dateFormat.parse(days.get(i))).getTime();
						ArrayList<QueryExpression> qe = new ArrayList<QueryExpression>();
						QueryExpression myQEDay = new QueryExpression("year_month_day",IndexOperator.EQ,days.get(i));
						QueryExpression myQETime = new QueryExpression("data_time",IndexOperator.GTE,""+timeTemp);
						QueryExpression myQEProvince = new QueryExpression("cassandra_province",IndexOperator.EQ,""+province);
						QueryExpression myQECity = new QueryExpression("cassandra_city",IndexOperator.EQ,""+city);
						qe.add(myQEDay);
						qe.add(myQETime);
						qe.add(myQEProvince);
						qe.add(myQECity);
						qes.add(qe);
					}
				}
			}
			return getSignalData(type,orgs,testTypes,qes,"",-1,false);
		}
		return null;
	}
	
	/**
	 * 根据省份获取测试数据
	 * @author Ocean
	 * @param province
	 * @param city
	 * @param district
	 * @param orgs
	 * @param testTypes
	 */
	public Map<String, Object> getSignalAnalyzeByLocationDistrict(String type, String province, String city, String district, JSONArray orgs, JSONArray testTypes, Date startDate, Date endDate) throws Exception{
		List<ArrayList<QueryExpression>> qes = new ArrayList<ArrayList<QueryExpression>>();
		long startTime = startDate.getTime();
		long endTime = endDate.getTime();
		SimpleDateFormat dateFormat = new SimpleDateFormat(
				"yyyyMMdd");
		String startStr = dateFormat.format(startDate);
		String endStr = dateFormat.format(endDate);
		
		List<String> days = this.parseDays(startDate, endDate);
		if(days!=null && days.size()!=0){
			if(days.size()==1){
				ArrayList<QueryExpression> qe = new ArrayList<QueryExpression>();
				QueryExpression myQEDay = new QueryExpression("year_month_day",IndexOperator.EQ,startStr);
				QueryExpression myQETimeStart = new QueryExpression("data_time",IndexOperator.GTE,""+startTime);
				QueryExpression myQETimeEnd = new QueryExpression("data_time",IndexOperator.LTE,""+endTime);
				QueryExpression myQEProvince = new QueryExpression("cassandra_province",IndexOperator.EQ,""+province);
				QueryExpression myQECity = new QueryExpression("cassandra_city",IndexOperator.EQ,""+city);
				QueryExpression myQEDistrict = new QueryExpression("cassandra_district",IndexOperator.EQ,""+district);
				qe.add(myQEDay);
				qe.add(myQETimeStart);
				qe.add(myQETimeEnd);
				qe.add(myQEProvince);
				qe.add(myQECity);
				qe.add(myQEDistrict);
				qes.add(qe);
			}else if(days.size()==2){
				ArrayList<QueryExpression> qe = new ArrayList<QueryExpression>();
				QueryExpression myQEDay = new QueryExpression("year_month_day",IndexOperator.EQ,startStr);
				QueryExpression myQETime = new QueryExpression("data_time",IndexOperator.GTE,""+startTime);
				QueryExpression myQEProvince = new QueryExpression("cassandra_province",IndexOperator.EQ,""+province);
				QueryExpression myQECity = new QueryExpression("cassandra_city",IndexOperator.EQ,""+city);
				QueryExpression myQEDistrict = new QueryExpression("cassandra_district",IndexOperator.EQ,""+district);
				qe.add(myQEDay);
				qe.add(myQETime);
				qe.add(myQEProvince);
				qe.add(myQECity);
				qe.add(myQEDistrict);
				qes.add(qe);
				
				ArrayList<QueryExpression> qe1 = new ArrayList<QueryExpression>();
				QueryExpression myQEDay1 = new QueryExpression("year_month_day",IndexOperator.EQ,endStr);
				QueryExpression myQETime1 = new QueryExpression("data_time",IndexOperator.LTE,""+endTime);
				QueryExpression myQEProvince1 = new QueryExpression("cassandra_province",IndexOperator.EQ,""+province);
				QueryExpression myQECity1 = new QueryExpression("cassandra_city",IndexOperator.EQ,""+city);
				QueryExpression myQEDistrict1 = new QueryExpression("cassandra_district",IndexOperator.EQ,""+district);
				qe.add(myQEDay1);
				qe.add(myQETime1);
				qe.add(myQEProvince1);
				qe.add(myQECity1);
				qe.add(myQEDistrict1);
				qes.add(qe1);
			}else{
				for(int i=0; i<days.size(); i++){
					if(days.get(i).equals(startStr)){
						ArrayList<QueryExpression> qe = new ArrayList<QueryExpression>();
						QueryExpression myQEDay = new QueryExpression("year_month_day",IndexOperator.EQ,startStr);
						QueryExpression myQETime = new QueryExpression("data_time",IndexOperator.GTE,""+startTime);
						QueryExpression myQEProvince = new QueryExpression("cassandra_province",IndexOperator.EQ,""+province);
						QueryExpression myQECity = new QueryExpression("cassandra_city",IndexOperator.EQ,""+city);
						QueryExpression myQEDistrict = new QueryExpression("cassandra_district",IndexOperator.EQ,""+district);
						qe.add(myQEDay);
						qe.add(myQETime);
						qe.add(myQEProvince);
						qe.add(myQECity);
						qe.add(myQEDistrict);
						qes.add(qe);
					}else if(days.get(i).equals(endStr)){
						ArrayList<QueryExpression> qe = new ArrayList<QueryExpression>();
						QueryExpression myQEDay = new QueryExpression("year_month_day",IndexOperator.EQ,endStr);
						QueryExpression myQETime = new QueryExpression("data_time",IndexOperator.LTE,""+endTime);
						QueryExpression myQEProvince = new QueryExpression("cassandra_province",IndexOperator.EQ,""+province);
						QueryExpression myQECity = new QueryExpression("cassandra_city",IndexOperator.EQ,""+city);
						QueryExpression myQEDistrict = new QueryExpression("cassandra_district",IndexOperator.EQ,""+district);
						qe.add(myQEDay);
						qe.add(myQETime);
						qe.add(myQEProvince);
						qe.add(myQECity);
						qe.add(myQEDistrict);
						qes.add(qe);
					}else{
						long timeTemp = (dateFormat.parse(days.get(i))).getTime();
						ArrayList<QueryExpression> qe = new ArrayList<QueryExpression>();
						QueryExpression myQEDay = new QueryExpression("year_month_day",IndexOperator.EQ,days.get(i));
						QueryExpression myQETime = new QueryExpression("data_time",IndexOperator.GTE,""+timeTemp);
						QueryExpression myQEProvince = new QueryExpression("cassandra_province",IndexOperator.EQ,""+province);
						QueryExpression myQECity = new QueryExpression("cassandra_city",IndexOperator.EQ,""+city);
						QueryExpression myQEDistrict = new QueryExpression("cassandra_district",IndexOperator.EQ,""+district);
						qe.add(myQEDay);
						qe.add(myQETime);
						qe.add(myQEProvince);
						qe.add(myQECity);
						qe.add(myQEDistrict);
						qes.add(qe);
					}
				}
			}
			return getSignalData(type,orgs,testTypes,qes,"",-1,false);
		}
		return null;
	}
	
	/**
	 * 根据省份获取测试数据
	 * @author Ocean
	 * @param province
	 * @param city
	 * @param district
	 * @param orgs
	 * @param testTypes
	 */
	public Map<String, Object> getSpeedtestDataByLocationProvince(String type, String province, JSONArray orgs, Date startDate, Date endDate) throws Exception{
		List<ArrayList<QueryExpression>> qes = new ArrayList<ArrayList<QueryExpression>>();
		long startTime = startDate.getTime();
		long endTime = endDate.getTime();
		SimpleDateFormat dateFormat = new SimpleDateFormat(
				"yyyyMMdd");
		String startStr = dateFormat.format(startDate);
		String endStr = dateFormat.format(endDate);
		
		List<String> days = this.parseDays(startDate, endDate);
		if(days!=null && days.size()!=0){
			if(days.size()==1){
				ArrayList<QueryExpression> qe = new ArrayList<QueryExpression>();
				QueryExpression myQEDay = new QueryExpression("year_month_day",IndexOperator.EQ,startStr);
				QueryExpression myQETimeStart = new QueryExpression("data_time",IndexOperator.GTE,""+startTime);
				QueryExpression myQETimeEnd = new QueryExpression("data_time",IndexOperator.LTE,""+endTime);
				QueryExpression myQEProvince = new QueryExpression("cassandra_province",IndexOperator.EQ,""+province);
				qe.add(myQEDay);
				qe.add(myQETimeStart);
				qe.add(myQETimeEnd);
				qe.add(myQEProvince);
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
			return getSpeedtestData(type,orgs,qes,"",-1,true);
		}
		return null;
	}

	/**
	 * 根据省份获取测试数据
	 * @author Ocean
	 * @param province
	 * @param city
	 * @param district
	 * @param orgs
	 * @param testTypes
	 */
	public Map<String, Object> getSpeedtestDataByLocationCity(String type, String province, String city, JSONArray orgs, Date startDate, Date endDate) throws Exception{
		List<ArrayList<QueryExpression>> qes = new ArrayList<ArrayList<QueryExpression>>();
		long startTime = startDate.getTime();
		long endTime = endDate.getTime();
		SimpleDateFormat dateFormat = new SimpleDateFormat(
				"yyyyMMdd");
		String startStr = dateFormat.format(startDate);
		String endStr = dateFormat.format(endDate);
		
		List<String> days = this.parseDays(startDate, endDate);
		if(days!=null && days.size()!=0){
			if(days.size()==1){
				ArrayList<QueryExpression> qe = new ArrayList<QueryExpression>();
				QueryExpression myQEDay = new QueryExpression("year_month_day",IndexOperator.EQ,startStr);
				QueryExpression myQETimeStart = new QueryExpression("data_time",IndexOperator.GTE,""+startTime);
				QueryExpression myQETimeEnd = new QueryExpression("data_time",IndexOperator.LTE,""+endTime);
				QueryExpression myQEProvince = new QueryExpression("cassandra_province",IndexOperator.EQ,""+province);
				QueryExpression myQECity = new QueryExpression("cassandra_city",IndexOperator.EQ,""+city);
				qe.add(myQEDay);
				qe.add(myQETimeStart);
				qe.add(myQETimeEnd);
				qe.add(myQEProvince);
				qe.add(myQECity);
				qes.add(qe);
			}else if(days.size()==2){
				ArrayList<QueryExpression> qe = new ArrayList<QueryExpression>();
				QueryExpression myQEDay = new QueryExpression("year_month_day",IndexOperator.EQ,startStr);
				QueryExpression myQETime = new QueryExpression("data_time",IndexOperator.GTE,""+startTime);
				QueryExpression myQEProvince = new QueryExpression("cassandra_province",IndexOperator.EQ,""+province);
				QueryExpression myQECity = new QueryExpression("cassandra_city",IndexOperator.EQ,""+city);
				qe.add(myQEDay);
				qe.add(myQETime);
				qe.add(myQEProvince);
				qe.add(myQECity);
				qes.add(qe);
				
				ArrayList<QueryExpression> qe1 = new ArrayList<QueryExpression>();
				QueryExpression myQEDay1 = new QueryExpression("year_month_day",IndexOperator.EQ,endStr);
				QueryExpression myQETime1 = new QueryExpression("data_time",IndexOperator.LTE,""+endTime);
				QueryExpression myQEProvince1 = new QueryExpression("cassandra_province",IndexOperator.EQ,""+province);
				QueryExpression myQECity1 = new QueryExpression("cassandra_city",IndexOperator.EQ,""+city);
				qe.add(myQEDay1);
				qe.add(myQETime1);
				qe.add(myQEProvince1);
				qe.add(myQECity1);
				qes.add(qe1);
			}else{
				for(int i=0; i<days.size(); i++){
					if(days.get(i).equals(startStr)){
						ArrayList<QueryExpression> qe = new ArrayList<QueryExpression>();
						QueryExpression myQEDay = new QueryExpression("year_month_day",IndexOperator.EQ,startStr);
						QueryExpression myQETime = new QueryExpression("data_time",IndexOperator.GTE,""+startTime);
						QueryExpression myQEProvince = new QueryExpression("cassandra_province",IndexOperator.EQ,""+province);
						QueryExpression myQECity = new QueryExpression("cassandra_city",IndexOperator.EQ,""+city);
						qe.add(myQEDay);
						qe.add(myQETime);
						qe.add(myQEProvince);
						qe.add(myQECity);
						qes.add(qe);
					}else if(days.get(i).equals(endStr)){
						ArrayList<QueryExpression> qe = new ArrayList<QueryExpression>();
						QueryExpression myQEDay = new QueryExpression("year_month_day",IndexOperator.EQ,endStr);
						QueryExpression myQETime = new QueryExpression("data_time",IndexOperator.LTE,""+endTime);
						QueryExpression myQEProvince = new QueryExpression("cassandra_province",IndexOperator.EQ,""+province);
						QueryExpression myQECity = new QueryExpression("cassandra_city",IndexOperator.EQ,""+city);
						qe.add(myQEDay);
						qe.add(myQETime);
						qe.add(myQEProvince);
						qe.add(myQECity);
						qes.add(qe);
					}else{
						long timeTemp = (dateFormat.parse(days.get(i))).getTime();
						ArrayList<QueryExpression> qe = new ArrayList<QueryExpression>();
						QueryExpression myQEDay = new QueryExpression("year_month_day",IndexOperator.EQ,days.get(i));
						QueryExpression myQETime = new QueryExpression("data_time",IndexOperator.GTE,""+timeTemp);
						QueryExpression myQEProvince = new QueryExpression("cassandra_province",IndexOperator.EQ,""+province);
						QueryExpression myQECity = new QueryExpression("cassandra_city",IndexOperator.EQ,""+city);
						qe.add(myQEDay);
						qe.add(myQETime);
						qe.add(myQEProvince);
						qe.add(myQECity);
						qes.add(qe);
					}
				}
			}
			return getSpeedtestData(type,orgs,qes,"",-1,true);
		}
		return null;
	}
	
	/**
	 * 根据省份获取测试数据
	 * @author Ocean
	 * @param province
	 * @param city
	 * @param district
	 * @param orgs
	 * @param testTypes
	 */
	public Map<String, Object> getSpeedtestDataByLocationDistrict(String type, String province, String city, String district, JSONArray orgs, Date startDate, Date endDate) throws Exception{
		List<ArrayList<QueryExpression>> qes = new ArrayList<ArrayList<QueryExpression>>();
		long startTime = startDate.getTime();
		long endTime = endDate.getTime();
		SimpleDateFormat dateFormat = new SimpleDateFormat(
				"yyyyMMdd");
		String startStr = dateFormat.format(startDate);
		String endStr = dateFormat.format(endDate);
		
		List<String> days = this.parseDays(startDate, endDate);
		if(days!=null && days.size()!=0){
			if(days.size()==1){
				ArrayList<QueryExpression> qe = new ArrayList<QueryExpression>();
				QueryExpression myQEDay = new QueryExpression("year_month_day",IndexOperator.EQ,startStr);
				QueryExpression myQETimeStart = new QueryExpression("data_time",IndexOperator.GTE,""+startTime);
				QueryExpression myQETimeEnd = new QueryExpression("data_time",IndexOperator.LTE,""+endTime);
				QueryExpression myQEProvince = new QueryExpression("cassandra_province",IndexOperator.EQ,""+province);
				QueryExpression myQECity = new QueryExpression("cassandra_city",IndexOperator.EQ,""+city);
				QueryExpression myQEDistrict = new QueryExpression("cassandra_district",IndexOperator.EQ,""+district);
				qe.add(myQEDay);
				qe.add(myQETimeStart);
				qe.add(myQETimeEnd);
				qe.add(myQEProvince);
				qe.add(myQECity);
				qe.add(myQEDistrict);
				qes.add(qe);
			}else if(days.size()==2){
				ArrayList<QueryExpression> qe = new ArrayList<QueryExpression>();
				QueryExpression myQEDay = new QueryExpression("year_month_day",IndexOperator.EQ,startStr);
				QueryExpression myQETime = new QueryExpression("data_time",IndexOperator.GTE,""+startTime);
				QueryExpression myQEProvince = new QueryExpression("cassandra_province",IndexOperator.EQ,""+province);
				QueryExpression myQECity = new QueryExpression("cassandra_city",IndexOperator.EQ,""+city);
				QueryExpression myQEDistrict = new QueryExpression("cassandra_district",IndexOperator.EQ,""+district);
				qe.add(myQEDay);
				qe.add(myQETime);
				qe.add(myQEProvince);
				qe.add(myQECity);
				qe.add(myQEDistrict);
				qes.add(qe);
				
				ArrayList<QueryExpression> qe1 = new ArrayList<QueryExpression>();
				QueryExpression myQEDay1 = new QueryExpression("year_month_day",IndexOperator.EQ,endStr);
				QueryExpression myQETime1 = new QueryExpression("data_time",IndexOperator.LTE,""+endTime);
				QueryExpression myQEProvince1 = new QueryExpression("cassandra_province",IndexOperator.EQ,""+province);
				QueryExpression myQECity1 = new QueryExpression("cassandra_city",IndexOperator.EQ,""+city);
				QueryExpression myQEDistrict1 = new QueryExpression("cassandra_district",IndexOperator.EQ,""+district);
				qe.add(myQEDay1);
				qe.add(myQETime1);
				qe.add(myQEProvince1);
				qe.add(myQECity1);
				qe.add(myQEDistrict1);
				qes.add(qe1);
			}else{
				for(int i=0; i<days.size(); i++){
					if(days.get(i).equals(startStr)){
						ArrayList<QueryExpression> qe = new ArrayList<QueryExpression>();
						QueryExpression myQEDay = new QueryExpression("year_month_day",IndexOperator.EQ,startStr);
						QueryExpression myQETime = new QueryExpression("data_time",IndexOperator.GTE,""+startTime);
						QueryExpression myQEProvince = new QueryExpression("cassandra_province",IndexOperator.EQ,""+province);
						QueryExpression myQECity = new QueryExpression("cassandra_city",IndexOperator.EQ,""+city);
						QueryExpression myQEDistrict = new QueryExpression("cassandra_district",IndexOperator.EQ,""+district);
						qe.add(myQEDay);
						qe.add(myQETime);
						qe.add(myQEProvince);
						qe.add(myQECity);
						qe.add(myQEDistrict);
						qes.add(qe);
					}else if(days.get(i).equals(endStr)){
						ArrayList<QueryExpression> qe = new ArrayList<QueryExpression>();
						QueryExpression myQEDay = new QueryExpression("year_month_day",IndexOperator.EQ,endStr);
						QueryExpression myQETime = new QueryExpression("data_time",IndexOperator.LTE,""+endTime);
						QueryExpression myQEProvince = new QueryExpression("cassandra_province",IndexOperator.EQ,""+province);
						QueryExpression myQECity = new QueryExpression("cassandra_city",IndexOperator.EQ,""+city);
						QueryExpression myQEDistrict = new QueryExpression("cassandra_district",IndexOperator.EQ,""+district);
						qe.add(myQEDay);
						qe.add(myQETime);
						qe.add(myQEProvince);
						qe.add(myQECity);
						qe.add(myQEDistrict);
						qes.add(qe);
					}else{
						long timeTemp = (dateFormat.parse(days.get(i))).getTime();
						ArrayList<QueryExpression> qe = new ArrayList<QueryExpression>();
						QueryExpression myQEDay = new QueryExpression("year_month_day",IndexOperator.EQ,days.get(i));
						QueryExpression myQETime = new QueryExpression("data_time",IndexOperator.GTE,""+timeTemp);
						QueryExpression myQEProvince = new QueryExpression("cassandra_province",IndexOperator.EQ,""+province);
						QueryExpression myQECity = new QueryExpression("cassandra_city",IndexOperator.EQ,""+city);
						QueryExpression myQEDistrict = new QueryExpression("cassandra_district",IndexOperator.EQ,""+district);
						qe.add(myQEDay);
						qe.add(myQETime);
						qe.add(myQEProvince);
						qe.add(myQECity);
						qe.add(myQEDistrict);
						qes.add(qe);
					}
				}
			}
			return getSpeedtestData(type,orgs,qes,"",-1,true);
		}
		return null;
	}
	
	/**
	 * 根据省份获取测试数据
	 * @author Ocean
	 * @param province
	 * @param city
	 * @param district
	 * @param orgs
	 * @param testTypes
	 */
	public Map<String, Object> getSpeedtestAnalyzeDataByLocationProvince(String province, JSONArray orgs, Date startDate, Date endDate) throws Exception{
		List<ArrayList<QueryExpression>> qes = new ArrayList<ArrayList<QueryExpression>>();
		long startTime = startDate.getTime();
		long endTime = endDate.getTime();
		SimpleDateFormat dateFormat = new SimpleDateFormat(
				"yyyyMMdd");
		String startStr = dateFormat.format(startDate);
		String endStr = dateFormat.format(endDate);
		
		List<String> days = this.parseDays(startDate, endDate);
		if(days!=null && days.size()!=0){
			if(days.size()==1){
				ArrayList<QueryExpression> qe = new ArrayList<QueryExpression>();
				QueryExpression myQEDay = new QueryExpression("year_month_day",IndexOperator.EQ,startStr);
				QueryExpression myQETimeStart = new QueryExpression("data_time",IndexOperator.GTE,""+startTime);
				QueryExpression myQETimeEnd = new QueryExpression("data_time",IndexOperator.LTE,""+endTime);
				QueryExpression myQEProvince = new QueryExpression("cassandra_province",IndexOperator.EQ,""+province);
				qe.add(myQEDay);
				qe.add(myQETimeStart);
				qe.add(myQETimeEnd);
				qe.add(myQEProvince);
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
			return getSpeedtestAnalyzeData(orgs,qes,"",-1);
		}
		return null;
	}

	/**
	 * 根据省份获取测试数据
	 * @author Ocean
	 * @param province
	 * @param city
	 * @param district
	 * @param orgs
	 * @param testTypes
	 */
	public Map<String, Object> getSpeedtestAnalyzeDataByLocationCity(String province, String city, JSONArray orgs, Date startDate, Date endDate) throws Exception{
		List<ArrayList<QueryExpression>> qes = new ArrayList<ArrayList<QueryExpression>>();
		long startTime = startDate.getTime();
		long endTime = endDate.getTime();
		SimpleDateFormat dateFormat = new SimpleDateFormat(
				"yyyyMMdd");
		String startStr = dateFormat.format(startDate);
		String endStr = dateFormat.format(endDate);
		
		List<String> days = this.parseDays(startDate, endDate);
		if(days!=null && days.size()!=0){
			if(days.size()==1){
				ArrayList<QueryExpression> qe = new ArrayList<QueryExpression>();
				QueryExpression myQEDay = new QueryExpression("year_month_day",IndexOperator.EQ,startStr);
				QueryExpression myQETimeStart = new QueryExpression("data_time",IndexOperator.GTE,""+startTime);
				QueryExpression myQETimeEnd = new QueryExpression("data_time",IndexOperator.LTE,""+endTime);
				QueryExpression myQEProvince = new QueryExpression("cassandra_province",IndexOperator.EQ,""+province);
				QueryExpression myQECity = new QueryExpression("cassandra_city",IndexOperator.EQ,""+city);
				qe.add(myQEDay);
				qe.add(myQETimeStart);
				qe.add(myQETimeEnd);
				qe.add(myQEProvince);
				qe.add(myQECity);
				qes.add(qe);
			}else if(days.size()==2){
				ArrayList<QueryExpression> qe = new ArrayList<QueryExpression>();
				QueryExpression myQEDay = new QueryExpression("year_month_day",IndexOperator.EQ,startStr);
				QueryExpression myQETime = new QueryExpression("data_time",IndexOperator.GTE,""+startTime);
				QueryExpression myQEProvince = new QueryExpression("cassandra_province",IndexOperator.EQ,""+province);
				QueryExpression myQECity = new QueryExpression("cassandra_city",IndexOperator.EQ,""+city);
				qe.add(myQEDay);
				qe.add(myQETime);
				qe.add(myQEProvince);
				qe.add(myQECity);
				qes.add(qe);
				
				ArrayList<QueryExpression> qe1 = new ArrayList<QueryExpression>();
				QueryExpression myQEDay1 = new QueryExpression("year_month_day",IndexOperator.EQ,endStr);
				QueryExpression myQETime1 = new QueryExpression("data_time",IndexOperator.LTE,""+endTime);
				QueryExpression myQEProvince1 = new QueryExpression("cassandra_province",IndexOperator.EQ,""+province);
				QueryExpression myQECity1 = new QueryExpression("cassandra_city",IndexOperator.EQ,""+city);
				qe.add(myQEDay1);
				qe.add(myQETime1);
				qe.add(myQEProvince1);
				qe.add(myQECity1);
				qes.add(qe1);
			}else{
				for(int i=0; i<days.size(); i++){
					if(days.get(i).equals(startStr)){
						ArrayList<QueryExpression> qe = new ArrayList<QueryExpression>();
						QueryExpression myQEDay = new QueryExpression("year_month_day",IndexOperator.EQ,startStr);
						QueryExpression myQETime = new QueryExpression("data_time",IndexOperator.GTE,""+startTime);
						QueryExpression myQEProvince = new QueryExpression("cassandra_province",IndexOperator.EQ,""+province);
						QueryExpression myQECity = new QueryExpression("cassandra_city",IndexOperator.EQ,""+city);
						qe.add(myQEDay);
						qe.add(myQETime);
						qe.add(myQEProvince);
						qe.add(myQECity);
						qes.add(qe);
					}else if(days.get(i).equals(endStr)){
						ArrayList<QueryExpression> qe = new ArrayList<QueryExpression>();
						QueryExpression myQEDay = new QueryExpression("year_month_day",IndexOperator.EQ,endStr);
						QueryExpression myQETime = new QueryExpression("data_time",IndexOperator.LTE,""+endTime);
						QueryExpression myQEProvince = new QueryExpression("cassandra_province",IndexOperator.EQ,""+province);
						QueryExpression myQECity = new QueryExpression("cassandra_city",IndexOperator.EQ,""+city);
						qe.add(myQEDay);
						qe.add(myQETime);
						qe.add(myQEProvince);
						qe.add(myQECity);
						qes.add(qe);
					}else{
						long timeTemp = (dateFormat.parse(days.get(i))).getTime();
						ArrayList<QueryExpression> qe = new ArrayList<QueryExpression>();
						QueryExpression myQEDay = new QueryExpression("year_month_day",IndexOperator.EQ,days.get(i));
						QueryExpression myQETime = new QueryExpression("data_time",IndexOperator.GTE,""+timeTemp);
						QueryExpression myQEProvince = new QueryExpression("cassandra_province",IndexOperator.EQ,""+province);
						QueryExpression myQECity = new QueryExpression("cassandra_city",IndexOperator.EQ,""+city);
						qe.add(myQEDay);
						qe.add(myQETime);
						qe.add(myQEProvince);
						qe.add(myQECity);
						qes.add(qe);
					}
				}
			}
			return getSpeedtestAnalyzeData(orgs,qes,"",-1);
		}
		return null;
	}
	
	/**
	 * 根据省份获取测试数据
	 * @author Ocean
	 * @param province
	 * @param city
	 * @param district
	 * @param orgs
	 * @param testTypes
	 */
	public Map<String, Object> getSpeedtestAnalyzeDataByLocationDistrict(String province, String city, String district, JSONArray orgs, Date startDate, Date endDate) throws Exception{
		List<ArrayList<QueryExpression>> qes = new ArrayList<ArrayList<QueryExpression>>();
		long startTime = startDate.getTime();
		long endTime = endDate.getTime();
		SimpleDateFormat dateFormat = new SimpleDateFormat(
				"yyyyMMdd");
		String startStr = dateFormat.format(startDate);
		String endStr = dateFormat.format(endDate);
		
		List<String> days = this.parseDays(startDate, endDate);
		if(days!=null && days.size()!=0){
			if(days.size()==1){
				ArrayList<QueryExpression> qe = new ArrayList<QueryExpression>();
				QueryExpression myQEDay = new QueryExpression("year_month_day",IndexOperator.EQ,startStr);
				QueryExpression myQETimeStart = new QueryExpression("data_time",IndexOperator.GTE,""+startTime);
				QueryExpression myQETimeEnd = new QueryExpression("data_time",IndexOperator.LTE,""+endTime);
				QueryExpression myQEProvince = new QueryExpression("cassandra_province",IndexOperator.EQ,""+province);
				QueryExpression myQECity = new QueryExpression("cassandra_city",IndexOperator.EQ,""+city);
				QueryExpression myQEDistrict = new QueryExpression("cassandra_district",IndexOperator.EQ,""+district);
				qe.add(myQEDay);
				qe.add(myQETimeStart);
				qe.add(myQETimeEnd);
				qe.add(myQEProvince);
				qe.add(myQECity);
				qe.add(myQEDistrict);
				qes.add(qe);
			}else if(days.size()==2){
				ArrayList<QueryExpression> qe = new ArrayList<QueryExpression>();
				QueryExpression myQEDay = new QueryExpression("year_month_day",IndexOperator.EQ,startStr);
				QueryExpression myQETime = new QueryExpression("data_time",IndexOperator.GTE,""+startTime);
				QueryExpression myQEProvince = new QueryExpression("cassandra_province",IndexOperator.EQ,""+province);
				QueryExpression myQECity = new QueryExpression("cassandra_city",IndexOperator.EQ,""+city);
				QueryExpression myQEDistrict = new QueryExpression("cassandra_district",IndexOperator.EQ,""+district);
				qe.add(myQEDay);
				qe.add(myQETime);
				qe.add(myQEProvince);
				qe.add(myQECity);
				qe.add(myQEDistrict);
				qes.add(qe);
				
				ArrayList<QueryExpression> qe1 = new ArrayList<QueryExpression>();
				QueryExpression myQEDay1 = new QueryExpression("year_month_day",IndexOperator.EQ,endStr);
				QueryExpression myQETime1 = new QueryExpression("data_time",IndexOperator.LTE,""+endTime);
				QueryExpression myQEProvince1 = new QueryExpression("cassandra_province",IndexOperator.EQ,""+province);
				QueryExpression myQECity1 = new QueryExpression("cassandra_city",IndexOperator.EQ,""+city);
				QueryExpression myQEDistrict1 = new QueryExpression("cassandra_district",IndexOperator.EQ,""+district);
				qe.add(myQEDay1);
				qe.add(myQETime1);
				qe.add(myQEProvince1);
				qe.add(myQECity1);
				qe.add(myQEDistrict1);
				qes.add(qe1);
			}else{
				for(int i=0; i<days.size(); i++){
					if(days.get(i).equals(startStr)){
						ArrayList<QueryExpression> qe = new ArrayList<QueryExpression>();
						QueryExpression myQEDay = new QueryExpression("year_month_day",IndexOperator.EQ,startStr);
						QueryExpression myQETime = new QueryExpression("data_time",IndexOperator.GTE,""+startTime);
						QueryExpression myQEProvince = new QueryExpression("cassandra_province",IndexOperator.EQ,""+province);
						QueryExpression myQECity = new QueryExpression("cassandra_city",IndexOperator.EQ,""+city);
						QueryExpression myQEDistrict = new QueryExpression("cassandra_district",IndexOperator.EQ,""+district);
						qe.add(myQEDay);
						qe.add(myQETime);
						qe.add(myQEProvince);
						qe.add(myQECity);
						qe.add(myQEDistrict);
						qes.add(qe);
					}else if(days.get(i).equals(endStr)){
						ArrayList<QueryExpression> qe = new ArrayList<QueryExpression>();
						QueryExpression myQEDay = new QueryExpression("year_month_day",IndexOperator.EQ,endStr);
						QueryExpression myQETime = new QueryExpression("data_time",IndexOperator.LTE,""+endTime);
						QueryExpression myQEProvince = new QueryExpression("cassandra_province",IndexOperator.EQ,""+province);
						QueryExpression myQECity = new QueryExpression("cassandra_city",IndexOperator.EQ,""+city);
						QueryExpression myQEDistrict = new QueryExpression("cassandra_district",IndexOperator.EQ,""+district);
						qe.add(myQEDay);
						qe.add(myQETime);
						qe.add(myQEProvince);
						qe.add(myQECity);
						qe.add(myQEDistrict);
						qes.add(qe);
					}else{
						long timeTemp = (dateFormat.parse(days.get(i))).getTime();
						ArrayList<QueryExpression> qe = new ArrayList<QueryExpression>();
						QueryExpression myQEDay = new QueryExpression("year_month_day",IndexOperator.EQ,days.get(i));
						QueryExpression myQETime = new QueryExpression("data_time",IndexOperator.GTE,""+timeTemp);
						QueryExpression myQEProvince = new QueryExpression("cassandra_province",IndexOperator.EQ,""+province);
						QueryExpression myQECity = new QueryExpression("cassandra_city",IndexOperator.EQ,""+city);
						QueryExpression myQEDistrict = new QueryExpression("cassandra_district",IndexOperator.EQ,""+district);
						qe.add(myQEDay);
						qe.add(myQETime);
						qe.add(myQEProvince);
						qe.add(myQECity);
						qe.add(myQEDistrict);
						qes.add(qe);
					}
				}
			}
			return getSpeedtestAnalyzeData(orgs,qes,"",-1);
		}
		return null;
	}

	/**
	 * 根据省份获取测试数据
	 * @author Ocean
	 * @param province
	 * @param city
	 * @param district
	 * @param orgs
	 * @param testTypes
	 */
	public Map<String, Object> getHttpTestDataByLocationProvince(String type, String province, JSONArray orgs, Date startDate, Date endDate) throws Exception{
		List<ArrayList<QueryExpression>> qes = new ArrayList<ArrayList<QueryExpression>>();
		long startTime = startDate.getTime();
		long endTime = endDate.getTime();
		SimpleDateFormat dateFormat = new SimpleDateFormat(
				"yyyyMMdd");
		String startStr = dateFormat.format(startDate);
		String endStr = dateFormat.format(endDate);
		
		List<String> days = this.parseDays(startDate, endDate);
		if(days!=null && days.size()!=0){
			if(days.size()==1){
				ArrayList<QueryExpression> qe = new ArrayList<QueryExpression>();
				QueryExpression myQEDay = new QueryExpression("year_month_day",IndexOperator.EQ,startStr);
				QueryExpression myQETimeStart = new QueryExpression("data_time",IndexOperator.GTE,""+startTime);
				QueryExpression myQETimeEnd = new QueryExpression("data_time",IndexOperator.LTE,""+endTime);
				QueryExpression myQEProvince = new QueryExpression("cassandra_province",IndexOperator.EQ,""+province);
				qe.add(myQEDay);
				qe.add(myQETimeStart);
				qe.add(myQETimeEnd);
				qe.add(myQEProvince);
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
			return getHttpTestData(type,orgs,qes,"",-1,true);
		}
		return null;
	}

	/**
	 * 根据省份获取测试数据
	 * @author Ocean
	 * @param province
	 * @param city
	 * @param district
	 * @param orgs
	 * @param testTypes
	 */
	public Map<String, Object> getHttpTestDataByLocationCity(String type, String province, String city, JSONArray orgs, Date startDate, Date endDate) throws Exception{
		List<ArrayList<QueryExpression>> qes = new ArrayList<ArrayList<QueryExpression>>();
		long startTime = startDate.getTime();
		long endTime = endDate.getTime();
		SimpleDateFormat dateFormat = new SimpleDateFormat(
				"yyyyMMdd");
		String startStr = dateFormat.format(startDate);
		String endStr = dateFormat.format(endDate);
		
		List<String> days = this.parseDays(startDate, endDate);
		if(days!=null && days.size()!=0){
			if(days.size()==1){
				ArrayList<QueryExpression> qe = new ArrayList<QueryExpression>();
				QueryExpression myQEDay = new QueryExpression("year_month_day",IndexOperator.EQ,startStr);
				QueryExpression myQETimeStart = new QueryExpression("data_time",IndexOperator.GTE,""+startTime);
				QueryExpression myQETimeEnd = new QueryExpression("data_time",IndexOperator.LTE,""+endTime);
				QueryExpression myQEProvince = new QueryExpression("cassandra_province",IndexOperator.EQ,""+province);
				QueryExpression myQECity = new QueryExpression("cassandra_city",IndexOperator.EQ,""+city);
				qe.add(myQEDay);
				qe.add(myQETimeStart);
				qe.add(myQETimeEnd);
				qe.add(myQEProvince);
				qe.add(myQECity);
				qes.add(qe);
			}else if(days.size()==2){
				ArrayList<QueryExpression> qe = new ArrayList<QueryExpression>();
				QueryExpression myQEDay = new QueryExpression("year_month_day",IndexOperator.EQ,startStr);
				QueryExpression myQETime = new QueryExpression("data_time",IndexOperator.GTE,""+startTime);
				QueryExpression myQEProvince = new QueryExpression("cassandra_province",IndexOperator.EQ,""+province);
				QueryExpression myQECity = new QueryExpression("cassandra_city",IndexOperator.EQ,""+city);
				qe.add(myQEDay);
				qe.add(myQETime);
				qe.add(myQEProvince);
				qe.add(myQECity);
				qes.add(qe);
				
				ArrayList<QueryExpression> qe1 = new ArrayList<QueryExpression>();
				QueryExpression myQEDay1 = new QueryExpression("year_month_day",IndexOperator.EQ,endStr);
				QueryExpression myQETime1 = new QueryExpression("data_time",IndexOperator.LTE,""+endTime);
				QueryExpression myQEProvince1 = new QueryExpression("cassandra_province",IndexOperator.EQ,""+province);
				QueryExpression myQECity1 = new QueryExpression("cassandra_city",IndexOperator.EQ,""+city);
				qe.add(myQEDay1);
				qe.add(myQETime1);
				qe.add(myQEProvince1);
				qe.add(myQECity1);
				qes.add(qe1);
			}else{
				for(int i=0; i<days.size(); i++){
					if(days.get(i).equals(startStr)){
						ArrayList<QueryExpression> qe = new ArrayList<QueryExpression>();
						QueryExpression myQEDay = new QueryExpression("year_month_day",IndexOperator.EQ,startStr);
						QueryExpression myQETime = new QueryExpression("data_time",IndexOperator.GTE,""+startTime);
						QueryExpression myQEProvince = new QueryExpression("cassandra_province",IndexOperator.EQ,""+province);
						QueryExpression myQECity = new QueryExpression("cassandra_city",IndexOperator.EQ,""+city);
						qe.add(myQEDay);
						qe.add(myQETime);
						qe.add(myQEProvince);
						qe.add(myQECity);
						qes.add(qe);
					}else if(days.get(i).equals(endStr)){
						ArrayList<QueryExpression> qe = new ArrayList<QueryExpression>();
						QueryExpression myQEDay = new QueryExpression("year_month_day",IndexOperator.EQ,endStr);
						QueryExpression myQETime = new QueryExpression("data_time",IndexOperator.LTE,""+endTime);
						QueryExpression myQEProvince = new QueryExpression("cassandra_province",IndexOperator.EQ,""+province);
						QueryExpression myQECity = new QueryExpression("cassandra_city",IndexOperator.EQ,""+city);
						qe.add(myQEDay);
						qe.add(myQETime);
						qe.add(myQEProvince);
						qe.add(myQECity);
						qes.add(qe);
					}else{
						long timeTemp = (dateFormat.parse(days.get(i))).getTime();
						ArrayList<QueryExpression> qe = new ArrayList<QueryExpression>();
						QueryExpression myQEDay = new QueryExpression("year_month_day",IndexOperator.EQ,days.get(i));
						QueryExpression myQETime = new QueryExpression("data_time",IndexOperator.GTE,""+timeTemp);
						QueryExpression myQEProvince = new QueryExpression("cassandra_province",IndexOperator.EQ,""+province);
						QueryExpression myQECity = new QueryExpression("cassandra_city",IndexOperator.EQ,""+city);
						qe.add(myQEDay);
						qe.add(myQETime);
						qe.add(myQEProvince);
						qe.add(myQECity);
						qes.add(qe);
					}
				}
			}
			return getHttpTestData(type,orgs,qes,"",-1,true);
		}
		return null;
	}
	
	/**
	 * 根据省份获取测试数据
	 * @author Ocean
	 * @param province
	 * @param city
	 * @param district
	 * @param orgs
	 * @param testTypes
	 */
	public Map<String, Object> getHttpTestDataByLocationDistrict(String type, String province, String city, String district, JSONArray orgs, Date startDate, Date endDate) throws Exception{
		List<ArrayList<QueryExpression>> qes = new ArrayList<ArrayList<QueryExpression>>();
		long startTime = startDate.getTime();
		long endTime = endDate.getTime();
		SimpleDateFormat dateFormat = new SimpleDateFormat(
				"yyyyMMdd");
		String startStr = dateFormat.format(startDate);
		String endStr = dateFormat.format(endDate);
		
		List<String> days = this.parseDays(startDate, endDate);
		if(days!=null && days.size()!=0){
			if(days.size()==1){
				ArrayList<QueryExpression> qe = new ArrayList<QueryExpression>();
				QueryExpression myQEDay = new QueryExpression("year_month_day",IndexOperator.EQ,startStr);
				QueryExpression myQETimeStart = new QueryExpression("data_time",IndexOperator.GTE,""+startTime);
				QueryExpression myQETimeEnd = new QueryExpression("data_time",IndexOperator.LTE,""+endTime);
				QueryExpression myQEProvince = new QueryExpression("cassandra_province",IndexOperator.EQ,""+province);
				QueryExpression myQECity = new QueryExpression("cassandra_city",IndexOperator.EQ,""+city);
				QueryExpression myQEDistrict = new QueryExpression("cassandra_district",IndexOperator.EQ,""+district);
				qe.add(myQEDay);
				qe.add(myQETimeStart);
				qe.add(myQETimeEnd);
				qe.add(myQEProvince);
				qe.add(myQECity);
				qe.add(myQEDistrict);
				qes.add(qe);
			}else if(days.size()==2){
				ArrayList<QueryExpression> qe = new ArrayList<QueryExpression>();
				QueryExpression myQEDay = new QueryExpression("year_month_day",IndexOperator.EQ,startStr);
				QueryExpression myQETime = new QueryExpression("data_time",IndexOperator.GTE,""+startTime);
				QueryExpression myQEProvince = new QueryExpression("cassandra_province",IndexOperator.EQ,""+province);
				QueryExpression myQECity = new QueryExpression("cassandra_city",IndexOperator.EQ,""+city);
				QueryExpression myQEDistrict = new QueryExpression("cassandra_district",IndexOperator.EQ,""+district);
				qe.add(myQEDay);
				qe.add(myQETime);
				qe.add(myQEProvince);
				qe.add(myQECity);
				qe.add(myQEDistrict);
				qes.add(qe);
				
				ArrayList<QueryExpression> qe1 = new ArrayList<QueryExpression>();
				QueryExpression myQEDay1 = new QueryExpression("year_month_day",IndexOperator.EQ,endStr);
				QueryExpression myQETime1 = new QueryExpression("data_time",IndexOperator.LTE,""+endTime);
				QueryExpression myQEProvince1 = new QueryExpression("cassandra_province",IndexOperator.EQ,""+province);
				QueryExpression myQECity1 = new QueryExpression("cassandra_city",IndexOperator.EQ,""+city);
				QueryExpression myQEDistrict1 = new QueryExpression("cassandra_district",IndexOperator.EQ,""+district);
				qe.add(myQEDay1);
				qe.add(myQETime1);
				qe.add(myQEProvince1);
				qe.add(myQECity1);
				qe.add(myQEDistrict1);
				qes.add(qe1);
			}else{
				for(int i=0; i<days.size(); i++){
					if(days.get(i).equals(startStr)){
						ArrayList<QueryExpression> qe = new ArrayList<QueryExpression>();
						QueryExpression myQEDay = new QueryExpression("year_month_day",IndexOperator.EQ,startStr);
						QueryExpression myQETime = new QueryExpression("data_time",IndexOperator.GTE,""+startTime);
						QueryExpression myQEProvince = new QueryExpression("cassandra_province",IndexOperator.EQ,""+province);
						QueryExpression myQECity = new QueryExpression("cassandra_city",IndexOperator.EQ,""+city);
						QueryExpression myQEDistrict = new QueryExpression("cassandra_district",IndexOperator.EQ,""+district);
						qe.add(myQEDay);
						qe.add(myQETime);
						qe.add(myQEProvince);
						qe.add(myQECity);
						qe.add(myQEDistrict);
						qes.add(qe);
					}else if(days.get(i).equals(endStr)){
						ArrayList<QueryExpression> qe = new ArrayList<QueryExpression>();
						QueryExpression myQEDay = new QueryExpression("year_month_day",IndexOperator.EQ,endStr);
						QueryExpression myQETime = new QueryExpression("data_time",IndexOperator.LTE,""+endTime);
						QueryExpression myQEProvince = new QueryExpression("cassandra_province",IndexOperator.EQ,""+province);
						QueryExpression myQECity = new QueryExpression("cassandra_city",IndexOperator.EQ,""+city);
						QueryExpression myQEDistrict = new QueryExpression("cassandra_district",IndexOperator.EQ,""+district);
						qe.add(myQEDay);
						qe.add(myQETime);
						qe.add(myQEProvince);
						qe.add(myQECity);
						qe.add(myQEDistrict);
						qes.add(qe);
					}else{
						long timeTemp = (dateFormat.parse(days.get(i))).getTime();
						ArrayList<QueryExpression> qe = new ArrayList<QueryExpression>();
						QueryExpression myQEDay = new QueryExpression("year_month_day",IndexOperator.EQ,days.get(i));
						QueryExpression myQETime = new QueryExpression("data_time",IndexOperator.GTE,""+timeTemp);
						QueryExpression myQEProvince = new QueryExpression("cassandra_province",IndexOperator.EQ,""+province);
						QueryExpression myQECity = new QueryExpression("cassandra_city",IndexOperator.EQ,""+city);
						QueryExpression myQEDistrict = new QueryExpression("cassandra_district",IndexOperator.EQ,""+district);
						qe.add(myQEDay);
						qe.add(myQETime);
						qe.add(myQEProvince);
						qe.add(myQECity);
						qe.add(myQEDistrict);
						qes.add(qe);
					}
				}
			}
			return getHttpTestData(type,orgs,qes,"",-1,true);
		}
		return null;
	}
	
	/**
	 * 根据省份获取测试数据
	 * @author Ocean
	 * @param province
	 * @param city
	 * @param district
	 * @param orgs
	 * @param testTypes
	 */
	public Map<String, Object> getBrowseTestDataByLocationProvince(String province, JSONArray orgs, Date startDate, Date endDate) throws Exception{
		List<ArrayList<QueryExpression>> qes = new ArrayList<ArrayList<QueryExpression>>();
		long startTime = startDate.getTime();
		long endTime = endDate.getTime();
		SimpleDateFormat dateFormat = new SimpleDateFormat(
				"yyyyMMdd");
		String startStr = dateFormat.format(startDate);
		String endStr = dateFormat.format(endDate);
		
		List<String> days = this.parseDays(startDate, endDate);
		if(days!=null && days.size()!=0){
			if(days.size()==1){
				ArrayList<QueryExpression> qe = new ArrayList<QueryExpression>();
				QueryExpression myQEDay = new QueryExpression("year_month_day",IndexOperator.EQ,startStr);
				QueryExpression myQETimeStart = new QueryExpression("data_time",IndexOperator.GTE,""+startTime);
				QueryExpression myQETimeEnd = new QueryExpression("data_time",IndexOperator.LTE,""+endTime);
				QueryExpression myQEProvince = new QueryExpression("cassandra_province",IndexOperator.EQ,""+province);
				qe.add(myQEDay);
				qe.add(myQETimeStart);
				qe.add(myQETimeEnd);
				qe.add(myQEProvince);
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
			return getBrowseTestData(orgs,qes,"",-1,true);
		}
		return null;
	}

	/**
	 * 根据省份获取测试数据
	 * @author Ocean
	 * @param province
	 * @param city
	 * @param district
	 * @param orgs
	 * @param testTypes
	 */
	public Map<String, Object> getBrowseTestDataByLocationCity(String province, String city, JSONArray orgs, Date startDate, Date endDate) throws Exception{
		List<ArrayList<QueryExpression>> qes = new ArrayList<ArrayList<QueryExpression>>();
		long startTime = startDate.getTime();
		long endTime = endDate.getTime();
		SimpleDateFormat dateFormat = new SimpleDateFormat(
				"yyyyMMdd");
		String startStr = dateFormat.format(startDate);
		String endStr = dateFormat.format(endDate);
		
		List<String> days = this.parseDays(startDate, endDate);
		if(days!=null && days.size()!=0){
			if(days.size()==1){
				ArrayList<QueryExpression> qe = new ArrayList<QueryExpression>();
				QueryExpression myQEDay = new QueryExpression("year_month_day",IndexOperator.EQ,startStr);
				QueryExpression myQETimeStart = new QueryExpression("data_time",IndexOperator.GTE,""+startTime);
				QueryExpression myQETimeEnd = new QueryExpression("data_time",IndexOperator.LTE,""+endTime);
				QueryExpression myQEProvince = new QueryExpression("cassandra_province",IndexOperator.EQ,""+province);
				QueryExpression myQECity = new QueryExpression("cassandra_city",IndexOperator.EQ,""+city);
				qe.add(myQEDay);
				qe.add(myQETimeStart);
				qe.add(myQETimeEnd);
				qe.add(myQEProvince);
				qe.add(myQECity);
				qes.add(qe);
			}else if(days.size()==2){
				ArrayList<QueryExpression> qe = new ArrayList<QueryExpression>();
				QueryExpression myQEDay = new QueryExpression("year_month_day",IndexOperator.EQ,startStr);
				QueryExpression myQETime = new QueryExpression("data_time",IndexOperator.GTE,""+startTime);
				QueryExpression myQEProvince = new QueryExpression("cassandra_province",IndexOperator.EQ,""+province);
				QueryExpression myQECity = new QueryExpression("cassandra_city",IndexOperator.EQ,""+city);
				qe.add(myQEDay);
				qe.add(myQETime);
				qe.add(myQEProvince);
				qe.add(myQECity);
				qes.add(qe);
				
				ArrayList<QueryExpression> qe1 = new ArrayList<QueryExpression>();
				QueryExpression myQEDay1 = new QueryExpression("year_month_day",IndexOperator.EQ,endStr);
				QueryExpression myQETime1 = new QueryExpression("data_time",IndexOperator.LTE,""+endTime);
				QueryExpression myQEProvince1 = new QueryExpression("cassandra_province",IndexOperator.EQ,""+province);
				QueryExpression myQECity1 = new QueryExpression("cassandra_city",IndexOperator.EQ,""+city);
				qe.add(myQEDay1);
				qe.add(myQETime1);
				qe.add(myQEProvince1);
				qe.add(myQECity1);
				qes.add(qe1);
			}else{
				for(int i=0; i<days.size(); i++){
					if(days.get(i).equals(startStr)){
						ArrayList<QueryExpression> qe = new ArrayList<QueryExpression>();
						QueryExpression myQEDay = new QueryExpression("year_month_day",IndexOperator.EQ,startStr);
						QueryExpression myQETime = new QueryExpression("data_time",IndexOperator.GTE,""+startTime);
						QueryExpression myQEProvince = new QueryExpression("cassandra_province",IndexOperator.EQ,""+province);
						QueryExpression myQECity = new QueryExpression("cassandra_city",IndexOperator.EQ,""+city);
						qe.add(myQEDay);
						qe.add(myQETime);
						qe.add(myQEProvince);
						qe.add(myQECity);
						qes.add(qe);
					}else if(days.get(i).equals(endStr)){
						ArrayList<QueryExpression> qe = new ArrayList<QueryExpression>();
						QueryExpression myQEDay = new QueryExpression("year_month_day",IndexOperator.EQ,endStr);
						QueryExpression myQETime = new QueryExpression("data_time",IndexOperator.LTE,""+endTime);
						QueryExpression myQEProvince = new QueryExpression("cassandra_province",IndexOperator.EQ,""+province);
						QueryExpression myQECity = new QueryExpression("cassandra_city",IndexOperator.EQ,""+city);
						qe.add(myQEDay);
						qe.add(myQETime);
						qe.add(myQEProvince);
						qe.add(myQECity);
						qes.add(qe);
					}else{
						long timeTemp = (dateFormat.parse(days.get(i))).getTime();
						ArrayList<QueryExpression> qe = new ArrayList<QueryExpression>();
						QueryExpression myQEDay = new QueryExpression("year_month_day",IndexOperator.EQ,days.get(i));
						QueryExpression myQETime = new QueryExpression("data_time",IndexOperator.GTE,""+timeTemp);
						QueryExpression myQEProvince = new QueryExpression("cassandra_province",IndexOperator.EQ,""+province);
						QueryExpression myQECity = new QueryExpression("cassandra_city",IndexOperator.EQ,""+city);
						qe.add(myQEDay);
						qe.add(myQETime);
						qe.add(myQEProvince);
						qe.add(myQECity);
						qes.add(qe);
					}
				}
			}
			return getBrowseTestData(orgs,qes,"",-1,true);
		}
		return null;
	}
	
	/**
	 * 根据省份获取测试数据
	 * @author Ocean
	 * @param province
	 * @param city
	 * @param district
	 * @param orgs
	 * @param testTypes
	 */
	public Map<String, Object> getBrowseTestDataByLocationDistrict(String province, String city, String district, JSONArray orgs, Date startDate, Date endDate) throws Exception{
		List<ArrayList<QueryExpression>> qes = new ArrayList<ArrayList<QueryExpression>>();
		long startTime = startDate.getTime();
		long endTime = endDate.getTime();
		SimpleDateFormat dateFormat = new SimpleDateFormat(
				"yyyyMMdd");
		String startStr = dateFormat.format(startDate);
		String endStr = dateFormat.format(endDate);
		
		List<String> days = this.parseDays(startDate, endDate);
		if(days!=null && days.size()!=0){
			if(days.size()==1){
				ArrayList<QueryExpression> qe = new ArrayList<QueryExpression>();
				QueryExpression myQEDay = new QueryExpression("year_month_day",IndexOperator.EQ,startStr);
				QueryExpression myQETimeStart = new QueryExpression("data_time",IndexOperator.GTE,""+startTime);
				QueryExpression myQETimeEnd = new QueryExpression("data_time",IndexOperator.LTE,""+endTime);
				QueryExpression myQEProvince = new QueryExpression("cassandra_province",IndexOperator.EQ,""+province);
				QueryExpression myQECity = new QueryExpression("cassandra_city",IndexOperator.EQ,""+city);
				QueryExpression myQEDistrict = new QueryExpression("cassandra_district",IndexOperator.EQ,""+district);
				qe.add(myQEDay);
				qe.add(myQETimeStart);
				qe.add(myQETimeEnd);
				qe.add(myQEProvince);
				qe.add(myQECity);
				qe.add(myQEDistrict);
				qes.add(qe);
			}else if(days.size()==2){
				ArrayList<QueryExpression> qe = new ArrayList<QueryExpression>();
				QueryExpression myQEDay = new QueryExpression("year_month_day",IndexOperator.EQ,startStr);
				QueryExpression myQETime = new QueryExpression("data_time",IndexOperator.GTE,""+startTime);
				QueryExpression myQEProvince = new QueryExpression("cassandra_province",IndexOperator.EQ,""+province);
				QueryExpression myQECity = new QueryExpression("cassandra_city",IndexOperator.EQ,""+city);
				QueryExpression myQEDistrict = new QueryExpression("cassandra_district",IndexOperator.EQ,""+district);
				qe.add(myQEDay);
				qe.add(myQETime);
				qe.add(myQEProvince);
				qe.add(myQECity);
				qe.add(myQEDistrict);
				qes.add(qe);
				
				ArrayList<QueryExpression> qe1 = new ArrayList<QueryExpression>();
				QueryExpression myQEDay1 = new QueryExpression("year_month_day",IndexOperator.EQ,endStr);
				QueryExpression myQETime1 = new QueryExpression("data_time",IndexOperator.LTE,""+endTime);
				QueryExpression myQEProvince1 = new QueryExpression("cassandra_province",IndexOperator.EQ,""+province);
				QueryExpression myQECity1 = new QueryExpression("cassandra_city",IndexOperator.EQ,""+city);
				QueryExpression myQEDistrict1 = new QueryExpression("cassandra_district",IndexOperator.EQ,""+district);
				qe.add(myQEDay1);
				qe.add(myQETime1);
				qe.add(myQEProvince1);
				qe.add(myQECity1);
				qe.add(myQEDistrict1);
				qes.add(qe1);
			}else{
				for(int i=0; i<days.size(); i++){
					if(days.get(i).equals(startStr)){
						ArrayList<QueryExpression> qe = new ArrayList<QueryExpression>();
						QueryExpression myQEDay = new QueryExpression("year_month_day",IndexOperator.EQ,startStr);
						QueryExpression myQETime = new QueryExpression("data_time",IndexOperator.GTE,""+startTime);
						QueryExpression myQEProvince = new QueryExpression("cassandra_province",IndexOperator.EQ,""+province);
						QueryExpression myQECity = new QueryExpression("cassandra_city",IndexOperator.EQ,""+city);
						QueryExpression myQEDistrict = new QueryExpression("cassandra_district",IndexOperator.EQ,""+district);
						qe.add(myQEDay);
						qe.add(myQETime);
						qe.add(myQEProvince);
						qe.add(myQECity);
						qe.add(myQEDistrict);
						qes.add(qe);
					}else if(days.get(i).equals(endStr)){
						ArrayList<QueryExpression> qe = new ArrayList<QueryExpression>();
						QueryExpression myQEDay = new QueryExpression("year_month_day",IndexOperator.EQ,endStr);
						QueryExpression myQETime = new QueryExpression("data_time",IndexOperator.LTE,""+endTime);
						QueryExpression myQEProvince = new QueryExpression("cassandra_province",IndexOperator.EQ,""+province);
						QueryExpression myQECity = new QueryExpression("cassandra_city",IndexOperator.EQ,""+city);
						QueryExpression myQEDistrict = new QueryExpression("cassandra_district",IndexOperator.EQ,""+district);
						qe.add(myQEDay);
						qe.add(myQETime);
						qe.add(myQEProvince);
						qe.add(myQECity);
						qe.add(myQEDistrict);
						qes.add(qe);
					}else{
						long timeTemp = (dateFormat.parse(days.get(i))).getTime();
						ArrayList<QueryExpression> qe = new ArrayList<QueryExpression>();
						QueryExpression myQEDay = new QueryExpression("year_month_day",IndexOperator.EQ,days.get(i));
						QueryExpression myQETime = new QueryExpression("data_time",IndexOperator.GTE,""+timeTemp);
						QueryExpression myQEProvince = new QueryExpression("cassandra_province",IndexOperator.EQ,""+province);
						QueryExpression myQECity = new QueryExpression("cassandra_city",IndexOperator.EQ,""+city);
						QueryExpression myQEDistrict = new QueryExpression("cassandra_district",IndexOperator.EQ,""+district);
						qe.add(myQEDay);
						qe.add(myQETime);
						qe.add(myQEProvince);
						qe.add(myQECity);
						qe.add(myQEDistrict);
						qes.add(qe);
					}
				}
			}
			return getBrowseTestData(orgs,qes,"",-1,true);
		}
		return null;
	}
	
	/**
	 * 根据省份获取测试数据
	 * @author Ocean
	 * @param province
	 * @param orgs
	 * @param testTypes
	 * @param startDate
	 * @param endDate
	 */
	public List<Map<String, String>> getGpsDataByLocationProvince(String province, JSONArray orgs, JSONArray testTypes, Date startDate, Date endDate) throws Exception{
		List<ArrayList<QueryExpression>> qes = new ArrayList<ArrayList<QueryExpression>>();
		long startTime = startDate.getTime();
		long endTime = endDate.getTime();
		SimpleDateFormat dateFormat = new SimpleDateFormat(
				"yyyyMMdd");
		String startStr = dateFormat.format(startDate);
		String endStr = dateFormat.format(endDate);
		
		List<String> days = this.parseDays(startDate, endDate);
		if(days!=null && days.size()!=0){
			if(days.size()==1){
				ArrayList<QueryExpression> qe = new ArrayList<QueryExpression>();
				QueryExpression myQEDay = new QueryExpression("year_month_day",IndexOperator.EQ,startStr);
				QueryExpression myQETimeStart = new QueryExpression("data_time",IndexOperator.GTE,""+startTime);
				QueryExpression myQETimeEnd = new QueryExpression("data_time",IndexOperator.LTE,""+endTime);
				QueryExpression myQEProvince = new QueryExpression("cassandra_province",IndexOperator.EQ,""+province);
				qe.add(myQEDay);
				qe.add(myQETimeStart);
				qe.add(myQETimeEnd);
				qe.add(myQEProvince);
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
			return getGpsData(orgs,testTypes,qes,"",-1);
		}
		return null;
	}
	
	/**
	 * 根据省份+城市获取测试数据
	 * @author Ocean
	 * @param province
	 * @param city
	 * @param orgs
	 * @param testTypes
	 * @param startDate
	 * @param endDate
	 */
	public List<Map<String, String>> getGpsDataByLocationCity(String province, String city, JSONArray orgs, JSONArray testTypes, Date startDate, Date endDate) throws Exception{
		List<ArrayList<QueryExpression>> qes = new ArrayList<ArrayList<QueryExpression>>();
		long startTime = startDate.getTime();
		long endTime = endDate.getTime();
		SimpleDateFormat dateFormat = new SimpleDateFormat(
				"yyyyMMdd");
		String startStr = dateFormat.format(startDate);
		String endStr = dateFormat.format(endDate);
		
		List<String> days = this.parseDays(startDate, endDate);
		if(days!=null && days.size()!=0){
			if(days.size()==1){
				ArrayList<QueryExpression> qe = new ArrayList<QueryExpression>();
				QueryExpression myQEDay = new QueryExpression("year_month_day",IndexOperator.EQ,startStr);
				QueryExpression myQETimeStart = new QueryExpression("data_time",IndexOperator.GTE,""+startTime);
				QueryExpression myQETimeEnd = new QueryExpression("data_time",IndexOperator.LTE,""+endTime);
				QueryExpression myQEProvince = new QueryExpression("cassandra_province",IndexOperator.EQ,""+province);
				QueryExpression myQECity = new QueryExpression("cassandra_city",IndexOperator.EQ,""+city);
				qe.add(myQEDay);
				qe.add(myQETimeStart);
				qe.add(myQETimeEnd);
				qe.add(myQEProvince);
				qe.add(myQECity);
				qes.add(qe);
			}else if(days.size()==2){
				ArrayList<QueryExpression> qe = new ArrayList<QueryExpression>();
				QueryExpression myQEDay = new QueryExpression("year_month_day",IndexOperator.EQ,startStr);
				QueryExpression myQETime = new QueryExpression("data_time",IndexOperator.GTE,""+startTime);
				QueryExpression myQEProvince = new QueryExpression("cassandra_province",IndexOperator.EQ,""+province);
				QueryExpression myQECity = new QueryExpression("cassandra_city",IndexOperator.EQ,""+city);
				qe.add(myQEDay);
				qe.add(myQETime);
				qe.add(myQEProvince);
				qe.add(myQECity);
				qes.add(qe);
				
				ArrayList<QueryExpression> qe1 = new ArrayList<QueryExpression>();
				QueryExpression myQEDay1 = new QueryExpression("year_month_day",IndexOperator.EQ,endStr);
				QueryExpression myQETime1 = new QueryExpression("data_time",IndexOperator.LTE,""+endTime);
				QueryExpression myQEProvince1 = new QueryExpression("cassandra_province",IndexOperator.EQ,""+province);
				QueryExpression myQECity1 = new QueryExpression("cassandra_city",IndexOperator.EQ,""+city);
				qe.add(myQEDay1);
				qe.add(myQETime1);
				qe.add(myQEProvince1);
				qe.add(myQECity1);
				qes.add(qe1);
			}else{
				for(int i=0; i<days.size(); i++){
					if(days.get(i).equals(startStr)){
						ArrayList<QueryExpression> qe = new ArrayList<QueryExpression>();
						QueryExpression myQEDay = new QueryExpression("year_month_day",IndexOperator.EQ,startStr);
						QueryExpression myQETime = new QueryExpression("data_time",IndexOperator.GTE,""+startTime);
						QueryExpression myQEProvince = new QueryExpression("cassandra_province",IndexOperator.EQ,""+province);
						QueryExpression myQECity = new QueryExpression("cassandra_city",IndexOperator.EQ,""+city);
						qe.add(myQEDay);
						qe.add(myQETime);
						qe.add(myQEProvince);
						qe.add(myQECity);
						qes.add(qe);
					}else if(days.get(i).equals(endStr)){
						ArrayList<QueryExpression> qe = new ArrayList<QueryExpression>();
						QueryExpression myQEDay = new QueryExpression("year_month_day",IndexOperator.EQ,endStr);
						QueryExpression myQETime = new QueryExpression("data_time",IndexOperator.LTE,""+endTime);
						QueryExpression myQEProvince = new QueryExpression("cassandra_province",IndexOperator.EQ,""+province);
						QueryExpression myQECity = new QueryExpression("cassandra_city",IndexOperator.EQ,""+city);
						qe.add(myQEDay);
						qe.add(myQETime);
						qe.add(myQEProvince);
						qe.add(myQECity);
						qes.add(qe);
					}else{
						long timeTemp = (dateFormat.parse(days.get(i))).getTime();
						ArrayList<QueryExpression> qe = new ArrayList<QueryExpression>();
						QueryExpression myQEDay = new QueryExpression("year_month_day",IndexOperator.EQ,days.get(i));
						QueryExpression myQETime = new QueryExpression("data_time",IndexOperator.GTE,""+timeTemp);
						QueryExpression myQEProvince = new QueryExpression("cassandra_province",IndexOperator.EQ,""+province);
						QueryExpression myQECity = new QueryExpression("cassandra_city",IndexOperator.EQ,""+city);
						qe.add(myQEDay);
						qe.add(myQETime);
						qe.add(myQEProvince);
						qe.add(myQECity);
						qes.add(qe);
					}
				}
			}
			return getGpsData(orgs,testTypes,qes,"",-1);
		}
		return null;
	}
	
	/**
	 * 根据省份+城市+地区获取测试数据
	 * @author Ocean
	 * @param province
	 * @param city
	 * @param district
	 * @param orgs
	 * @param testTypes
	 * @param startDate
	 * @param endDate
	 */
	public List<Map<String, String>> getGpsDataByLocationDistrict(String province, String city, String district, JSONArray orgs, JSONArray testTypes, Date startDate, Date endDate) throws Exception{
		List<ArrayList<QueryExpression>> qes = new ArrayList<ArrayList<QueryExpression>>();
		long startTime = startDate.getTime();
		long endTime = endDate.getTime();
		SimpleDateFormat dateFormat = new SimpleDateFormat(
				"yyyyMMdd");
		String startStr = dateFormat.format(startDate);
		String endStr = dateFormat.format(endDate);
		
		List<String> days = this.parseDays(startDate, endDate);
		if(days!=null && days.size()!=0){
			if(days.size()==1){
				ArrayList<QueryExpression> qe = new ArrayList<QueryExpression>();
				QueryExpression myQEDay = new QueryExpression("year_month_day",IndexOperator.EQ,startStr);
				QueryExpression myQETimeStart = new QueryExpression("data_time",IndexOperator.GTE,""+startTime);
				QueryExpression myQETimeEnd = new QueryExpression("data_time",IndexOperator.LTE,""+endTime);
				QueryExpression myQEProvince = new QueryExpression("cassandra_province",IndexOperator.EQ,""+province);
				QueryExpression myQECity = new QueryExpression("cassandra_city",IndexOperator.EQ,""+city);
				QueryExpression myQEDistrict = new QueryExpression("cassandra_district",IndexOperator.EQ,""+district);
				qe.add(myQEDay);
				qe.add(myQETimeStart);
				qe.add(myQETimeEnd);
				qe.add(myQEProvince);
				qe.add(myQECity);
				qe.add(myQEDistrict);
				qes.add(qe);
			}else if(days.size()==2){
				ArrayList<QueryExpression> qe = new ArrayList<QueryExpression>();
				QueryExpression myQEDay = new QueryExpression("year_month_day",IndexOperator.EQ,startStr);
				QueryExpression myQETime = new QueryExpression("data_time",IndexOperator.GTE,""+startTime);
				QueryExpression myQEProvince = new QueryExpression("cassandra_province",IndexOperator.EQ,""+province);
				QueryExpression myQECity = new QueryExpression("cassandra_city",IndexOperator.EQ,""+city);
				QueryExpression myQEDistrict = new QueryExpression("cassandra_district",IndexOperator.EQ,""+district);
				qe.add(myQEDay);
				qe.add(myQETime);
				qe.add(myQEProvince);
				qe.add(myQECity);
				qe.add(myQEDistrict);
				qes.add(qe);
				
				ArrayList<QueryExpression> qe1 = new ArrayList<QueryExpression>();
				QueryExpression myQEDay1 = new QueryExpression("year_month_day",IndexOperator.EQ,endStr);
				QueryExpression myQETime1 = new QueryExpression("data_time",IndexOperator.LTE,""+endTime);
				QueryExpression myQEProvince1 = new QueryExpression("cassandra_province",IndexOperator.EQ,""+province);
				QueryExpression myQECity1 = new QueryExpression("cassandra_city",IndexOperator.EQ,""+city);
				QueryExpression myQEDistrict1 = new QueryExpression("cassandra_district",IndexOperator.EQ,""+district);
				qe.add(myQEDay1);
				qe.add(myQETime1);
				qe.add(myQEProvince1);
				qe.add(myQECity1);
				qe.add(myQEDistrict1);
				qes.add(qe1);
			}else{
				for(int i=0; i<days.size(); i++){
					if(days.get(i).equals(startStr)){
						ArrayList<QueryExpression> qe = new ArrayList<QueryExpression>();
						QueryExpression myQEDay = new QueryExpression("year_month_day",IndexOperator.EQ,startStr);
						QueryExpression myQETime = new QueryExpression("data_time",IndexOperator.GTE,""+startTime);
						QueryExpression myQEProvince = new QueryExpression("cassandra_province",IndexOperator.EQ,""+province);
						QueryExpression myQECity = new QueryExpression("cassandra_city",IndexOperator.EQ,""+city);
						QueryExpression myQEDistrict = new QueryExpression("cassandra_district",IndexOperator.EQ,""+district);
						qe.add(myQEDay);
						qe.add(myQETime);
						qe.add(myQEProvince);
						qe.add(myQECity);
						qe.add(myQEDistrict);
						qes.add(qe);
					}else if(days.get(i).equals(endStr)){
						ArrayList<QueryExpression> qe = new ArrayList<QueryExpression>();
						QueryExpression myQEDay = new QueryExpression("year_month_day",IndexOperator.EQ,endStr);
						QueryExpression myQETime = new QueryExpression("data_time",IndexOperator.LTE,""+endTime);
						QueryExpression myQEProvince = new QueryExpression("cassandra_province",IndexOperator.EQ,""+province);
						QueryExpression myQECity = new QueryExpression("cassandra_city",IndexOperator.EQ,""+city);
						QueryExpression myQEDistrict = new QueryExpression("cassandra_district",IndexOperator.EQ,""+district);
						qe.add(myQEDay);
						qe.add(myQETime);
						qe.add(myQEProvince);
						qe.add(myQECity);
						qe.add(myQEDistrict);
						qes.add(qe);
					}else{
						long timeTemp = (dateFormat.parse(days.get(i))).getTime();
						ArrayList<QueryExpression> qe = new ArrayList<QueryExpression>();
						QueryExpression myQEDay = new QueryExpression("year_month_day",IndexOperator.EQ,days.get(i));
						QueryExpression myQETime = new QueryExpression("data_time",IndexOperator.GTE,""+timeTemp);
						QueryExpression myQEProvince = new QueryExpression("cassandra_province",IndexOperator.EQ,""+province);
						QueryExpression myQECity = new QueryExpression("cassandra_city",IndexOperator.EQ,""+city);
						QueryExpression myQEDistrict = new QueryExpression("cassandra_district",IndexOperator.EQ,""+district);
						qe.add(myQEDay);
						qe.add(myQETime);
						qe.add(myQEProvince);
						qe.add(myQECity);
						qe.add(myQEDistrict);
						qes.add(qe);
					}
				}
			}
			return getGpsData(orgs,testTypes,qes,"",-1);
		}
		return null;
	}
	
	/**
	 * 根据省份获取测试数据
	 * @author Ocean
	 * @param province
	 * @param city
	 * @param district
	 * @param orgs
	 * @param testTypes
	 */
	public List<Map<String, String>> getTestDataDelayByLocationProvince(String province, JSONArray orgs, JSONArray testTypes, Date startDate, Date endDate) throws Exception{
		List<ArrayList<QueryExpression>> qes = new ArrayList<ArrayList<QueryExpression>>();
		long startTime = startDate.getTime();
		long endTime = endDate.getTime();
		SimpleDateFormat dateFormat = new SimpleDateFormat(
				"yyyyMMdd");
		String startStr = dateFormat.format(startDate);
		String endStr = dateFormat.format(endDate);
		
		List<String> days = this.parseDays(startDate, endDate);
		if(days!=null && days.size()!=0){
			if(days.size()==1){
				ArrayList<QueryExpression> qe = new ArrayList<QueryExpression>();
				QueryExpression myQEDay = new QueryExpression("year_month_day",IndexOperator.EQ,startStr);
				QueryExpression myQETimeStart = new QueryExpression("data_time",IndexOperator.GTE,""+startTime);
				QueryExpression myQETimeEnd = new QueryExpression("data_time",IndexOperator.LTE,""+endTime);
				QueryExpression myQEProvince = new QueryExpression("cassandra_province",IndexOperator.EQ,""+province);
				qe.add(myQEDay);
				qe.add(myQETimeStart);
				qe.add(myQETimeEnd);
				qe.add(myQEProvince);
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
			return getTestData(orgs,testTypes,qes,"",-1);
		}
		return null;
	}

	public List<Map<String, String>> getTestDataDelayByLocationCity(String province, String city, String district, JSONArray orgs, JSONArray testTypes){
		return null;
	}
	public List<Map<String, String>> getTestDataDelayByLocationDistrict(String province, String city, String district, JSONArray orgs, JSONArray testTypes){
		return null;
	}
	
	public static List<Map<String, String>> getTestDbm(JSONArray keyspaces, JSONArray columnFamilies, List<ArrayList<QueryExpression>> qes, String startKey, int count){
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
		int allCount = 0;
		int homeCountA = 0;//好点情况下小区个数
		Map homeMapA = new HashMap();
		int homeCountB = 0;//中等
		Map homeMapB = new HashMap();
		int homeCountC = 0;//差
		Map homeMapC = new HashMap();
		for(int x=0; x<qes.size(); x++){
			List<QueryExpression> qe = qes.get(x);
			for(int i=0; i<keyspaces.size(); i++){
				//EQ代表“=”，GTE代表“>=”，GT代表“>”，LTE代表“<=”，LT代表“<”
				if((String)keyspaces.get(i)==null || "".equals((String)keyspaces.get(i))){
					continue;
				}
				Keyspace keyspaceOperator = HFactory.createKeyspace((String)keyspaces.get(i),
						KeySpaceFactory.cluster);
				
				for(int j=0; j<columnFamilies.size(); j++){
					ColumnFamilyTemplate<String, String> columnFamilyTemplate = new ThriftColumnFamilyTemplate<String, String>(
							keyspaceOperator, (String)columnFamilies.get(j), KeySpaceFactory.stringSerializer, KeySpaceFactory.stringSerializer);
					IndexedSlicesPredicate<String, String, String> dataPredicate = new IndexedSlicesPredicate<String, String, String>(
							KeySpaceFactory.stringSerializer, KeySpaceFactory.stringSerializer, KeySpaceFactory.stringSerializer);
					String columnFamilyStr = (String)columnFamilies.get(j);
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
						ColumnFamilyResult<String, String> list = columnFamilyTemplate
								.queryColumns(dataPredicate);
						
						while (list.hasResults()) {
							if(lastKey.equals(list.getKey())){
								continue;
							}
							lastKey = list.getKey();
							Map<String, String> myData = new HashMap<String, String>();
							Collection<String> element = list.getColumnNames();
							Object[] columns = element.toArray();
							stepCount++;
							total++;
							if(total==count){
								break;
							}
							allCount++;
							Map columnMap = new HashMap();
							for(int r=0; r<columns.length; r++){
								String columnName = (String)columns[r];
								String columnValue = list.getString(""+columns[r]);
								myData.put(columnName, columnValue);
								columnMap.put(columnName, columnValue);
								result.add(myData);
								System.out.println(""+columnName+" : "+columnValue);
							}
							String netValue = "";
							//判断条件
							String netWorkType1 = "";
							String netWorkType2 = "";
							
							String singel = columnFamilyStr.substring(columnFamilyStr.lastIndexOf("_")+1, columnFamilyStr.length());
							System.out.println(singel);
							if (columnMap.containsKey("网络(2)网络制式") && columnMap.containsKey("网络(1)网络制式"))
							{
								netWorkType1 = (String)columnMap.get("网络(1)网络制式");
								netWorkType2 = (String)columnMap.get("网络(2)网络制式");
								if(netWorkType1.equals(singel)){
									allCount++;
									netValue = (String)columnMap.get((String)columnMap.get("网络(1)信号强度"));
									if(!netValue.equals("N/A") && !netValue.equals("")){
										int value = Integer.parseInt(netValue);
										if(value>=-80){
											homeCountA++;
											String homeName = (String)columnMap.get("小区信息(LAC/CID)");
											homeMapA.put(homeName, "");
										}else if(value>=-95&&value<-80){
											homeCountB++;
										}else if(value<-95){
											homeCountC++;
										}
									}	
								}
								if(netWorkType2.equals(singel)){
									allCount++;
									netValue = (String)columnMap.get((String)columnMap.get("网络(2)信号强度")); 
									if(!netValue.equals("N/A") && !netValue.equals("")){
										int value = Integer.parseInt(netValue);
										if(value>=-80){
											homeCountA++;
											String homeName = (String)columnMap.get("小区信息(LAC/CID)");
											homeMapA.put(homeName, "");
										}else if(value>=-95&&value<-80){
											homeCountB++;
										}else if(value<-95){
											homeCountC++;
										}
									}	
								}
							} else if (columnMap.containsKey("网络(2)网络制式")){
								netWorkType1 = (String)columnMap.get("网络(2)网络制式");
								if(netWorkType1.equals(singel)){
									allCount++;
									netValue = (String)columnMap.get((String)columnMap.get("网络(2)信号强度")); 
									if(!netValue.equals("N/A") && !netValue.equals("")){
										int value = Integer.parseInt(netValue);
										if(value>=-80){
											homeCountA++;
											String homeName = (String)columnMap.get("小区信息(LAC/CID)");
											homeMapA.put(homeName, "");
										}else if(value>=-95&&value<-80){
											homeCountB++;
										}else if(value<-95){
											homeCountC++;
										}
									}	
								}
							} else if (columnMap.containsKey("网络(1)网络制式")){
								netWorkType1 = (String)columnMap.get("网络(1)网络制式");
								if(netWorkType1.equals(singel)){
									allCount++;
									netValue = (String)columnMap.get((String)columnMap.get("网络(1)信号强度")); 
									if(!netValue.equals("N/A") && !netValue.equals("")){
										int value = Integer.parseInt(netValue);
										if(value>=-80){
											homeCountA++;
											String homeName = (String)columnMap.get("小区信息(LAC/CID)");
											homeMapA.put(homeName, "");
										}else if(value>=-95&&value<-80){
											homeCountB++;
										}else if(value<-95){
											homeCountC++;
										}
									}	
								}
							} else if (columnMap.containsKey("网络制式")){
								netWorkType1 = (String)columnMap.get("网络制式");
								if(netWorkType1.equals(singel)){
									allCount++;
									netValue = (String)columnMap.get((String)columnMap.get("信号强度")); 
									if(!netValue.equals("N/A") && !netValue.equals("")){
										int value = Integer.parseInt(netValue);
										if(value>=-80){
											homeCountA++;
											String homeName = (String)columnMap.get("小区信息(LAC/CID)");
											homeMapA.put(homeName, "");
										}else if(value>=-95&&value<-80){
											homeCountB++;
										}else if(value<-95){
											homeCountC++;
										}
									}	
								}
							} else if (columnMap.containsKey("网络类型")){
								netWorkType1 = (String)columnMap.get("网络类型");
								if(netWorkType1.equals(singel)){
									allCount++;
									netValue = (String)columnMap.get((String)columnMap.get("信号强度")); 
									if(!netValue.equals("N/A") && !netValue.equals("")){
										int value = Integer.parseInt(netValue);
										if(value>=-80){
											homeCountA++;
											String homeName = (String)columnMap.get("小区信息(LAC/CID)");
											homeMapA.put(homeName, "");
										}else if(value>=-95&&value<-80){
											homeCountB++;
										}else if(value<-95){
											homeCountC++;
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
		
		return result;
	}
	
	/**
	 * @author Ocean
	 * @param keyspaces
	 * @param columnFamilies
	 * @param qe
	 * @param startKey
	 * @param count
	 * @return
	 */
	public List<Map<String, String>> getTestData(JSONArray keyspaces, JSONArray columnFamilies, List<ArrayList<QueryExpression>> qes, String startKey, int count){
		List<Map<String, String>> result = new ArrayList<Map<String, String>> ();
		JSONArray allColumnFamilies = null;
		JSONArray sortColumnFamilies = null;
		
		int defaultStep = 200;
		int total = 0;
		boolean limit = false;
		if(count>-1){
			limit = true;
			if(defaultStep>=count){
				defaultStep = count;
			}
		}
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
					
					//默认认为以"0"或者“signal”开始的columnfamily name为测试类型
					if(columnFamilies.contains("all") || columnFamilies.contains("ALL")){
						allColumnFamilies = new JSONArray();
						KeyspaceDefinition kd = KeySpaceFactory.cluster.describeKeyspace((String)keyspaces.get(i));
						List<ColumnFamilyDefinition> cfList = kd.getCfDefs();
						for (int j = 0; j < cfList.size(); j++) {
							ColumnFamilyDefinition cfD = cfList.get(j);
							if(cfD.getName().startsWith("0") || cfD.getName().startsWith("signal")){
								allColumnFamilies.add(cfD.getName());
							}
						}
					}
				}catch (Exception e){
					e.printStackTrace();
					continue;
				}
				if(columnFamilies.contains("all") || columnFamilies.contains("ALL")){
					sortColumnFamilies = allColumnFamilies;
				}else{
					sortColumnFamilies = columnFamilies;
				}
				for(int j=0; j<sortColumnFamilies.size(); j++){
					try{
						ColumnFamilyTemplate<String, String> columnFamilyTemplate = new ThriftColumnFamilyTemplate<String, String>(
								keyspaceOperator, (String)sortColumnFamilies.get(j), KeySpaceFactory.stringSerializer, KeySpaceFactory.stringSerializer);
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
									if(list.hasNext()){
										list.next();
									}else{
										break;
									}
									continue;
								}
								lastKey = list.getKey();
								Map<String, String> myData = new HashMap<String, String>();
								Collection<String> element = list.getColumnNames();
								Object[] columns = element.toArray();
								stepCount++;
								total++;
								if(total==count){
									break;
								}
								for(int r=0; r<columns.length; r++){
									
									myData.put((String)columns[r], list.getString(""+columns[r]));
									result.add(myData);
									System.out.println(""+columns[r]+" : "+list.getString(""+columns[r]));
								}
								if(list.hasNext()){
									list.next();
								}else{
									break;
								}
							}
						}
					}catch (Exception e){
						e.printStackTrace();
						continue;
					}	
				}
			}
		}
		
		
		System.out.println("总处理数据量"+total);
		
		return result;
	}
	
	/**
	 * @author Ocean
	 * @param keyspaces
	 * @param columnFamilies
	 * @param qe
	 * @param startKey
	 * @param count
	 * @return
	 */
	public List<Map<String, String>> getGpsData(JSONArray keyspaces, JSONArray columnFamilies, List<ArrayList<QueryExpression>> qes, String startKey, int count){
		List<Map<String, String>> result = new ArrayList<Map<String, String>> ();
		JSONArray allColumnFamilies = null;
		JSONArray sortColumnFamilies = null;
		
		List<String> columnsPredict = new ArrayList<String>();
		columnsPredict.add("GPS位置");
		columnsPredict.add("GPS位置信息");
		columnsPredict.add("GPS信息");
		columnsPredict.add("测试位置");
		columnsPredict.add("测试GPS位置");
		columnsPredict.add("cassandra_province");
		columnsPredict.add("cassandra_city");
		columnsPredict.add("cassandra_district");
		
		int defaultStep = 20;
		int total = 0;
		int processCount = 0;
		boolean limit = false;
		if(count>-1){
			limit = true;
			if(defaultStep>=count){
				defaultStep = count;
			}
		}
		
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
					
					//默认认为以"0"或者“signal”开始的columnfamily name为测试类型
					if(columnFamilies.contains("all") || columnFamilies.contains("ALL")){
						allColumnFamilies = new JSONArray();
						KeyspaceDefinition kd = KeySpaceFactory.cluster.describeKeyspace((String)keyspaces.get(i));
						List<ColumnFamilyDefinition> cfList = kd.getCfDefs();
						for (int j = 0; j < cfList.size(); j++) {
							ColumnFamilyDefinition cfD = cfList.get(j);
							if(cfD.getName().startsWith("0") || cfD.getName().startsWith("signal")){
								allColumnFamilies.add(cfD.getName());
							}
						}
					}
				}catch (Exception e){
					e.printStackTrace();
					continue;
				}
				if(columnFamilies.contains("all") || columnFamilies.contains("ALL")){
					sortColumnFamilies = allColumnFamilies;
				}else{
					sortColumnFamilies = columnFamilies;
				}
				for(int j=0; j<sortColumnFamilies.size(); j++){
					try{
						ColumnFamilyTemplate<String, String> columnFamilyTemplate = new ThriftColumnFamilyTemplate<String, String>(
								keyspaceOperator, (String)sortColumnFamilies.get(j), KeySpaceFactory.stringSerializer, KeySpaceFactory.stringSerializer);
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
							SimpleDateFormat dateFormat = new SimpleDateFormat(
									"yyyy-MM-dd HH:mm:ss.SSS");
							processCount++;
							System.out.println("Start Reading---->"+(dateFormat.format(new Date()))+"  --->FILTERTIMES: "+x+"  --->KEYSPACE: "+i+"  --->COLUMNFAMILY: "+j+"  --->PROCESSCOUNT: "+processCount);
							try{
								list = columnFamilyTemplate
										.queryColumns(dataPredicate,columnsPredict);
							}catch (Exception e){
								System.out.println("KEYSPACE------>"+(String)keyspaces.get(i)+"   COLUMNFAMILY------->"+(String)sortColumnFamilies.get(j));
								e.printStackTrace();
								continue;
							}
							System.out.println("Stop Reading---->"+(dateFormat.format(new Date())));
							while (list.hasResults()) {
								if(lastKey.equals(list.getKey())){
									if(list.hasNext()){
										list.next();
									}else{
										break;
									}
									continue;
								}
								lastKey = list.getKey();
								Map<String, String> myData = new HashMap<String, String>();
								Collection<String> element = list.getColumnNames();
								Object[] columns = element.toArray();
								stepCount++;
								if(total==count){
									break;
								}
								int previ = 0;
								String gpsStr = "";
								String province = "";
								String city = "";
								String district = "";
								for(int r=0; r<columns.length; r++){
									if(((String)columns[r]).equals("GPS位置")){
										if(previ<5){
											previ = 5;
											gpsStr = list.getString(""+columns[r]);
										}
										continue;
									}else if(((String)columns[r]).equals("GPS位置信息")){
										if(previ<4){
											previ = 4;
											gpsStr = list.getString(""+columns[r]);
										}
										continue;
									}else if(((String)columns[r]).equals("GPS信息")){
										if(previ<3){
											previ = 3;
											gpsStr = list.getString(""+columns[r]);
										}
										continue;
									}else if(((String)columns[r]).equals("测试位置")){
										if(previ<2){
											previ = 3;
											gpsStr = list.getString(""+columns[r]);
										}
										continue;
									}else if(((String)columns[r]).equals("测试GPS位置")){
										if(previ<1){
											previ = 3;
											gpsStr = list.getString(""+columns[r]);
										}
										continue;
									}
									
									if(((String)columns[r]).equals("cassandra_province")){
										province = list.getString(""+columns[r]);
										continue;
									}
									if(((String)columns[r]).equals("cassandra_city")){
										city = list.getString(""+columns[r]);
										continue;
									}
									if(((String)columns[r]).equals("cassandra_district")){
										district = list.getString(""+columns[r]);
										continue;
									}
									
//									if(((String)columns[r]).startsWith("cassandra_")){
//										System.out.println(""+columns[r]+" : "+list.getString(""+columns[r]));
//									}
								}

								if(gpsStr != null && !gpsStr.trim().equals("--") && !gpsStr.trim().equals("-") && !gpsStr.trim().equals("") && !gpsStr.trim().equals("N/A")){
									if(gpsStr.contains(" ")){
										String[] gps = transGpsPoint(gpsStr);
										if(gps!=null && gps[0]!=null && gps[1]!=null){
											myData.put("GPSLocation", gps[0]+" "+gps[1]);
											myData.put("imgUrl", "1.png");
											myData.put("province", province);
											myData.put("city", city);
											myData.put("district", district);
											result.add(myData);
											total++;
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
					}catch (Exception e){
						e.printStackTrace();
						continue;
					}
				}
					
			}
		}
		
		
		System.out.println("总处理数据量"+total);
		
		return result;
	}
	
	/**
	 * @author Ocean
	 * @param keyspaces
	 * @param columnFamilies
	 * @param qe
	 * @param startKey
	 * @param count
	 * @return
	 */
	public Map<String, Object> getReportData(JSONArray keyspaces, String columnFamily, JSONArray testTypes, List<ArrayList<QueryExpression>> qes, String startKey){
		Map<String, Object> response = new HashMap<String, Object>();
		Map<String, String> result = new HashMap<String, String> ();
		
		int defaultStep = 200;
		int total = 0;
		
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
				
				
				ColumnFamilyTemplate<String, String> columnFamilyTemplate = new ThriftColumnFamilyTemplate<String, String>(
						keyspaceOperator, columnFamily, KeySpaceFactory.stringSerializer, KeySpaceFactory.stringSerializer);
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
				
				while(stepCount>=(defaultStep-1)){
					stepCount = 0;
					dataPredicate.count(defaultStep).startKey(lastKey);
					ColumnFamilyResult<String, String> list = null;
					try{
						list = columnFamilyTemplate
								.queryColumns(dataPredicate);
					}catch (Exception e){
						System.out.println("  --->FILTERTIMES: "+x+"  --->KEYSPACE: "+(String)keyspaces.get(i)+"  --->COLUMNFAMILY: "+columnFamily);
						
						e.printStackTrace();
						continue;
					}
					
					while (list.hasResults()) {
						if(lastKey.equals(list.getKey())){
							if(list.hasNext()){
								list.next();
							}else{
								break;
							}
							continue;
						}
						lastKey = list.getKey();
						Collection<String> element = list.getColumnNames();
						Object[] columns = element.toArray();
						stepCount++;
						
						for(int r=0; r<columns.length; r++){
							String columnName = (String)columns[r];
							
							if(testTypes.contains(columnName) || (testTypes.contains("all")&&!columnName.equals("year_month_day")&&!columnName.equals("province")&&!columnName.equals("city")&&!columnName.equals("district")) || (testTypes.contains("ALL")&&!columnName.equals("year_month_day")&&!columnName.equals("province")&&!columnName.equals("city")&&!columnName.equals("district"))){
								String countTemp = list.getString(""+columns[r]);
								String existCount = result.get(columnName);
								if(existCount==null || "".equals(existCount)){
									int countTempInt = Integer.parseInt(countTemp);
									total += countTempInt;
									result.put(columnName, countTemp);
								}else{
									int existCountInt = Integer.parseInt(existCount);
									int countTempInt = Integer.parseInt(countTemp);
									existCountInt += countTempInt;
									total += countTempInt;
									result.put(columnName, ""+existCountInt);
								}
								System.out.println(""+columns[r]+" : "+list.getString(""+columns[r]));
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
		
		
		System.out.println("总处理数据量"+total);
		response.put("total", total);
		response.put("detail", result);
		return response;
	}
	
	/**
	 * @author Ocean
	 * @param type   指LTE/SINR/EDGE等网络类型
	 * @param keyspaces
	 * @param columnFamilies
	 * @param qe
	 * @param startKey
	 * @param count
	 * @return
	 */
	public Map<String,Object> getSignalData(String type, JSONArray keyspaces, JSONArray columnFamilies, List<ArrayList<QueryExpression>> qes, String startKey, int count, boolean isDetail){
		Map<String,Object> result = new HashMap<String,Object> ();
		JSONArray allColumnFamilies = null;
		JSONArray sortColumnFamilies = null;
		
		List<String> columnsPredict = new ArrayList<String>();
		columnsPredict.add("网络(1)网络制式");
		columnsPredict.add("网络(2)网络制式");
		columnsPredict.add("网络制式");
		columnsPredict.add("网络类型");

		columnsPredict.add("网络(1)信号强度");
		columnsPredict.add("网络(2)信号强度");
		columnsPredict.add("信号强度");
		
		columnsPredict.add("GPS位置");
		columnsPredict.add("GPS位置信息");
		columnsPredict.add("GPS信息");
		columnsPredict.add("测试位置");
		columnsPredict.add("测试GPS位置");
		
		columnsPredict.add("cassandra_province");
		columnsPredict.add("cassandra_city");
		columnsPredict.add("cassandra_district");

		columnsPredict.add("SINR");
		
		int defaultStep = 200;
		int total = 0;
		boolean limit = false;
		if(count>-1){
			limit = true;
			if(defaultStep>=count){
				defaultStep = count;
			}
		}
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
					
					//默认认为以"0"或者“signal”开始的columnfamily name为测试类型
					if(columnFamilies.contains("all") || columnFamilies.contains("ALL")){
						allColumnFamilies = new JSONArray();
						KeyspaceDefinition kd = KeySpaceFactory.cluster.describeKeyspace((String)keyspaces.get(i));
						List<ColumnFamilyDefinition> cfList = kd.getCfDefs();
						for (int j = 0; j < cfList.size(); j++) {
							ColumnFamilyDefinition cfD = cfList.get(j);
							if(cfD.getName().startsWith("0") || cfD.getName().startsWith("signal")){
								allColumnFamilies.add(cfD.getName());
							}
						}
					}
				}catch (Exception e){
					e.printStackTrace();
					continue;
				}
				if(columnFamilies.contains("all") || columnFamilies.contains("ALL")){
					sortColumnFamilies = allColumnFamilies;
				}else{
					sortColumnFamilies = columnFamilies;
				}
				for(int j=0; j<sortColumnFamilies.size(); j++){
					try{
						ColumnFamilyTemplate<String, String> columnFamilyTemplate = new ThriftColumnFamilyTemplate<String, String>(
								keyspaceOperator, (String)sortColumnFamilies.get(j), KeySpaceFactory.stringSerializer, KeySpaceFactory.stringSerializer);
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
										.queryColumns(dataPredicate,columnsPredict);
							}catch (Exception e){
								e.printStackTrace();
								continue;
							}
							
							while (list.hasResults()) {
								if(list.getKey().equals("CMCC_CMRI_DEPT_CESHI_YEWUSHI|speedtest|Mobile|SM-G9008V|352107066544237|00000000_01001.1753_0-2014_11_04_15_05_34_656.summary.csv_0")){
									int h =1;
									h++;
								}
								if(lastKey.equals(list.getKey())){
									if(list.hasNext()){
										list.next();
									}else{
										break;
									}
									continue;
								}
								lastKey = list.getKey();
								HashMap<String, String> myData = new HashMap<String, String>();
								Collection<String> element = list.getColumnNames();
								Object[] columns = element.toArray();
								stepCount++;
								
								if(total==count){
									break;
								}

								int previGps = 0;
								int previNetType = 0;
								String gpsStr = "";
								String NetType1 = "";
								String NetType2 = "";
								String NetType = "";
								String signalStength1 = "";
								String signalStength2 = "";
								String signalStength = "";
								String sinr = "";
								
								String province = "";
								String city = "";
								String district = "";
								for(int r=0; r<columns.length; r++){
									
									//获取网络制式及信号强度Start
									if(((String)columns[r]).equals("网络(1)网络制式")){
										NetType1 = list.getString(""+columns[r]);
										continue;
									}else if(((String)columns[r]).equals("网络(2)网络制式")){
										NetType2 = list.getString(""+columns[r]);
										continue;
									}else if(((String)columns[r]).equals("网络制式")){
										if(previNetType<=2){
											previNetType = 2;
											NetType = list.getString(""+columns[r]);
										}
										continue;
									}else if(((String)columns[r]).equals("网络类型")){
										if(previNetType<=1){
											previNetType = 1;
											NetType = list.getString(""+columns[r]);
										}
										continue;
									}
									
									if(((String)columns[r]).equals("网络(1)信号强度")){
										signalStength1 = list.getString(""+columns[r]);
										continue;
									}else if(((String)columns[r]).equals("网络(2)信号强度")){
										signalStength2 = list.getString(""+columns[r]);
										continue;
									}else if(((String)columns[r]).equals("信号强度")){
										if(previNetType<=1){
											previNetType = 1;
											signalStength = list.getString(""+columns[r]);
										}
										continue;
									}
									//获取网络制式及信号强度End
									
									if(((String)columns[r]).equals("SINR")){
										sinr = list.getString(""+columns[r]);
										continue;
									}
									
									//获取GPS位置信息Start
									if(((String)columns[r]).equals("GPS位置")){
										if(previGps<5){
											previGps = 5;
											gpsStr = list.getString(""+columns[r]);
										}
										continue;
									}else if(((String)columns[r]).equals("GPS位置信息")){
										if(previGps<4){
											previGps = 4;
											gpsStr = list.getString(""+columns[r]);
										}
										continue;
									}else if(((String)columns[r]).equals("GPS信息")){
										if(previGps<3){
											previGps = 3;
											gpsStr = list.getString(""+columns[r]);
										}
										continue;
									}else if(((String)columns[r]).equals("测试位置")){
										if(previGps<2){
											previGps = 3;
											gpsStr = list.getString(""+columns[r]);
										}
										continue;
									}else if(((String)columns[r]).equals("测试GPS位置")){
										if(previGps<1){
											previGps = 3;
											gpsStr = list.getString(""+columns[r]);
										}
										continue;
									}									
									//获取GPS位置信息End
									
									if(((String)columns[r]).equals("cassandra_province")){
										province = list.getString(""+columns[r]);
										continue;
									}
									if(((String)columns[r]).equals("cassandra_city")){
										city = list.getString(""+columns[r]);
										continue;
									}
									if(((String)columns[r]).equals("cassandra_district")){
										district = list.getString(""+columns[r]);
										continue;
									}
								}
								
								//数据格式整理
								String netResult = "";
								String signalResult = "";
								String[] gpsresult = new String[2];
								String sinrResult = "";
								String[] g2 = {"2g","edge", "gprs", "gsm","cdma 1x"};
								String[] g3 = {"3g","hsdpa","td-scdma","tds-hsdpa","umts","hspa","hspa+","wcdma","cdma2000","evdo","evdo_a","evdo_b"};
								String[] g4 = {"lte","td-lte","fdd-lte","tdd-lte"};
								
								String[] gps = new String[2];
								if(gpsStr != null && !gpsStr.trim().equals("--") && !gpsStr.trim().equals("-") && !gpsStr.trim().equals("")){
									if(gpsStr.contains(" ")){
										gps = transGpsPoint(gpsStr);
										if(gps!=null && gps[0]!=null && gps[1]!=null){
											gpsresult = gps;
										}else{
											if(list.hasNext()){
												list.next();
											}else{
												break;
											}
											continue;
										}
									}
								}else{
									if(list.hasNext()){
										list.next();
									}else{
										break;
									}
									continue;
								}
								if(NetType1!=null && !NetType1.equals("") && !NetType1.trim().equals("-") && !NetType1.trim().equals("--") && !NetType1.trim().equals("N/A") && (NetType1.trim().equals(type) || type.toLowerCase().equals("all") || type.toLowerCase().equals("sinr"))){
									netResult = NetType1.trim();
									signalResult = signalStength1.trim();
								}else if(NetType2!=null && !NetType2.equals("") && !NetType2.trim().equals("-") && !NetType2.trim().equals("--") && !NetType2.trim().equals("N/A") && (NetType2.trim().equals(type) || type.toLowerCase().equals("all") || type.toLowerCase().equals("sinr"))){
									netResult = NetType2.trim();
									signalResult = signalStength2.trim();
								}else if(NetType!=null && !NetType.equals("") && !NetType.trim().equals("-") && !NetType.trim().equals("--") && !NetType.trim().equals("N/A") && (NetType.trim().equals(type) || type.toLowerCase().equals("all") || type.toLowerCase().equals("sinr"))){
									netResult = NetType.trim();
									signalResult = signalStength.trim();
								}
								
								boolean isSure = false;
								boolean is4G = false;
								for(int g=0; g<g4.length && !isSure; g++){
									if(netResult!=null && netResult.toLowerCase().equals(g4[g])){
										is4G = true;
										isSure = true;
										break;
									}
								}
								boolean is3G = false;
								for(int g=0; g<g3.length && !isSure; g++){
									if(netResult!=null && netResult.toLowerCase().equals(g3[g])){
										is3G = true;
										isSure = true;
										break;
									}
								}
								boolean is2G = false;
								for(int g=0; g<g2.length && !isSure; g++){
									if(netResult!=null && netResult.toLowerCase().equals(g2[g])){
										is2G = true;
										isSure = true;
										break;
									}
								}
								if(is4G && sinr!=null && !sinr.equals("") && !sinr.trim().equals("-") && !sinr.trim().equals("--") && !sinr.trim().equals("N/A") && (type.toLowerCase().equals("all") || type.toLowerCase().equals("sinr"))){
									int value = 2000;
									sinrResult = sinr;
									try{
										value = Integer.parseInt(sinrResult);
										if(value>=25){
											myData.put("imgUrl", "1.png");
										}else if(value>=15 && value<25){
											myData.put("imgUrl", "2.png");
										}else if(value>=5 && value<15){
											myData.put("imgUrl", "3.png");
										}else if(value>=0 && value<5){
											myData.put("imgUrl", "4.png");
										}else if(value<0){
											myData.put("imgUrl", "5.png");
										}
									}catch (Exception e){
										e.printStackTrace();
									}
									
									Map<String,Object> netResultList = (HashMap<String, Object>)result.get("SINR");
									if(netResultList==null){
										netResultList = new HashMap<String,Object>();
									}
									
									String totalType = (String)netResultList.get("count");
									int totalTypeInte = 0;
									if(totalType!=null && !"".equals(totalType)){
										totalTypeInte = Integer.parseInt(totalType);
									}
									String analyzeTotalType = (String)netResultList.get("analyzed_count");
									int analyzeTotalTypeInte = 0;
									if(analyzeTotalType!=null && !"".equals(analyzeTotalType)){
										analyzeTotalTypeInte = Integer.parseInt(analyzeTotalType);
									}
									myData.put("GPSLocation", gpsresult[0]+" "+gpsresult[1]);
									myData.put("province", province);
									myData.put("city", city);
									myData.put("district", district);
									myData.put("NetType", netResult);
									myData.put("SignalStrength", signalResult);
									myData.put("SINR", sinrResult);
									
									
									totalTypeInte++;
									if(value!=2000 && value>-3){
										analyzeTotalTypeInte++;
									}
									netResultList.put("count",""+totalTypeInte);
									netResultList.put("analyzed_count",""+analyzeTotalTypeInte);
									netResultList.put("ratio",""+(((float)analyzeTotalTypeInte*100)/totalTypeInte)+"%");
									if(isDetail){
										ArrayList<HashMap<String, String>> detail = (ArrayList<HashMap<String, String>>)netResultList.get("detail");
										if(detail==null){
											detail = new ArrayList<HashMap<String, String>>();
										}
										detail.add(myData);
										netResultList.put("detail",detail);
									}
									result.put("SINR", netResultList);
									total++;
								}
								
								if(!type.toLowerCase().equals("sinr") && netResult!=null && !netResult.equals("") && !netResult.trim().equals("-") && !netResult.trim().equals("--") && !netResult.trim().equals("N/A") && signalResult!=null && !signalResult.equals("") && !signalResult.trim().equals("-") && !signalResult.trim().equals("--") && !signalResult.trim().equals("N/A") ){
									Map<String,Object> netResultList = (HashMap<String, Object>)result.get(netResult);
									if(netResultList==null){
										netResultList = new HashMap<String,Object>();
									}
									
									String totalType = (String)netResultList.get("count");
									int totalTypeInte = 0;
									if(totalType!=null && !"".equals(totalType)){
										totalTypeInte = Integer.parseInt(totalType);
									}
									myData.put("GPSLocation", gpsresult[0]+" "+gpsresult[1]);
									myData.put("province", province);
									myData.put("city", city);
									myData.put("district", district);
									myData.put("NetType", netResult);
									myData.put("SignalStrength", signalResult);
									
									int signalValue = 2000;
									try{
										signalValue = Integer.parseInt(signalResult.toLowerCase().replace("dbm", ""));
									}catch (Exception e){
										e.printStackTrace();
										if(list.hasNext()){
											list.next();
										}else{
											break;
										}
										continue;
									}
									
									totalTypeInte++;
									if(is4G){
										if(signalValue>=-60){
											myData.put("imgUrl", "1.png");
										}else if(signalValue>=-80 && signalValue<-60){
											myData.put("imgUrl", "2.png");
										}else if(signalValue>=-95 && signalValue<-80){
											myData.put("imgUrl", "3.png");
										}else if(signalValue>=-110 && signalValue<-95){
											myData.put("imgUrl", "4.png");
										}else if(signalValue<-110){
											myData.put("imgUrl", "5.png");
										}
										
										myData.put("SINR", sinrResult);
										
										int sinrValue = 2000;
										if(sinr!=null && !sinr.equals("") && !sinr.trim().equals("-") && !sinr.trim().equals("--") && !sinr.trim().equals("N/A") && (type.toLowerCase().equals("all") || type.toLowerCase().equals("sinr"))){
											sinrResult = sinr;
											try{
												sinrValue = Integer.parseInt(sinrResult);
											}catch (Exception e){
												e.printStackTrace();
											}
										}
										
										String analyzeTotalType = (String)netResultList.get("analyzed_count");
										int analyzeTotalTypeInte = 0;
										if(analyzeTotalType!=null && !"".equals(analyzeTotalType)){
											analyzeTotalTypeInte = Integer.parseInt(analyzeTotalType);
										}
										if(signalValue!=2000 && sinrValue!=2000 && signalValue>-110 && sinrValue>-3){
											analyzeTotalTypeInte++;
										}
										
										netResultList.put("analyzed_count",""+analyzeTotalTypeInte);
										netResultList.put("ratio",""+(((float)analyzeTotalTypeInte*100)/totalTypeInte)+"%");
									}else if(is3G){
										if(signalValue>=-60){
											myData.put("imgUrl", "1.png");
										}else if(signalValue>=-80 && signalValue<-60){
											myData.put("imgUrl", "2.png");
										}else if(signalValue>=-100 && signalValue<-80){
											myData.put("imgUrl", "3.png");
										}else if(signalValue>=-110 && signalValue<-100){
											myData.put("imgUrl", "4.png");
										}else if(signalValue<-110){
											myData.put("imgUrl", "5.png");
										}
										String analyzeTotalType = (String)netResultList.get("analyzed_count");
										int analyzeTotalTypeInte = 0;
										if(analyzeTotalType!=null && !"".equals(analyzeTotalType)){
											analyzeTotalTypeInte = Integer.parseInt(analyzeTotalType);
										}
										if(signalValue!=2000 && signalValue>-85){
											analyzeTotalTypeInte++;
										}
										netResultList.put("analyzed_count",""+analyzeTotalTypeInte);
										netResultList.put("ratio",""+(((float)analyzeTotalTypeInte*100)/totalTypeInte)+"%");
									}else if(is2G){
										if(signalValue>=-60){
											myData.put("imgUrl", "1.png");
										}else if(signalValue>=-80 && signalValue<-60){
											myData.put("imgUrl", "2.png");
										}else if(signalValue>=-90 && signalValue<-80){
											myData.put("imgUrl", "3.png");
										}else if(signalValue>=-100 && signalValue<-90){
											myData.put("imgUrl", "4.png");
										}else if(signalValue<-100){
											myData.put("imgUrl", "5.png");
										}
										String analyzeTotalType = (String)netResultList.get("analyzed_count");
										int analyzeTotalTypeInte = 0;
										if(analyzeTotalType!=null && !"".equals(analyzeTotalType)){
											analyzeTotalTypeInte = Integer.parseInt(analyzeTotalType);
										}
										if(signalValue!=2000 && signalValue>-90){
											analyzeTotalTypeInte++;
										}
										netResultList.put("analyzed_count",""+analyzeTotalTypeInte);
										netResultList.put("ratio",""+(((float)analyzeTotalTypeInte*100)/totalTypeInte)+"%");
									}
									
									if(isDetail){
										ArrayList<HashMap<String, String>> detail = (ArrayList<HashMap<String, String>>)netResultList.get("detail");
										if(detail==null){
											detail = new ArrayList<HashMap<String, String>>();
										}
										detail.add(myData);
										netResultList.put("detail",detail);
									}
									netResultList.put("count",""+totalTypeInte);
									result.put(netResult, netResultList);
									total++;
								}else{
									if(list.hasNext()){
										list.next();
									}else{
										break;
									}
									continue;
								}
							}
						}
					}catch (Exception e){
						e.printStackTrace();
						continue;
					}	
				}
			}
		}
		
		
		System.out.println("总处理数据量"+total);
		result.put("total", total);
		return result;
	}
	
	/**
	 * 获取网络测速的业务质量地图
	 * @author Ocean
	 * @param type  指下行速率、上行速率、时延
	 * @param keyspaces
	 * @param qe
	 * @param startKey
	 * @param count
	 * @return
	 */
	public Map<String,Object> getSpeedtestData(String type, JSONArray keyspaces, List<ArrayList<QueryExpression>> qes, String startKey, int count, boolean isDetail){
		Map<String,Object> result = new HashMap<String,Object> ();
		
		List<String> columnsPredict = new ArrayList<String>();
		columnsPredict.add("网络(1)网络制式");
		columnsPredict.add("网络(2)网络制式");
		columnsPredict.add("网络（1）网络制式");
		columnsPredict.add("网络（2）网络制式");
		columnsPredict.add("网络制式");
		columnsPredict.add("网络类型");

		columnsPredict.add("GPS位置");
		columnsPredict.add("GPS位置信息");
		columnsPredict.add("GPS信息");
		columnsPredict.add("测试位置");
		columnsPredict.add("测试GPS位置");
		
		columnsPredict.add("下行速率");
		columnsPredict.add("上行速率");
		columnsPredict.add("时延");
		
		columnsPredict.add("时延(ms)");
		columnsPredict.add("下行速率(Mbps)");
		columnsPredict.add("上行速率(Mbps)");
		columnsPredict.add("下行速率(Kbps)");
		columnsPredict.add("上行速率(Kbps)");
		
		columnsPredict.add("时延（ms）");
		columnsPredict.add("下行速率（Mbps）");
		columnsPredict.add("上行速率（Mbps）");
		columnsPredict.add("下行速率（Kbps）");
		columnsPredict.add("上行速率（Kbps）");

		columnsPredict.add("cassandra_province");
		columnsPredict.add("cassandra_city");
		columnsPredict.add("cassandra_district");
//		columnsPredict.add("TAC/PCI");
//		columnsPredict.add("LAC/CID");
//		columnsPredict.add("TAC");
//		columnsPredict.add("PCI");
//		columnsPredict.add("LAC");
//		columnsPredict.add("CID");
		
		int defaultStep = 200;
		int total = 0;
		boolean limit = false;
		if(count>-1){
			limit = true;
			if(defaultStep>=count){
				defaultStep = count;
			}
		}
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

				try{
					ColumnFamilyTemplate<String, String> columnFamilyTemplate = new ThriftColumnFamilyTemplate<String, String>(
							keyspaceOperator, "01001", KeySpaceFactory.stringSerializer, KeySpaceFactory.stringSerializer);
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
									.queryColumns(dataPredicate,columnsPredict);
						}catch (Exception e){
							e.printStackTrace();
							continue;
						}
						
						while (list.hasResults()) {
							if(lastKey.equals(list.getKey())){
								if(list.hasNext()){
									list.next();
								}else{
									break;
								}
								continue;
							}
							lastKey = list.getKey();
							HashMap<String, String> myData = new HashMap<String, String>();
							Collection<String> element = list.getColumnNames();
							Object[] columns = element.toArray();
							stepCount++;
							
							if(total==count){
								break;
							}

							int previGps = 0;
							int previNetType = 0;
							String gpsStr = "";
							String NetType1 = "";
							String NetType2 = "";
							String NetType = "";
							String downloadSpeed = "";
							String uploadSpeed = "";
							String dlSpeedUnit = "";
							String ulSpeedUnit = "";
							String delay = "";
							
							String province = "";
							String city = "";
							String district = "";
							for(int r=0; r<columns.length; r++){
								
								//获取网络制式及信号强度Start
								if(((String)columns[r]).equals("网络(1)网络制式") || ((String)columns[r]).equals("网络（1）网络制式")){
									NetType1 = list.getString(""+columns[r]);
									continue;
								}else if(((String)columns[r]).equals("网络(2)网络制式") || ((String)columns[r]).equals("网络（2）网络制式")){
									NetType2 = list.getString(""+columns[r]);
									continue;
								}else if(((String)columns[r]).equals("网络制式")){
									if(previNetType<=2){
										previNetType = 2;
										NetType = list.getString(""+columns[r]);
									}
									continue;
								}else if(((String)columns[r]).equals("网络类型")){
									if(previNetType<=1){
										previNetType = 1;
										NetType = list.getString(""+columns[r]);
									}
									continue;
								}
								//获取网络制式End
								
								//获取GPS位置信息Start
								if(((String)columns[r]).equals("GPS位置")){
									if(previGps<5){
										previGps = 5;
										gpsStr = list.getString(""+columns[r]);
									}
									continue;
								}else if(((String)columns[r]).equals("GPS位置信息")){
									if(previGps<4){
										previGps = 4;
										gpsStr = list.getString(""+columns[r]);
									}
									continue;
								}else if(((String)columns[r]).equals("GPS信息")){
									if(previGps<3){
										previGps = 3;
										gpsStr = list.getString(""+columns[r]);
									}
									continue;
								}else if(((String)columns[r]).equals("测试位置")){
									if(previGps<2){
										previGps = 3;
										gpsStr = list.getString(""+columns[r]);
									}
									continue;
								}else if(((String)columns[r]).equals("测试GPS位置")){
									if(previGps<1){
										previGps = 3;
										gpsStr = list.getString(""+columns[r]);
									}
									continue;
								}									
								//获取GPS位置信息End
								
								
								//获取速率及时延信息Start
								if(((String)columns[r]).equals("下行速率") || ((String)columns[r]).equals("下行速率(Mbps)") || ((String)columns[r]).equals("下行速率（Mbps）") || ((String)columns[r]).equals("下行速率(Kbps)") || ((String)columns[r]).equals("下行速率（Kbps）")){
									downloadSpeed = list.getString(""+columns[r]);
									dlSpeedUnit = (String)columns[r];
									continue;
								}else if(((String)columns[r]).equals("上行速率") || ((String)columns[r]).equals("上行速率(Mbps)") || ((String)columns[r]).equals("上行速率(Kbps)") || ((String)columns[r]).equals("上行速率（Mbps）") || ((String)columns[r]).equals("上行速率（Kbps）")){
									uploadSpeed = list.getString(""+columns[r]);
									ulSpeedUnit = (String)columns[r];
									continue;
								}else if(((String)columns[r]).equals("时延") || ((String)columns[r]).equals("时延(ms)") || ((String)columns[r]).equals("时延（ms）")){
									delay = list.getString(""+columns[r]);
									continue;
								}								
								//获取速率及时延信息End
								
								if(((String)columns[r]).equals("cassandra_province")){
									province = list.getString(""+columns[r]);
									continue;
								}
								if(((String)columns[r]).equals("cassandra_city")){
									city = list.getString(""+columns[r]);
									continue;
								}
								if(((String)columns[r]).equals("cassandra_district")){
									district = list.getString(""+columns[r]);
									continue;
								}
								
							}
							
							//数据格式整理
							String netResult = "";
							String dlSpeedResult = "";
							String ulSpeedResult = "";
							String delayResult = "";
							String[] gpsresult = new String[2];
							String[] g2 = {"2g","edge", "gprs", "gsm","cdma 1x"};
							String[] g3 = {"3g","hsdpa","td-scdma","tds-hsdpa","umts","hspa","hspa+","wcdma","cdma2000","evdo","evdo_a","evdo_b"};
							String[] g4 = {"lte","td-lte","fdd-lte","tdd-lte"};
							
							String[] gps = new String[2];
							if(gpsStr != null && !gpsStr.trim().equals("--") && !gpsStr.trim().equals("-") && !gpsStr.trim().equals("")){
								if(gpsStr.contains(" ")){
									gps = transGpsPoint(gpsStr);
									if(gps!=null && gps[0]!=null && gps[1]!=null){
										gpsresult = gps;
									}else{
										if(list.hasNext()){
											list.next();
										}else{
											break;
										}
										continue;
									}
								}
							}else{
								if(list.hasNext()){
									list.next();
								}else{
									break;
								}
								continue;
							}
							if(NetType1!=null && !NetType1.equals("") && !NetType1.trim().equals("-") && !NetType1.trim().equals("--") && !NetType1.trim().equals("N/A") ){
								netResult = NetType1.trim();
							}else if(NetType2!=null && !NetType2.equals("") && !NetType2.trim().equals("-") && !NetType2.trim().equals("--") && !NetType2.trim().equals("N/A") ){
								netResult = NetType2.trim();
							}else if(NetType!=null && !NetType.equals("") && !NetType.trim().equals("-") && !NetType.trim().equals("--") && !NetType.trim().equals("N/A") ){
								netResult = NetType.trim();
							}
							
							if(downloadSpeed!=null && !downloadSpeed.equals("") && !downloadSpeed.trim().equals("-") && !downloadSpeed.trim().equals("--") && !downloadSpeed.trim().equals("N/A") ){
								dlSpeedResult = downloadSpeed.trim();
							}
							if(uploadSpeed!=null && !uploadSpeed.equals("") && !uploadSpeed.trim().equals("-") && !uploadSpeed.trim().equals("--") && !uploadSpeed.trim().equals("N/A")){
								ulSpeedResult = uploadSpeed.trim();
							}
							if(delay!=null && !delay.equals("") && !delay.trim().equals("-") && !delay.trim().equals("--") && !delay.trim().equals("N/A")){
								delayResult = delay.trim();
							}
							
//							boolean isSure = false;
//							boolean is4G = false;
//							for(int g=0; g<g4.length && !isSure; g++){
//								if(netResult!=null && netResult.toLowerCase().equals(g4[g])){
//									is4G = true;
//									isSure = true;
//									break;
//								}
//							}
//							boolean is3G = false;
//							for(int g=0; g<g3.length && !isSure; g++){
//								if(netResult!=null && netResult.toLowerCase().equals(g3[g])){
//									is3G = true;
//									isSure = true;
//									break;
//								}
//							}
//							boolean is2G = false;
//							for(int g=0; g<g2.length && !isSure; g++){
//								if(netResult!=null && netResult.toLowerCase().equals(g2[g])){
//									is2G = true;
//									isSure = true;
//									break;
//								}
//							}
							
							if(type.trim().toLowerCase().equals("download") && netResult!=null && !netResult.equals("") && !netResult.trim().equals("-") && !netResult.trim().equals("--") && !netResult.trim().equals("N/A") && dlSpeedResult!=null && !dlSpeedResult.equals("") && !dlSpeedResult.trim().equals("-") && !dlSpeedResult.trim().equals("--") && !dlSpeedResult.trim().equals("N/A")){
								Map<String,Object> netResultList = (HashMap<String, Object>)result.get(netResult);
								if(netResultList==null){
									netResultList = new HashMap<String,Object>();
								}
								
								String totalType = (String)netResultList.get("count");
								int totalTypeInte = 0;
								if(totalType!=null && !"".equals(totalType)){
									totalTypeInte = Integer.parseInt(totalType);
								}
								myData.put("GPSLocation", gpsresult[0]+" "+gpsresult[1]);
								myData.put("province", province);
								myData.put("city", city);
								myData.put("district", district);
								myData.put("NetType", netResult);
								myData.put("DownloadSpeed", dlSpeedResult);
								
								double dlSpeedValue = 2000;
								try{
									if(dlSpeedResult.toLowerCase().indexOf("mbps")!=-1){
										dlSpeedValue = Double.valueOf(dlSpeedResult.toLowerCase().replace("mbps", ""));
									}else if(dlSpeedResult.toLowerCase().indexOf("kbps")!=-1){
										dlSpeedValue = Double.valueOf(dlSpeedResult.toLowerCase().replace("kbps", ""))/1024;
									}else{
										System.out.println("速率格式异常----->"+dlSpeedResult);
										if(dlSpeedUnit.toLowerCase().indexOf("kbps")!=-1){
											dlSpeedValue = Double.valueOf(dlSpeedResult.toLowerCase())/1024;
										}else{
											dlSpeedValue = Double.valueOf(dlSpeedResult.toLowerCase());
										}
									}
								}catch (Exception e){
									e.printStackTrace();
									if(list.hasNext()){
										list.next();
									}else{
										break;
									}
									continue;
								}
								
								totalTypeInte++;
								if(dlSpeedValue>=60){
									myData.put("imgUrl", "1.png");
								}else if(dlSpeedValue>=40 && dlSpeedValue<60){
									myData.put("imgUrl", "2.png");
								}else if(dlSpeedValue>=20 && dlSpeedValue<40){
									myData.put("imgUrl", "3.png");
								}else if(dlSpeedValue>=10 && dlSpeedValue<20){
									myData.put("imgUrl", "4.png");
								}else if(dlSpeedValue<10){
									myData.put("imgUrl", "5.png");
								}
//								if(is4G){
//									if(dlSpeedValue>=60){
//										myData.put("imgUrl", "1.png");
//									}else if(dlSpeedValue>=40 && dlSpeedValue<60){
//										myData.put("imgUrl", "2.png");
//									}else if(dlSpeedValue>=20 && dlSpeedValue<40){
//										myData.put("imgUrl", "3.png");
//									}else if(dlSpeedValue>=10 && dlSpeedValue<20){
//										myData.put("imgUrl", "4.png");
//									}else if(dlSpeedValue<10){
//										myData.put("imgUrl", "5.png");
//									}
//									
//								}else if(is3G){
//									if(dlSpeedValue>=10){
//										myData.put("imgUrl", "1.png");
//									}else if(dlSpeedValue>=8 && dlSpeedValue<10){
//										myData.put("imgUrl", "2.png");
//									}else if(dlSpeedValue>=5 && dlSpeedValue<8){
//										myData.put("imgUrl", "3.png");
//									}else if(dlSpeedValue>=2 && dlSpeedValue<5){
//										myData.put("imgUrl", "4.png");
//									}else if(dlSpeedValue<2){
//										myData.put("imgUrl", "5.png");
//									}
//									
//								}else if(is2G){
//									if(dlSpeedValue>=-60){
//										myData.put("imgUrl", "1.png");
//									}else if(dlSpeedValue>=-80 && dlSpeedValue<-60){
//										myData.put("imgUrl", "2.png");
//									}else if(dlSpeedValue>=-90 && dlSpeedValue<-80){
//										myData.put("imgUrl", "3.png");
//									}else if(dlSpeedValue>=-100 && dlSpeedValue<-90){
//										myData.put("imgUrl", "4.png");
//									}else if(dlSpeedValue<-100){
//										myData.put("imgUrl", "5.png");
//									}
//									
//								}
//								
								if(isDetail){
									ArrayList<HashMap<String, String>> detail = (ArrayList<HashMap<String, String>>)netResultList.get("detail");
									if(detail==null){
										detail = new ArrayList<HashMap<String, String>>();
									}
									detail.add(myData);
									netResultList.put("detail",detail);
								}
								netResultList.put("count",""+totalTypeInte);
								result.put(netResult, netResultList);
								total++;
							}
							
							if(type.trim().toLowerCase().equals("upload") && netResult!=null && !netResult.equals("") && !netResult.trim().equals("-") && !netResult.trim().equals("--") && !netResult.trim().equals("N/A") && ulSpeedResult!=null && !ulSpeedResult.equals("") && !ulSpeedResult.trim().equals("-") && !ulSpeedResult.trim().equals("--") && !ulSpeedResult.trim().equals("N/A")){
								Map<String,Object> netResultList = (HashMap<String, Object>)result.get(netResult);
								if(netResultList==null){
									netResultList = new HashMap<String,Object>();
								}
								
								String totalType = (String)netResultList.get("count");
								int totalTypeInte = 0;
								if(totalType!=null && !"".equals(totalType)){
									totalTypeInte = Integer.parseInt(totalType);
								}
								myData.put("GPSLocation", gpsresult[0]+" "+gpsresult[1]);
								myData.put("province", province);
								myData.put("city", city);
								myData.put("district", district);
								myData.put("NetType", netResult);
								myData.put("UploadSpeed", ulSpeedResult);
								
								double ulSpeedValue = 2000;
								try{
									if(ulSpeedResult.toLowerCase().indexOf("mbps")!=-1){
										ulSpeedValue = Double.valueOf(ulSpeedResult.toLowerCase().replace("mbps", ""));
									}else if(ulSpeedResult.toLowerCase().indexOf("kbps")!=-1){
										ulSpeedValue = Double.valueOf(ulSpeedResult.toLowerCase().replace("kbps", ""))/1024;
									}else{
										System.out.println("速率格式异常----->"+ulSpeedResult);
										if(ulSpeedUnit.toLowerCase().indexOf("kbps")!=-1){
											ulSpeedValue = Double.valueOf(ulSpeedResult.toLowerCase())/1024;
										}else{
											ulSpeedValue = Double.valueOf(ulSpeedResult.toLowerCase());
										}
									}
								}catch (Exception e){
									e.printStackTrace();
									if(list.hasNext()){
										list.next();
									}else{
										break;
									}
									continue;
								}
								
								totalTypeInte++;
								if(ulSpeedValue>=10){
									myData.put("imgUrl", "1.png");
								}else if(ulSpeedValue>=8 && ulSpeedValue<10){
									myData.put("imgUrl", "2.png");
								}else if(ulSpeedValue>=5 && ulSpeedValue<8){
									myData.put("imgUrl", "3.png");
								}else if(ulSpeedValue>=2 && ulSpeedValue<5){
									myData.put("imgUrl", "4.png");
								}else if(ulSpeedValue<2){
									myData.put("imgUrl", "5.png");
								}
//								
								if(isDetail){
									ArrayList<HashMap<String, String>> detail = (ArrayList<HashMap<String, String>>)netResultList.get("detail");
									if(detail==null){
										detail = new ArrayList<HashMap<String, String>>();
									}
									detail.add(myData);
									netResultList.put("detail",detail);
								}
								netResultList.put("count",""+totalTypeInte);
								result.put(netResult, netResultList);
								total++;
							}
							
							if(type.trim().toLowerCase().equals("delay") && netResult!=null && !netResult.equals("") && !netResult.trim().equals("-") && !netResult.trim().equals("--") && !netResult.trim().equals("N/A") && delayResult!=null && !delayResult.equals("") && !delayResult.trim().equals("-") && !delayResult.trim().equals("--") && !delayResult.trim().equals("N/A")){
								Map<String,Object> netResultList = (HashMap<String, Object>)result.get(netResult);
								if(netResultList==null){
									netResultList = new HashMap<String,Object>();
								}
								
								String totalType = (String)netResultList.get("count");
								int totalTypeInte = 0;
								if(totalType!=null && !"".equals(totalType)){
									totalTypeInte = Integer.parseInt(totalType);
								}
								myData.put("GPSLocation", gpsresult[0]+" "+gpsresult[1]);
								myData.put("province", province);
								myData.put("city", city);
								myData.put("district", district);
								myData.put("NetType", netResult);
								myData.put("Delay", delayResult);
								
								double delayValue = 2000;
								try{
									if(delayResult.toLowerCase().indexOf("ms")!=-1){
										delayValue = Double.valueOf(delayResult.toLowerCase().replace("ms", ""));
									}else{
										System.out.println("时延数据格式----->"+ulSpeedResult);
										delayValue = Double.valueOf(delayResult.toLowerCase());
									}
								}catch (Exception e){
									e.printStackTrace();
									if(list.hasNext()){
										list.next();
									}else{
										break;
									}
									continue;
								}
								
								totalTypeInte++;
								if(delayValue>=200){
									myData.put("imgUrl", "5.png");
								}else if(delayValue>=100 && delayValue<200){
									myData.put("imgUrl", "4.png");
								}else if(delayValue>=60 && delayValue<100){
									myData.put("imgUrl", "3.png");
								}else if(delayValue>=30 && delayValue<60){
									myData.put("imgUrl", "2.png");
								}else if(delayValue<30){
									myData.put("imgUrl", "1.png");
								}
//								
								if(isDetail){
									ArrayList<HashMap<String, String>> detail = (ArrayList<HashMap<String, String>>)netResultList.get("detail");
									if(detail==null){
										detail = new ArrayList<HashMap<String, String>>();
									}
									detail.add(myData);
									netResultList.put("detail",detail);
								}
								netResultList.put("count",""+totalTypeInte);
								result.put(netResult, netResultList);
								total++;
							}
							
							if(list.hasNext()){
								list.next();
							}else{
								break;
							}
						}
					}
				}catch (Exception e){
					e.printStackTrace();
					continue;
				}	
			}
		}
		
		
		System.out.println("总处理数据量"+total);
		result.put("total", total);
		return result;
	}
	
	/**
	 * 获取网络测速的小区测试信息地图
	 * @author Ocean
	 * @param type  指下行速率、上行速率、时延
	 * @param keyspaces
	 * @param qe
	 * @param startKey
	 * @param count
	 * @return
	 */
	public Map<String,Object> getSpeedtestAnalyzeData(JSONArray keyspaces, List<ArrayList<QueryExpression>> qes, String startKey, int count){
		Map<String,Object> result = new HashMap<String,Object> ();
		
		List<String> columnsPredict = new ArrayList<String>();
		columnsPredict.add("网络(1)网络制式");
		columnsPredict.add("网络(2)网络制式");
		columnsPredict.add("网络（1）网络制式");
		columnsPredict.add("网络（2）网络制式");
		columnsPredict.add("网络制式");
		columnsPredict.add("网络类型");
		
		columnsPredict.add("网络(1)信号强度");
		columnsPredict.add("网络(2)信号强度");
		columnsPredict.add("网络（1）信号强度");
		columnsPredict.add("网络（2）信号强度");
		columnsPredict.add("信号强度");

		columnsPredict.add("GPS位置");
		columnsPredict.add("GPS位置信息");
		columnsPredict.add("GPS信息");
		columnsPredict.add("测试位置");
		columnsPredict.add("测试GPS位置");
		
		columnsPredict.add("下行速率");
		columnsPredict.add("上行速率");
		columnsPredict.add("时延");
		
		columnsPredict.add("时延(ms)");
		columnsPredict.add("下行速率(Mbps)");
		columnsPredict.add("上行速率(Mbps)");
		columnsPredict.add("下行速率(Kbps)");
		columnsPredict.add("上行速率(Kbps)");
		
		columnsPredict.add("时延（ms）");
		columnsPredict.add("下行速率（Mbps）");
		columnsPredict.add("上行速率（Mbps）");
		columnsPredict.add("下行速率（Kbps）");
		columnsPredict.add("上行速率（Kbps）");
		
		columnsPredict.add("TAC/PCI");
		columnsPredict.add("LAC/CID");
		columnsPredict.add("TAC");
		columnsPredict.add("PCI");
		columnsPredict.add("LAC");
		columnsPredict.add("CID");
		columnsPredict.add("网络(1)小区信息");
		columnsPredict.add("网络(2)小区信息");
		columnsPredict.add("网络（1）小区信息");
		columnsPredict.add("网络（2）小区信息");

		columnsPredict.add("cassandra_province");
		columnsPredict.add("cassandra_city");
		columnsPredict.add("cassandra_district");
		int defaultStep = 200;
		int total = 0;
		boolean limit = false;
		if(count>-1){
			limit = true;
			if(defaultStep>=count){
				defaultStep = count;
			}
		}
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

				try{
					ColumnFamilyTemplate<String, String> columnFamilyTemplate = new ThriftColumnFamilyTemplate<String, String>(
							keyspaceOperator, "01001", KeySpaceFactory.stringSerializer, KeySpaceFactory.stringSerializer);
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
									.queryColumns(dataPredicate,columnsPredict);
						}catch (Exception e){
							e.printStackTrace();
							continue;
						}
						
						while (list.hasResults()) {
							if(lastKey.equals(list.getKey())){
								if(list.hasNext()){
									list.next();
								}else{
									break;
								}
								continue;
							}
							lastKey = list.getKey();
							Collection<String> element = list.getColumnNames();
							Object[] columns = element.toArray();
							stepCount++;
							
							if(total==count){
								break;
							}

							int previGps = 0;
							int previNetType = 0;
							String gpsStr = "";
							String NetType1 = "";
							String NetType2 = "";
							String NetType = "";
							String downloadSpeed = "";
							String uploadSpeed = "";
							String dlSpeedUnit = "";
							String ulSpeedUnit = "";
							String delay = "";
							String signalStength1 = "";
							String signalStength2 = "";
							String signalStength = "";
							String cellInfo1 = "";
							String cellInfo2 = "";
							String cellInfo = "";
							String lac = "";
							String cid = "";
							String tac = "";
							String pci = "";
							String tac_pci = "";
							String lac_cid = "";
							for(int r=0; r<columns.length; r++){
								
								//获取网络制式及信号强度Start
								if(((String)columns[r]).equals("网络(1)网络制式") || ((String)columns[r]).equals("网络（1）网络制式")){
									NetType1 = list.getString(""+columns[r]);
									continue;
								}else if(((String)columns[r]).equals("网络(2)网络制式") || ((String)columns[r]).equals("网络（2）网络制式")){
									NetType2 = list.getString(""+columns[r]);
									continue;
								}else if(((String)columns[r]).equals("网络制式")){
									if(previNetType<=2){
										previNetType = 2;
										NetType = list.getString(""+columns[r]);
									}
									continue;
								}else if(((String)columns[r]).equals("网络类型")){
									if(previNetType<=1){
										previNetType = 1;
										NetType = list.getString(""+columns[r]);
									}
									continue;
								}
								
								if(((String)columns[r]).equals("网络(1)信号强度") || ((String)columns[r]).equals("网络（1）信号强度")){
									signalStength1 = list.getString(""+columns[r]);
									continue;
								}else if(((String)columns[r]).equals("网络(2)信号强度") || ((String)columns[r]).equals("网络（2）信号强度")){
									signalStength2 = list.getString(""+columns[r]);
									continue;
								}else if(((String)columns[r]).equals("信号强度")){
									if(previNetType<=1){
										previNetType = 1;
										signalStength = list.getString(""+columns[r]);
									}
									continue;
								}
								//获取网络制式及信号强度End
								
								//获取GPS位置信息Start
								if(((String)columns[r]).equals("GPS位置")){
									if(previGps<5){
										previGps = 5;
										gpsStr = list.getString(""+columns[r]);
									}
									continue;
								}else if(((String)columns[r]).equals("GPS位置信息")){
									if(previGps<4){
										previGps = 4;
										gpsStr = list.getString(""+columns[r]);
									}
									continue;
								}else if(((String)columns[r]).equals("GPS信息")){
									if(previGps<3){
										previGps = 3;
										gpsStr = list.getString(""+columns[r]);
									}
									continue;
								}else if(((String)columns[r]).equals("测试位置")){
									if(previGps<2){
										previGps = 3;
										gpsStr = list.getString(""+columns[r]);
									}
									continue;
								}else if(((String)columns[r]).equals("测试GPS位置")){
									if(previGps<1){
										previGps = 3;
										gpsStr = list.getString(""+columns[r]);
									}
									continue;
								}									
								//获取GPS位置信息End
								
								
								//获取速率及时延信息Start
								if(((String)columns[r]).equals("下行速率") || ((String)columns[r]).equals("下行速率(Mbps)") || ((String)columns[r]).equals("下行速率（Mbps）") || ((String)columns[r]).equals("下行速率(Kbps)") || ((String)columns[r]).equals("下行速率（Kbps）")){
									downloadSpeed = list.getString(""+columns[r]);
									dlSpeedUnit = (String)columns[r];
									continue;
								}else if(((String)columns[r]).equals("上行速率") || ((String)columns[r]).equals("上行速率(Mbps)") || ((String)columns[r]).equals("上行速率(Kbps)") || ((String)columns[r]).equals("上行速率（Mbps）") || ((String)columns[r]).equals("上行速率（Kbps）")){
									uploadSpeed = list.getString(""+columns[r]);
									ulSpeedUnit = (String)columns[r];
									continue;
								}else if(((String)columns[r]).equals("时延") || ((String)columns[r]).equals("时延(ms)") || ((String)columns[r]).equals("时延（ms）")){
									delay = list.getString(""+columns[r]);
									continue;
								}								
								//获取速率及时延信息End
								
								//获取小区信息Start
								if(((String)columns[r]).equals("网络(1)小区信息") || ((String)columns[r]).equals("网络（1）小区信息")){
									cellInfo1 = list.getString(""+columns[r]);
									continue;
								}else if(((String)columns[r]).equals("网络(2)小区信息") || ((String)columns[r]).equals("网络（2）小区信息")){
									cellInfo2 = list.getString(""+columns[r]);
									continue;
								}else if(((String)columns[r]).equals("小区信息")){
									cellInfo = list.getString(""+columns[r]);
									continue;
								}
								if(((String)columns[r]).toLowerCase().equals("tac/pci")){
									tac_pci = list.getString(""+columns[r]);
									continue;
								}else if(((String)columns[r]).toLowerCase().equals("lac/cid")){
									lac_cid = list.getString(""+columns[r]);
									continue;
								}else if(((String)columns[r]).toLowerCase().equals("lac")){
									lac = list.getString(""+columns[r]);
									continue;
								}else if(((String)columns[r]).toLowerCase().equals("cid")){
									cid = list.getString(""+columns[r]);
									continue;
								}else if(((String)columns[r]).toLowerCase().equals("tac")){
									tac = list.getString(""+columns[r]);
									continue;
								}else if(((String)columns[r]).toLowerCase().equals("pci")){
									pci = list.getString(""+columns[r]);
									continue;
								}
								//获取速率及时延信息End
								
							}
							
							//数据格式整理
							String netResult = "";
							String signalResult = "";
							String dlSpeedResult = "";
							String ulSpeedResult = "";
							String delayResult = "";
							String cellResult = "";
							String cellResult2G = "";
							String[] gpsresult = new String[2];
							String[] g2 = {"2g","edge", "gprs", "gsm","cdma 1x"};
							String[] g3 = {"3g","hsdpa","td-scdma","tds-hsdpa","umts","hspa","hspa+","wcdma","cdma2000","evdo","evdo_a","evdo_b"};
							String[] g4 = {"lte","td-lte","fdd-lte","tdd-lte"};
							
							String[] gps = {"",""};
							if(gpsStr.contains(" ")){
								gps = transGpsPoint(gpsStr);
								if(gps!=null && gps[0]!=null && gps[1]!=null){
									gpsresult = gps;
								}
							}
							
							if(NetType1!=null && !NetType1.equals("") && !NetType1.trim().equals("-") && !NetType1.trim().equals("--") && !NetType1.trim().equals("N/A")){
								netResult = NetType1.trim();
								signalResult = signalStength1.trim();
								cellResult = cellInfo1.trim();
							}else if(NetType2!=null && !NetType2.equals("") && !NetType2.trim().equals("-") && !NetType2.trim().equals("--") && !NetType2.trim().equals("N/A")){
								netResult = NetType2.trim();
								signalResult = signalStength2.trim();
								cellResult = cellInfo2.trim();
							}else if(NetType!=null && !NetType.equals("") && !NetType.trim().equals("-") && !NetType.trim().equals("--") && !NetType.trim().equals("N/A")){
								netResult = NetType.trim();
								signalResult = signalStength.trim();
								cellResult = cellInfo.trim();
							}
							
							if(cellResult!=null && !cellResult.equals("") && !cellResult.trim().equals("-") && !cellResult.trim().equals("--") && !cellResult.trim().equals("N/A") && !cellResult.trim().equals("N/A/N/A")){
								
							}else{
								if(netResult.equals("LTE") && tac_pci!=null && !tac_pci.equals("") && !tac_pci.trim().equals("-") && !tac_pci.trim().equals("--") && !tac_pci.trim().equals("N/A") && !tac_pci.trim().equals("N/A/N/A")){
									cellResult = tac_pci.trim();
								}else if(netResult.equals("LTE") && tac!=null && !tac.equals("") && !tac.trim().equals("-") && !tac.trim().equals("--") && !tac.trim().equals("N/A") && !(tac.trim().toLowerCase().indexOf("n/a")!=-1) && pci!=null && !pci.equals("") && !pci.trim().equals("-") && !pci.trim().equals("--") && !pci.trim().equals("N/A") && !(pci.trim().toLowerCase().indexOf("n/a")!=-1)){
									if(tac.toLowerCase().indexOf("tac")!=-1){
										tac = tac.toLowerCase().replace("tac=", "");
									}
									if(pci.toLowerCase().indexOf("pci")!=-1){
										pci = pci.toLowerCase().replace("pci=", "");
									}
									cellResult = tac+"/"+pci;
								}else if(lac_cid!=null && !lac_cid.equals("") && !lac_cid.trim().equals("-") && !lac_cid.trim().equals("--") && !lac_cid.trim().equals("N/A") && !lac_cid.trim().equals("N/A/N/A")){
									cellResult2G = lac_cid.trim();
								}else if(lac!=null && !lac.equals("") && !lac.trim().equals("-") && !lac.trim().equals("--") && !lac.trim().equals("N/A") && !(lac.trim().toLowerCase().indexOf("n/a")!=-1) && cid!=null && !cid.equals("") && !cid.trim().equals("-") && !cid.trim().equals("--") && !cid.trim().equals("N/A") && !(cid.trim().toLowerCase().indexOf("n/a")!=-1)){
									if(lac.toLowerCase().indexOf("lac")!=-1){
										lac = lac.toLowerCase().replace("lac=", "");
									}
									if(cid.toLowerCase().indexOf("cid")!=-1){
										lac = lac.toLowerCase().replace("cid=", "");
									}
									cellResult2G = lac+"/"+cid;
								}
							}
							
							
							if(downloadSpeed!=null && !downloadSpeed.equals("") && !downloadSpeed.trim().equals("-") && !downloadSpeed.trim().equals("--") && !downloadSpeed.trim().equals("N/A") ){
								dlSpeedResult = downloadSpeed.trim();
							}
							if(uploadSpeed!=null && !uploadSpeed.equals("") && !uploadSpeed.trim().equals("-") && !uploadSpeed.trim().equals("--") && !uploadSpeed.trim().equals("N/A")){
								ulSpeedResult = uploadSpeed.trim();
							}
							if(delay!=null && !delay.equals("") && !delay.trim().equals("-") && !delay.trim().equals("--") && !delay.trim().equals("N/A")){
								delayResult = delay.trim();
							}
							
							if(netResult!=null && !netResult.trim().equals("LTE") && cellResult!=null && !cellResult.equals("") && !cellResult.trim().equals("-") && !cellResult.trim().equals("--") && !cellResult.trim().equals("N/A")){
								cellResult = cellResult;
							}else if(netResult!=null && !netResult.trim().equals("LTE") && cellResult2G!=null && !cellResult2G.equals("") && !cellResult2G.trim().equals("-") && !cellResult2G.trim().equals("--") && !cellResult2G.trim().equals("N/A")){
								cellResult = cellResult2G;
							}else if(cellResult!=null && !cellResult.equals("") && !cellResult.trim().equals("-") && !cellResult.trim().equals("--") && !cellResult.trim().equals("N/A")){
								cellResult = cellResult;
							}else if(cellResult2G!=null && !cellResult2G.equals("") && !cellResult2G.trim().equals("-") && !cellResult2G.trim().equals("--") && !cellResult2G.trim().equals("N/A")){
								cellResult = cellResult2G;
							}
							
							if(netResult!=null && !netResult.equals("") && !netResult.trim().equals("-") && !netResult.trim().equals("--") && !netResult.trim().equals("N/A") && cellResult!=null && !cellResult.equals("") && !cellResult.trim().equals("-") && !cellResult.trim().equals("--") && !cellResult.trim().equals("N/A")){
								int signalLevel = 0;
								Map<String,Object> netResultList = (HashMap<String, Object>)result.get(netResult);
								if(netResultList==null){
									netResultList = new HashMap<String,Object>();
								}
								
								Map<String,Object> cellList = (Map<String,Object>)netResultList.get("detail");
								if(cellList==null){
									cellList = new HashMap<String,Object>();
								}
								Map<String,Object> cellMetaInfoMap = (Map<String,Object>)cellList.get(cellResult);
								if(cellMetaInfoMap==null){
									cellMetaInfoMap = new HashMap<String,Object>();
									String totalType = (String)netResultList.get("count");
									int totalTypeInte = 0;
									if(totalType!=null && !"".equals(totalType)){
										totalTypeInte = Integer.parseInt(totalType);
									}
									totalTypeInte++;
									netResultList.put("count",""+totalTypeInte);
									total++;
								}
								
								int signalValue = 2000;
								try{
									signalValue = Integer.parseInt(signalResult.toLowerCase().replace("dbm", ""));
								}catch (Exception e){
									e.printStackTrace();
									if(list.hasNext()){
										list.next();
									}else{
										break;
									}
									continue;
								}
								
								if(signalValue>=-80){
									signalLevel = 2;
								}else if(signalValue<-80 && signalValue>=-95){
									signalLevel = 1;
								}else{
									signalLevel = 0;
								}
								
								Map<String,Object> signalMap = null;
								
								if(signalLevel==2){
									signalMap = (Map<String,Object>)cellMetaInfoMap.get("good");
								}else if(signalLevel==1){
									signalMap = (Map<String,Object>)cellMetaInfoMap.get("medium");
								}else{
									signalMap = (Map<String,Object>)cellMetaInfoMap.get("bad");
								}
								
								
								if(signalMap==null){
									signalMap = new HashMap<String,Object>();
								}
								Map<String,Object> cellInfoMap = signalMap;
//								Map<String,Object> cellInfoMap = (Map<String,Object>)signalMap.get(cellResult);
//								if(cellInfoMap==null){
//									cellInfoMap = new HashMap<String,Object>();
//								}
								
								double dlSpeedValue = 0;
								try{
									if(dlSpeedResult.toLowerCase().indexOf("mbps")!=-1){
										dlSpeedValue = Double.valueOf(dlSpeedResult.toLowerCase().replace("mbps", ""));
									}else if(dlSpeedResult.toLowerCase().indexOf("kbps")!=-1){
										dlSpeedValue = Double.valueOf(dlSpeedResult.toLowerCase().replace("kbps", ""))/1024;
									}else{
										System.out.println("速率格式异常----->"+dlSpeedResult);
										if(dlSpeedUnit.toLowerCase().indexOf("kbps")!=-1){
											dlSpeedValue = Double.valueOf(dlSpeedResult.toLowerCase())/1024;
										}else{
											dlSpeedValue = Double.valueOf(dlSpeedResult.toLowerCase());
										}
									}
								}catch (Exception e){
									e.printStackTrace();
									if(list.hasNext()){
										list.next();
									}else{
										break;
									}
									continue;
								}
								
								double ulSpeedValue = 0;
								try{
									if(ulSpeedResult.toLowerCase().indexOf("mbps")!=-1){
										ulSpeedValue = Double.valueOf(ulSpeedResult.toLowerCase().replace("mbps", ""));
									}else if(ulSpeedResult.toLowerCase().indexOf("kbps")!=-1){
										ulSpeedValue = Double.valueOf(ulSpeedResult.toLowerCase().replace("kbps", ""))/1024;
									}else{
										System.out.println("速率格式异常----->"+ulSpeedResult);
										if(ulSpeedUnit.toLowerCase().indexOf("kbps")!=-1){
											ulSpeedValue = Double.valueOf(ulSpeedResult.toLowerCase())/1024;
										}else{
											ulSpeedValue = Double.valueOf(ulSpeedResult.toLowerCase());
										}
									}
								}catch (Exception e){
									e.printStackTrace();
									if(list.hasNext()){
										list.next();
									}else{
										break;
									}
									continue;
								}
								
								double delayValue = 0;
								try{
									if(delayResult.toLowerCase().indexOf("ms")!=-1){
										delayValue = Double.valueOf(delayResult.toLowerCase().replace("ms", ""));
									}else{
										System.out.println("时延数据格式----->"+ulSpeedResult);
										delayValue = Double.valueOf(delayResult.toLowerCase());
									}
								}catch (Exception e){
									e.printStackTrace();
									if(list.hasNext()){
										list.next();
									}else{
										break;
									}
									continue;
								}
								
								double meanDownSpeedHistoryDouble = 0;
								double maxDownSpeedHistoryDouble = 0;
								double meanUpSpeedHistoryDouble = 0;
								double maxUpSpeedHistoryDouble = 0;
								double meanDelayHistoryDouble = 0;
								double maxDelayHistoryDouble = 0;
								double minDelayHistoryDouble = 0;
								
								String maxDownSpeedHistory = (String)cellInfoMap.get("max_download");
								if(maxDownSpeedHistory!=null && !"".equals(maxDownSpeedHistory)){
									maxDownSpeedHistory = maxDownSpeedHistory.toLowerCase().replace("mbps", "").replace("kbps", "");
									maxDownSpeedHistoryDouble = Double.valueOf(maxDownSpeedHistory);
								}
								String meanDownSpeedHistory = (String)cellInfoMap.get("mean_download");
								if(meanDownSpeedHistory!=null && !"".equals(meanDownSpeedHistory)){
									meanDownSpeedHistory = meanDownSpeedHistory.toLowerCase().replace("mbps", "").replace("kbps", "");
									meanDownSpeedHistoryDouble = Double.valueOf(meanDownSpeedHistory);
								}
								String maxUpSpeedHistory = (String)cellInfoMap.get("max_upload");
								if(maxUpSpeedHistory!=null && !"".equals(maxUpSpeedHistory)){
									maxUpSpeedHistory = maxUpSpeedHistory.toLowerCase().replace("mbps", "").replace("kbps", "");
									maxUpSpeedHistoryDouble = Double.valueOf(maxUpSpeedHistory);
								}
								String meanUpSpeedHistory = (String)cellInfoMap.get("mean_upload");
								if(meanUpSpeedHistory!=null && !"".equals(meanUpSpeedHistory)){
									meanUpSpeedHistory = meanUpSpeedHistory.toLowerCase().replace("mbps", "").replace("kbps", "");
									meanUpSpeedHistoryDouble = Double.valueOf(meanUpSpeedHistory);
								}
								String maxDelaySpeedHistory = (String)cellInfoMap.get("max_delay");
								if(maxDelaySpeedHistory!=null && !"".equals(maxDelaySpeedHistory)){
									maxDelaySpeedHistory = maxDelaySpeedHistory.toLowerCase().replace("ms", "");
									maxDelayHistoryDouble = Double.valueOf(maxDelaySpeedHistory);
								}
								String minDelaySpeedHistory = (String)cellInfoMap.get("min_delay");
								if(minDelaySpeedHistory!=null && !"".equals(minDelaySpeedHistory)){
									minDelaySpeedHistory = minDelaySpeedHistory.toLowerCase().replace("ms", "");
									minDelayHistoryDouble = Double.valueOf(minDelaySpeedHistory);
								}
								String meanDelaySpeedHistory = (String)cellInfoMap.get("mean_delay");
								if(meanDelaySpeedHistory!=null && !"".equals(meanDelaySpeedHistory)){
									meanDelaySpeedHistory = meanDelaySpeedHistory.toLowerCase().replace("ms", "");
									meanDelayHistoryDouble = Double.valueOf(meanDelaySpeedHistory);
								}
								
								String totalType = (String)cellInfoMap.get("count");
								int totalTypeInte = 0;
								if(totalType!=null && !"".equals(totalType)){
									totalTypeInte = Integer.parseInt(totalType);
								}
								totalTypeInte++;
								
								if(dlSpeedValue>maxDownSpeedHistoryDouble){
									cellInfoMap.put("max_download", ""+dlSpeedValue+"Mbps");
								}
								double meanSpeedValueNow = (meanDownSpeedHistoryDouble*(totalTypeInte-1)+dlSpeedValue)/totalTypeInte;
								cellInfoMap.put("mean_download", ""+meanSpeedValueNow+"Mbps");
								
								if(ulSpeedValue>maxUpSpeedHistoryDouble){
									cellInfoMap.put("max_upload", ""+ulSpeedValue+"Mbps");
								}
								double meanUpSpeedValueNow = (meanUpSpeedHistoryDouble*(totalTypeInte-1)+ulSpeedValue)/totalTypeInte;
								cellInfoMap.put("mean_upload", ""+meanUpSpeedValueNow+"Mbps");
								
								if(delayValue>maxDelayHistoryDouble){
									cellInfoMap.put("max_delay", ""+delayValue+"ms");
								}
								if(delayValue<minDelayHistoryDouble){
									cellInfoMap.put("max_delay", ""+delayValue+"ms");
								}
								double meanDelaySpeedValueNow = (meanDelayHistoryDouble*(totalTypeInte-1)+delayValue)/totalTypeInte;
								cellInfoMap.put("mean_delay", ""+meanDelaySpeedValueNow+"ms");
								
								cellInfoMap.put("count", ""+totalTypeInte);
								//signalMap.put(cellResult,cellInfoMap);
								signalMap = cellInfoMap;
								if(signalLevel==2){
									cellMetaInfoMap.put("good",signalMap);
								}else if(signalLevel==1){
									cellMetaInfoMap.put("medium",signalMap);
								}else if(signalLevel==0){
									cellMetaInfoMap.put("bad",signalMap);
								}
								cellList.put(cellResult,cellMetaInfoMap);
								netResultList.put("detail",cellList);
								
								result.put(netResult, netResultList);
							}
														
							if(list.hasNext()){
								list.next();
							}else{
								break;
							}
						}
					}
				}catch (Exception e){
					e.printStackTrace();
					continue;
				}	
			}
		}
		
		
		System.out.println("总处理数据量"+total);
		result.put("total", total);
		return result;
	}
	
	/**
	 * 获取HTTP测试的业务质量地图
	 * @author Ocean
	 * @param type  指下行速率、上行速率、时延
	 * @param keyspaces
	 * @param qe
	 * @param startKey
	 * @param count
	 * @return
	 */
	public Map<String,Object> getHttpTestData(String type, JSONArray keyspaces, List<ArrayList<QueryExpression>> qes, String startKey, int count, boolean isDetail){
		Map<String,Object> result = new HashMap<String,Object> ();
		
		List<String> columnsPredict = new ArrayList<String>();
		columnsPredict.add("网络(1)网络制式");
		columnsPredict.add("网络(2)网络制式");
		columnsPredict.add("网络（1）网络制式");
		columnsPredict.add("网络（2）网络制式");
		columnsPredict.add("网络制式");
		columnsPredict.add("网络类型");

		columnsPredict.add("GPS位置");
		columnsPredict.add("GPS位置信息");
		columnsPredict.add("GPS信息");
		columnsPredict.add("测试位置");
		columnsPredict.add("测试GPS位置");
		
		columnsPredict.add("协议");
		columnsPredict.add("类型");
		
		columnsPredict.add("平均时延(ms)");
		columnsPredict.add("平均时延（ms）");
		columnsPredict.add("最大速率(Mbps)");
		columnsPredict.add("平均速率(Mbps)");
		columnsPredict.add("最大速率（Mbps）");
		columnsPredict.add("平均速率（Mbps）");
		columnsPredict.add("最大速率(Kbps)");
		columnsPredict.add("平均速率(Kbps)");
		columnsPredict.add("最大速率（Kbps）");
		columnsPredict.add("平均速率（Kbps）");

		columnsPredict.add("cassandra_province");
		columnsPredict.add("cassandra_city");
		columnsPredict.add("cassandra_district");
		int defaultStep = 200;
		int total = 0;
		boolean limit = false;
		if(count>-1){
			limit = true;
			if(defaultStep>=count){
				defaultStep = count;
			}
		}
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

				try{
					ColumnFamilyTemplate<String, String> columnFamilyTemplate = new ThriftColumnFamilyTemplate<String, String>(
							keyspaceOperator, "03001", KeySpaceFactory.stringSerializer, KeySpaceFactory.stringSerializer);
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
									.queryColumns(dataPredicate,columnsPredict);
						}catch (Exception e){
							e.printStackTrace();
							continue;
						}
						
						while (list.hasResults()) {
							if(lastKey.equals(list.getKey())){
								if(list.hasNext()){
									list.next();
								}else{
									break;
								}
								continue;
							}
							lastKey = list.getKey();
							HashMap<String, String> myData = new HashMap<String, String>();
							Collection<String> element = list.getColumnNames();
							Object[] columns = element.toArray();
							stepCount++;
							
							if(total==count){
								break;
							}

							int previGps = 0;
							int previNetType = 0;
							String gpsStr = "";
							String NetType1 = "";
							String NetType2 = "";
							String NetType = "";
							String protocal = "";
							String testType = "";
							String meanSpeedUnit = "";
							String maxSpeedUnit = "";
							String meanSpeed = "";
							String maxSpeed = "";
							String delay = "";
							
							String province = "";
							String city = "";
							String district = "";
							for(int r=0; r<columns.length; r++){
								
								//获取网络制式及信号强度Start
								if(((String)columns[r]).equals("网络(1)网络制式") || ((String)columns[r]).equals("网络（1）网络制式")){
									NetType1 = list.getString(""+columns[r]);
									continue;
								}else if(((String)columns[r]).equals("网络(2)网络制式") || ((String)columns[r]).equals("网络（2）网络制式")){
									NetType2 = list.getString(""+columns[r]);
									continue;
								}else if(((String)columns[r]).equals("网络制式")){
									if(previNetType<=2){
										previNetType = 2;
										NetType = list.getString(""+columns[r]);
									}
									continue;
								}else if(((String)columns[r]).equals("网络类型")){
									if(previNetType<=1){
										previNetType = 1;
										NetType = list.getString(""+columns[r]);
									}
									continue;
								}
								//获取网络制式End
								
								//获取GPS位置信息Start
								if(((String)columns[r]).equals("GPS位置")){
									if(previGps<5){
										previGps = 5;
										gpsStr = list.getString(""+columns[r]);
									}
									continue;
								}else if(((String)columns[r]).equals("GPS位置信息")){
									if(previGps<4){
										previGps = 4;
										gpsStr = list.getString(""+columns[r]);
									}
									continue;
								}else if(((String)columns[r]).equals("GPS信息")){
									if(previGps<3){
										previGps = 3;
										gpsStr = list.getString(""+columns[r]);
									}
									continue;
								}else if(((String)columns[r]).equals("测试位置")){
									if(previGps<2){
										previGps = 3;
										gpsStr = list.getString(""+columns[r]);
									}
									continue;
								}else if(((String)columns[r]).equals("测试GPS位置")){
									if(previGps<1){
										previGps = 3;
										gpsStr = list.getString(""+columns[r]);
									}
									continue;
								}									
								//获取GPS位置信息End
								
								
								//获取速率及时延信息Start
								if(((String)columns[r]).equals("平均速率(Mbps)") || ((String)columns[r]).equals("平均速率(Kbps)") || ((String)columns[r]).equals("平均速率（Mbps）") || ((String)columns[r]).equals("平均速率（Kbps）") || ((String)columns[r]).equals("平均速率")){
									meanSpeedUnit = (String)columns[r];
									meanSpeed = list.getString(""+columns[r]);
									continue;
								}else if(((String)columns[r]).equals("最大速率(Mbps)") || ((String)columns[r]).equals("最大速率(Kbps)") ||((String)columns[r]).equals("最大速率（Mbps）") || ((String)columns[r]).equals("最大速率（Kbps）") || ((String)columns[r]).equals("最大速率")){
									maxSpeedUnit = (String)columns[r];
									maxSpeed = list.getString(""+columns[r]);
									continue;
								}else if(((String)columns[r]).equals("协议")){
									protocal = list.getString(""+columns[r]);
									continue;
								}else if(((String)columns[r]).equals("类型")){
									testType = list.getString(""+columns[r]);
									continue;
								}else if(((String)columns[r]).equals("平均时延(ms)") || ((String)columns[r]).equals("平均时延（ms）") || ((String)columns[r]).equals("平均时延") || ((String)columns[r]).equals("时延") || ((String)columns[r]).equals("时延(ms)")){
									delay = list.getString(""+columns[r]);
									continue;
								}							
								//获取速率及时延信息End
								
								if(((String)columns[r]).equals("cassandra_province")){
									province = list.getString(""+columns[r]);
									continue;
								}
								if(((String)columns[r]).equals("cassandra_city")){
									city = list.getString(""+columns[r]);
									continue;
								}
								if(((String)columns[r]).equals("cassandra_district")){
									district = list.getString(""+columns[r]);
									continue;
								}
								
							}
							
							//数据格式整理
							String netResult = "";
							String meanSpeedResult = "";
							String maxSpeedResult = "";
							String delayResult = "";
							String[] gpsresult = new String[2];
							String[] g2 = {"2g","edge", "gprs", "gsm","cdma 1x"};
							String[] g3 = {"3g","hsdpa","td-scdma","tds-hsdpa","umts","hspa","hspa+","wcdma","cdma2000","evdo","evdo_a","evdo_b"};
							String[] g4 = {"lte","td-lte","fdd-lte","tdd-lte"};
							
							String[] gps = new String[2];
							if(gpsStr != null && !gpsStr.trim().equals("--") && !gpsStr.trim().equals("-") && !gpsStr.trim().equals("")){
								if(gpsStr.contains(" ")){
									gps = transGpsPoint(gpsStr);
									if(gps!=null && gps[0]!=null && gps[1]!=null){
										gpsresult = gps;
									}else{
										if(list.hasNext()){
											list.next();
										}else{
											break;
										}
										continue;
									}
								}
							}else{
								if(list.hasNext()){
									list.next();
								}else{
									break;
								}
								continue;
							}
							if(NetType1!=null && !NetType1.equals("") && !NetType1.trim().equals("-") && !NetType1.trim().equals("--") && !netResult.trim().equals("N/A") ){
								netResult = NetType1.trim();
							}else if(NetType2!=null && !NetType2.equals("") && !NetType2.trim().equals("-") && !NetType2.trim().equals("--") && !netResult.trim().equals("N/A") ){
								netResult = NetType2.trim();
							}else if(NetType!=null && !NetType.equals("") && !NetType.trim().equals("-") && !NetType.trim().equals("--") && !netResult.trim().equals("N/A") ){
								netResult = NetType.trim();
							}
							
							if(meanSpeed!=null && !meanSpeed.equals("") && !meanSpeed.trim().equals("-") && !meanSpeed.trim().equals("--") && !meanSpeed.trim().equals("N/A") ){
								meanSpeedResult = meanSpeed.trim();
							}
							if(maxSpeed!=null && !maxSpeed.equals("") && !maxSpeed.trim().equals("-") && !maxSpeed.trim().equals("--") && !maxSpeed.trim().equals("N/A")){
								maxSpeedResult = maxSpeed.trim();
							}
							if(delay!=null && !delay.equals("") && !delay.trim().equals("-") && !delay.trim().equals("--") && !delay.trim().equals("N/A")){
								delayResult = delay.trim();
							}
							
							if((((type.trim().toLowerCase().equals("download") && (testType.toLowerCase().equals("下载")||testType.toLowerCase().equals("download")))) || ((type.trim().toLowerCase().equals("upload") && (testType.toLowerCase().equals("上传")||testType.toLowerCase().equals("upload"))))) && netResult!=null && !netResult.equals("") && !netResult.trim().equals("-") && !netResult.trim().equals("--") && !netResult.trim().equals("N/A") && maxSpeedResult!=null && !maxSpeedResult.equals("") && !maxSpeedResult.trim().equals("-") && !maxSpeedResult.trim().equals("--") && !maxSpeedResult.trim().equals("N/A")){
								Map<String,Object> netResultList = (HashMap<String, Object>)result.get(netResult);
								if(netResultList==null){
									netResultList = new HashMap<String,Object>();
								}
								
								String totalType = (String)netResultList.get("count");
								int totalTypeInte = 0;
								if(totalType!=null && !"".equals(totalType)){
									totalTypeInte = Integer.parseInt(totalType);
								}
								
								myData.put("GPSLocation", gpsresult[0]+" "+gpsresult[1]);
								myData.put("province", province);
								myData.put("city", city);
								myData.put("district", district);
								myData.put("NetType", netResult);
								myData.put("Protocal", protocal);
								myData.put("Type", testType);
								myData.put("MaxSpeed", maxSpeedResult);
								myData.put("MeanSpeed", meanSpeedResult);
								myData.put("Delay", delayResult);
								
								double meanSpeedValue = 0;
								double meanSpeedHistoryDouble = 0;
								double maxSpeedHistoryDouble = 0;
								try{
									if(meanSpeedResult.toLowerCase().indexOf("mbps")!=-1){
										meanSpeedValue = Double.valueOf(meanSpeedResult.toLowerCase().replace("mbps", ""));
									}else if(meanSpeedResult.toLowerCase().indexOf("kbps")!=-1){
										meanSpeedValue = Double.valueOf(meanSpeedResult.toLowerCase().replace("kbps", ""))/1024;
									}else{
										System.out.println("速率格式异常----->"+meanSpeedResult);
										if(meanSpeedUnit.toLowerCase().indexOf("kbps")!=-1){
											meanSpeedValue = Double.valueOf(meanSpeedResult.toLowerCase())/1024;
										}else{
											meanSpeedValue = Double.valueOf(meanSpeedResult.toLowerCase());
										}
									}
									
									String maxSpeedHistory = (String)netResultList.get("max");
									if(maxSpeedHistory!=null && !"".equals(maxSpeedHistory)){
										maxSpeedHistoryDouble = Double.valueOf(maxSpeedHistory);
									}
									String meanSpeedHistory = (String)netResultList.get("mean");
									if(meanSpeedHistory!=null && !"".equals(meanSpeedHistory)){
										meanSpeedHistoryDouble = Double.valueOf(meanSpeedHistory);
									}
								}catch (Exception e){
									e.printStackTrace();
									if(list.hasNext()){
										list.next();
									}else{
										break;
									}
									continue;
								}
								
								totalTypeInte++;
								if(type.trim().toLowerCase().equals("download")){
									if(meanSpeedValue>=30){
										myData.put("imgUrl", "1.png");
									}else if(meanSpeedValue>=20 && meanSpeedValue<30){
										myData.put("imgUrl", "2.png");
									}else if(meanSpeedValue>=10 && meanSpeedValue<20){
										myData.put("imgUrl", "3.png");
									}else if(meanSpeedValue>=5 && meanSpeedValue<10){
										myData.put("imgUrl", "4.png");
									}else if(meanSpeedValue<5){
										myData.put("imgUrl", "5.png");
									}
								}else if(type.trim().toLowerCase().equals("upload")){
									if(meanSpeedValue>=10){
										myData.put("imgUrl", "1.png");
									}else if(meanSpeedValue>=8 && meanSpeedValue<10){
										myData.put("imgUrl", "2.png");
									}else if(meanSpeedValue>=5 && meanSpeedValue<8){
										myData.put("imgUrl", "3.png");
									}else if(meanSpeedValue>=2 && meanSpeedValue<5){
										myData.put("imgUrl", "4.png");
									}else if(meanSpeedValue<2){
										myData.put("imgUrl", "5.png");
									}
								}
								
								if(isDetail){
									ArrayList<HashMap<String, String>> detail = (ArrayList<HashMap<String, String>>)netResultList.get("detail");
									if(detail==null){
										detail = new ArrayList<HashMap<String, String>>();
									}
									detail.add(myData);
									netResultList.put("detail",detail);
								}
								
								if(meanSpeedValue>26){
									int asd = 0;
									asd++;
								}
								if(meanSpeedValue>maxSpeedHistoryDouble){
									netResultList.put("max",""+meanSpeedValue);
								}
								double meanSpeedValueNow = (meanSpeedHistoryDouble*(totalTypeInte-1)+meanSpeedValue)/totalTypeInte;
								netResultList.put("mean",""+meanSpeedValueNow);
								netResultList.put("count",""+totalTypeInte);
								result.put(netResult, netResultList);
								total++;
							}
							
							if(list.hasNext()){
								list.next();
							}else{
								break;
							}
						}
					}
				}catch (Exception e){
					e.printStackTrace();
					continue;
				}	
			}
		}
		
		
		System.out.println("总处理数据量"+total);
		result.put("total", total);
		return result;
	}
	
	/**
	 * 获取HTTP测试的业务质量地图
	 * @author Ocean
	 * @param type  指下行速率、上行速率、时延
	 * @param keyspaces
	 * @param qe
	 * @param startKey
	 * @param count
	 * @return
	 */
	public Map<String,Object> getBrowseTestData(JSONArray keyspaces, List<ArrayList<QueryExpression>> qes, String startKey, int count, boolean isDetail){
		Map<String,Object> result = new HashMap<String,Object> ();
		
		List<String> columnsPredict = new ArrayList<String>();
		columnsPredict.add("网络(1)网络制式");
		columnsPredict.add("网络(2)网络制式");
		columnsPredict.add("网络（1）网络制式");
		columnsPredict.add("网络（2）网络制式");
		columnsPredict.add("网络制式");
		columnsPredict.add("网络类型");

		columnsPredict.add("GPS位置");
		columnsPredict.add("GPS位置信息");
		columnsPredict.add("GPS信息");
		columnsPredict.add("测试位置");
		columnsPredict.add("测试GPS位置");
		
		columnsPredict.add("地址");
		columnsPredict.add("网址");
		
		columnsPredict.add("平均时长(ms)");
		columnsPredict.add("时长(ms)");
		columnsPredict.add("平均时延");
		columnsPredict.add("平均时长（ms）");
		columnsPredict.add("时长（ms）");

		columnsPredict.add("cassandra_province");
		columnsPredict.add("cassandra_city");
		columnsPredict.add("cassandra_district");
		int defaultStep = 200;
		int total = 0;
		boolean limit = false;
		if(count>-1){
			limit = true;
			if(defaultStep>=count){
				defaultStep = count;
			}
		}
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

				try{
					ColumnFamilyTemplate<String, String> columnFamilyTemplate = new ThriftColumnFamilyTemplate<String, String>(
							keyspaceOperator, "02001", KeySpaceFactory.stringSerializer, KeySpaceFactory.stringSerializer);
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
									.queryColumns(dataPredicate,columnsPredict);
						}catch (Exception e){
							e.printStackTrace();
							continue;
						}
						
						while (list.hasResults()) {
							if(lastKey.equals(list.getKey())){
								if(list.hasNext()){
									list.next();
								}else{
									break;
								}
								continue;
							}
							lastKey = list.getKey();
							HashMap<String, String> myData = new HashMap<String, String>();
							Collection<String> element = list.getColumnNames();
							Object[] columns = element.toArray();
							stepCount++;
							
							if(total==count){
								break;
							}

							int previGps = 0;
							int previNetType = 0;
							String gpsStr = "";
							String NetType1 = "";
							String NetType2 = "";
							String NetType = "";
							String url = "";
							String delay = "";

							String province = "";
							String city = "";
							String district = "";	
							for(int r=0; r<columns.length; r++){
								
								//获取网络制式及信号强度Start
								if(((String)columns[r]).equals("网络(1)网络制式") || ((String)columns[r]).equals("网络（1）网络制式")){
									NetType1 = list.getString(""+columns[r]);
									continue;
								}else if(((String)columns[r]).equals("网络(2)网络制式") || ((String)columns[r]).equals("网络（2）网络制式")){
									NetType2 = list.getString(""+columns[r]);
									continue;
								}else if(((String)columns[r]).equals("网络制式")){
									if(previNetType<=2){
										previNetType = 2;
										NetType = list.getString(""+columns[r]);
									}
									continue;
								}else if(((String)columns[r]).equals("网络类型")){
									if(previNetType<=1){
										previNetType = 1;
										NetType = list.getString(""+columns[r]);
									}
									continue;
								}
								//获取网络制式End
								
								//获取GPS位置信息Start
								if(((String)columns[r]).equals("GPS位置")){
									if(previGps<5){
										previGps = 5;
										gpsStr = list.getString(""+columns[r]);
									}
									continue;
								}else if(((String)columns[r]).equals("GPS位置信息")){
									if(previGps<4){
										previGps = 4;
										gpsStr = list.getString(""+columns[r]);
									}
									continue;
								}else if(((String)columns[r]).equals("GPS信息")){
									if(previGps<3){
										previGps = 3;
										gpsStr = list.getString(""+columns[r]);
									}
									continue;
								}else if(((String)columns[r]).equals("测试位置")){
									if(previGps<2){
										previGps = 3;
										gpsStr = list.getString(""+columns[r]);
									}
									continue;
								}else if(((String)columns[r]).equals("测试GPS位置")){
									if(previGps<1){
										previGps = 3;
										gpsStr = list.getString(""+columns[r]);
									}
									continue;
								}									
								//获取GPS位置信息End
								
								//获取时延信息Start
								if(((String)columns[r]).equals("地址") || ((String)columns[r]).equals("网址")){
									url = list.getString(""+columns[r]).trim();
									continue;
								}
								if(((String)columns[r]).equals("平均时长(ms)") || ((String)columns[r]).equals("平均时延(ms)") ||((String)columns[r]).equals("平均时长（ms）") || ((String)columns[r]).equals("平均时延（ms）")){
									delay = list.getString(""+columns[r]).trim();
									continue;
								}else if(((String)columns[r]).equals("平均时延")){
									delay = list.getString(""+columns[r]).trim();
									continue;
								}else if(delay!=null && !delay.equals("") && (((String)columns[r]).equals("时长(ms)") || ((String)columns[r]).equals("时长（ms）"))){
									delay = list.getString(""+columns[r]).trim();
									continue;
								}							
								//获取时延信息End
								
								if(((String)columns[r]).equals("cassandra_province")){
									province = list.getString(""+columns[r]);
									continue;
								}
								if(((String)columns[r]).equals("cassandra_city")){
									city = list.getString(""+columns[r]);
									continue;
								}
								if(((String)columns[r]).equals("cassandra_district")){
									district = list.getString(""+columns[r]);
									continue;
								}
								
							}
							
							//数据格式整理
							String netResult = "";
							String urlResult = "";
							String delayResult = "";
							String[] gpsresult = new String[2];
							
							String[] gps = new String[2];
							if(gpsStr != null && !gpsStr.trim().equals("--") && !gpsStr.trim().equals("-") && !gpsStr.trim().equals("")){
								if(gpsStr.contains(" ")){
									gps = transGpsPoint(gpsStr);
									if(gps!=null && gps[0]!=null && gps[1]!=null){
										gpsresult = gps;
									}else{
										if(list.hasNext()){
											list.next();
										}else{
											break;
										}
										continue;
									}
								}
							}else{
								if(list.hasNext()){
									list.next();
								}else{
									break;
								}
								continue;
							}
							
							if(NetType1!=null && !NetType1.equals("") && !NetType1.trim().equals("-") && !NetType1.trim().equals("--") && !netResult.trim().equals("N/A") ){
								netResult = NetType1.trim();
							}else if(NetType2!=null && !NetType2.equals("") && !NetType2.trim().equals("-") && !NetType2.trim().equals("--") && !netResult.trim().equals("N/A") ){
								netResult = NetType2.trim();
							}else if(NetType!=null && !NetType.equals("") && !NetType.trim().equals("-") && !NetType.trim().equals("--") && !netResult.trim().equals("N/A") ){
								netResult = NetType.trim();
							}
							
							if(url!=null && !url.equals("") && !url.trim().equals("-") && !url.trim().equals("--") && !url.trim().equals("N/A")){
								urlResult = url.trim();
							}
							
							if(delay!=null && !delay.equals("") && !delay.trim().equals("-") && !delay.trim().equals("--") && !delay.trim().equals("N/A")){
								delayResult = delay.trim();
							}
							
							if(urlResult!=null && !urlResult.equals("") && !urlResult.trim().equals("-") && !urlResult.trim().equals("--") && !urlResult.trim().equals("N/A") && delayResult!=null && !delayResult.equals("") && !delayResult.trim().equals("-") && !delayResult.trim().equals("--") && !delayResult.trim().equals("N/A")){
								Map<String,Object> netResultList = (HashMap<String, Object>)result.get(urlResult);
								if(netResultList==null){
									netResultList = new HashMap<String,Object>();
								}
								
								String totalType = (String)netResultList.get("count");
								int totalTypeInte = 0;
								if(totalType!=null && !"".equals(totalType)){
									totalTypeInte = Integer.parseInt(totalType);
								}
								
								myData.put("GPSLocation", gpsresult[0]+" "+gpsresult[1]);
								myData.put("province", province);
								myData.put("city", city);
								myData.put("district", district);
								myData.put("NetType", netResult);
								myData.put("Delay", delayResult);
								
								double delayValue = 0;
								try{
									if(delayResult.toLowerCase().indexOf("ms")!=-1){
										delayValue = Double.valueOf(delayResult.toLowerCase().replace("ms", ""));
									}else{
										System.out.println("时延数据格式----->"+delayResult);
										delayValue = Double.valueOf(delayResult.toLowerCase());
									}
								}catch (Exception e){
									e.printStackTrace();
									if(list.hasNext()){
										list.next();
									}else{
										break;
									}
									continue;
								}
								
								if(url.trim().toLowerCase().indexOf("baidu.com")!=-1){
									if(delayValue>=1000){
										myData.put("imgUrl", "5.png");
									}else if(delayValue>=800 && delayValue<1000){
										myData.put("imgUrl", "4.png");
									}else if(delayValue>=500 && delayValue<800){
										myData.put("imgUrl", "3.png");
									}else if(delayValue>=200 && delayValue<500){
										myData.put("imgUrl", "2.png");
									}else if(delayValue<200){
										myData.put("imgUrl", "1.png");
									}
								}else{
									if(delayValue>=60000){
										myData.put("imgUrl", "5.png");
									}else if(delayValue>=40000 && delayValue<60000){
										myData.put("imgUrl", "4.png");
									}else if(delayValue>=20000 && delayValue<40000){
										myData.put("imgUrl", "3.png");
									}else if(delayValue>=5000 && delayValue<20000){
										myData.put("imgUrl", "2.png");
									}else if(delayValue<5000){
										myData.put("imgUrl", "1.png");
									}
								}
								
								if(isDetail){
									ArrayList<HashMap<String, String>> detail = (ArrayList<HashMap<String, String>>)netResultList.get("detail");
									if(detail==null){
										detail = new ArrayList<HashMap<String, String>>();
									}
									detail.add(myData);
									netResultList.put("detail",detail);
								}

								totalTypeInte++;
								
								String meanDelayHistory = (String)netResultList.get("mean_delay");
								
								double meanDelayHistoryDouble = 0;
								if(meanDelayHistory!=null && !"".equals(meanDelayHistory)){
									meanDelayHistory = meanDelayHistory.toLowerCase().replace("ms", "");
									meanDelayHistoryDouble = Double.valueOf(meanDelayHistory);
								}
								double meanDelayValueNow = (meanDelayHistoryDouble*(totalTypeInte-1)+delayValue)/totalTypeInte;
								netResultList.put("mean_download", ""+meanDelayValueNow+"ms");
								
								netResultList.put("count",""+totalTypeInte);
								result.put(urlResult, netResultList);
								total++;
							}
							
							if(list.hasNext()){
								list.next();
							}else{
								break;
							}
						}
					}
				}catch (Exception e){
					e.printStackTrace();
					continue;
				}	
			}
		}
		
		
		System.out.println("总处理数据量"+total);
		result.put("total", total);
		return result;
	}
	
	/**
	 * @author Ocean
	 * @param type   指LTE/SINR/EDGE等网络类型
	 * @param keyspaces
	 * @param columnFamilies
	 * @param qe
	 * @param startKey
	 * @param count
	 * @return
	 */
	public Map<String,Object> getAllData(JSONArray keyspaces, JSONArray columnFamilies, String startKey, int count, boolean isDetail){
		
		
		Map<String,Object> result = new HashMap<String,Object> ();
		JSONArray allColumnFamilies = null;
		JSONArray sortColumnFamilies = null;
		
		List<String> columnsPredict = new ArrayList<String>();
		
		int defaultStep = 200;
		int total = 0;
		int processCount = 0;
		SimpleDateFormat dateFormat = new SimpleDateFormat(
				"yyyy-MM-dd HH:mm:ss");
		boolean limit = false;
		if(count>-1){
			limit = true;
			if(defaultStep>=count){
				defaultStep = count;
			}
		}

		for(int i=0; i<keyspaces.size(); i++){
			CsvWrite csv = new CsvWrite();
//			csv.start((String)keyspaces.get(i));
			//EQ代表“=”，GTE代表“>=”，GT代表“>”，LTE代表“<=”，LT代表“<”
			if((String)keyspaces.get(i)==null || "".equals((String)keyspaces.get(i))){
				continue;
			}
			Keyspace keyspaceOperator = null;
			try{
				keyspaceOperator = HFactory.createKeyspace((String)keyspaces.get(i),
						KeySpaceFactory.cluster);
				
				//默认认为以"0"或者“signal”开始的columnfamily name为测试类型
				if(columnFamilies.contains("all") || columnFamilies.contains("ALL")){
					allColumnFamilies = new JSONArray();
					KeyspaceDefinition kd = KeySpaceFactory.cluster.describeKeyspace((String)keyspaces.get(i));
					List<ColumnFamilyDefinition> cfList = kd.getCfDefs();
					for (int j = 0; j < cfList.size(); j++) {
						ColumnFamilyDefinition cfD = cfList.get(j);
						if(cfD.getName().startsWith("0") || cfD.getName().startsWith("signal")){
							allColumnFamilies.add(cfD.getName());
						}
					}
				}
			}catch (Exception e){
				e.printStackTrace();
				continue;
			}
			if(columnFamilies.contains("all") || columnFamilies.contains("ALL")){
				sortColumnFamilies = allColumnFamilies;
			}else{
				sortColumnFamilies = columnFamilies;
			}
//			sortColumnFamilies = columnFamilies;
			for(int j=0; j<sortColumnFamilies.size(); j++){
				try{
					
					RangeSlicesQuery<String, String, String> rangeQuery = HFactory
							.createRangeSlicesQuery(keyspaceOperator, KeySpaceFactory.stringSerializer, KeySpaceFactory.stringSerializer, KeySpaceFactory.stringSerializer);
					rangeQuery.setColumnFamily((String)sortColumnFamilies.get(j));
					rangeQuery.setKeys("", "");
					rangeQuery.setRowCount(200);
					rangeQuery.setColumnNames("网络(1)网络制式","网络(2)网络制式","网络（1）网络制式","网络（2）网络制式",
							"网络制式","网络类型","网络(1)信号强度","网络(2)信号强度","信号强度","网络（1）信号强度","网络（2）信号强度","信号强度","SINR","GPS位置","GPS位置信息","GPS信息","测试位置","测试GPS位置",
							"下行速率","上行速率","时延","时延(ms)","下行速率(Mbps)","上行速率(Mbps)","下行速率(Kbps)","上行速率(Kbps)","时延（ms）","下行速率（Mbps）","上行速率（Mbps）","下行速率（Kbps）","上行速率（Kbps）",
							"TAC/PCI","LAC/CID","TAC","PCI","LAC","CID","网络(1)小区信息","网络(2)小区信息","网络（1）小区信息","网络（2）小区信息","协议","类型","平均时延(ms)","平均时延（ms）",
							"最大速率(Mbps)","平均速率(Mbps)","最大速率（Mbps）","平均速率（Mbps）","最大速率(Kbps)","平均速率(Kbps)","最大速率（Kbps）","平均速率（Kbps）",
							"地址","网址","平均时长(ms)","时长(ms)","平均时延","平均时长（ms）","时长（ms）","cassandra_province","cassandra_city","cassandra_district", "year_month_day");
//					地址	成功率	最大时延	最小时延	平均时延

//					rangeQuery.setColumnNames("网络(1)网络制式","cassandra_province","cassandra_city","cassandra_district");
					
					int stepCount = defaultStep;
					String lastKey = "";
					
					while(stepCount>=(defaultStep-1) && ((limit==true && total<count)||limit==false)){
						stepCount = 0;
						
						rangeQuery.setKeys(lastKey, "");
						
						QueryResult<OrderedRows<String, String, String>> resultQuery = null;
						OrderedRows<String, String, String> oRows = null;
						try{
							processCount++;
							System.out.println("Start Reading---->"+(dateFormat.format(new Date()))+"----->KEYSPACE: "+i+"  --->COLUMNFAMILY: "+j+"  --->PROCESSCOUNT: "+processCount);
							
							resultQuery = rangeQuery
									.execute();
							oRows = resultQuery.get();
						}catch (Exception e){
							e.printStackTrace();
							try{
								resultQuery = rangeQuery
										.execute();
								oRows = resultQuery.get();
							}catch (Exception e1){
								e1.printStackTrace();
								continue;
							}
							continue;
						}
						
						List<Row<String, String, String>> rowList = oRows.getList();
						
						for (Row<String, String, String> list : rowList) {
							if(lastKey.equals(list.getKey())){
								continue;
							}
							lastKey = list.getKey();
							
							HashMap<String, String> myData = new HashMap<String, String>();
							List<HColumn<String, String>> columns = list.getColumnSlice()
									.getColumns();
							
							stepCount++;
							
							if(total==count){
								break;
							}

							int previGps = 0;
							int previNetType = 0;
							String gpsStr = "";
							String NetType1 = "";
							String NetType2 = "";
							String NetType = "";
							String signalStength1 = "";
							String signalStength2 = "";
							String signalStength = "";
							String sinr = "";
							
							//SIGNAL
							
							//SPEEDTEST
							String downloadSpeed = "";
							String uploadSpeed = "";
							String dlSpeedUnit = "";
							String ulSpeedUnit = "";
							String delaySpeedtest = "";
							String cellInfo1 = "";
							String cellInfo2 = "";
							String cellInfo = "";
							String lac = "";
							String cid = "";
							String tac = "";
							String pci = "";
							String tac_pci = "";
							String lac_cid = "";
							
							//HTTP
							String protocal = "";
							String testType = "";
							String meanSpeedUnit = "";
							String maxSpeedUnit = "";
							String meanSpeed = "";
							String maxSpeed = "";
							String delayHttp = "";
							
							//BROWSE
							String url = "";
							String delayBrowse = "";
							
							
							String province = "";
							String city = "";
							String district = "";
							String year_month_day = "";
							for (HColumn<String, String> hColumn : columns) {
								
								//获取网络制式及信号强度Start
								if(((String)hColumn.getName()).equals("网络(1)网络制式") || ((String)hColumn.getName()).equals("网络（1）网络制式")){
									NetType1 = KeySpaceFactory.stringSerializer.fromByteBuffer(hColumn.getValueBytes());
									continue;
								}else if(((String)hColumn.getName()).equals("网络(2)网络制式") || ((String)hColumn.getName()).equals("网络（2）网络制式")){
									NetType2 = KeySpaceFactory.stringSerializer.fromByteBuffer(hColumn.getValueBytes());
									continue;
								}else if(((String)hColumn.getName()).equals("网络制式")){
									if(previNetType<=2){
										previNetType = 2;
										NetType = KeySpaceFactory.stringSerializer.fromByteBuffer(hColumn.getValueBytes());
									}
									continue;
								}else if(((String)hColumn.getName()).equals("网络类型")){
									if(previNetType<=1){
										previNetType = 1;
										NetType = KeySpaceFactory.stringSerializer.fromByteBuffer(hColumn.getValueBytes());
									}
									continue;
								}
								
								if(((String)hColumn.getName()).equals("网络(1)信号强度") || ((String)hColumn.getName()).equals("网络（1）信号强度")){
									signalStength1 = KeySpaceFactory.stringSerializer.fromByteBuffer(hColumn.getValueBytes());
									continue;
								}else if(((String)hColumn.getName()).equals("网络(2)信号强度") || ((String)hColumn.getName()).equals("网络（2）信号强度")){
									signalStength2 = KeySpaceFactory.stringSerializer.fromByteBuffer(hColumn.getValueBytes());
									continue;
								}else if(((String)hColumn.getName()).equals("信号强度")){
									if(previNetType<=1){
										previNetType = 1;
										signalStength = KeySpaceFactory.stringSerializer.fromByteBuffer(hColumn.getValueBytes());
									}
									continue;
								}
								//获取网络制式及信号强度End
								
								if(((String)hColumn.getName()).toLowerCase().equals("sinr")){
									sinr = KeySpaceFactory.stringSerializer.fromByteBuffer(hColumn.getValueBytes());
									continue;
								}
								
								//获取GPS位置信息Start
								if(((String)hColumn.getName()).equals("GPS信息")){
									if(previGps<5){
										previGps = 5;
										gpsStr = KeySpaceFactory.stringSerializer.fromByteBuffer(hColumn.getValueBytes());
									}
									continue;
								}else if(((String)hColumn.getName()).equals("GPS位置信息")){
									if(previGps<4){
										previGps = 4;
										gpsStr = KeySpaceFactory.stringSerializer.fromByteBuffer(hColumn.getValueBytes());
									}
									continue;
								}else if(((String)hColumn.getName()).equals("GPS位置")){
									if(previGps<3){
										previGps = 3;
										gpsStr = KeySpaceFactory.stringSerializer.fromByteBuffer(hColumn.getValueBytes());
									}
									continue;
								}else if(((String)hColumn.getName()).equals("测试位置")){
									if(previGps<2){
										previGps = 3;
										gpsStr = KeySpaceFactory.stringSerializer.fromByteBuffer(hColumn.getValueBytes());
									}
									continue;
								}else if(((String)hColumn.getName()).equals("测试GPS位置")){
									if(previGps<1){
										previGps = 3;
										gpsStr = KeySpaceFactory.stringSerializer.fromByteBuffer(hColumn.getValueBytes());
									}
									continue;
								}									
								//获取GPS位置信息End
								
								//获取速率及时延信息Start
								if(((String)hColumn.getName()).equals("下行速率") || ((String)hColumn.getName()).equals("下行速率(Mbps)") || ((String)hColumn.getName()).equals("下行速率（Mbps）") || ((String)hColumn.getName()).equals("下行速率(Kbps)") || ((String)hColumn.getName()).equals("下行速率（Kbps）")){
									downloadSpeed = KeySpaceFactory.stringSerializer.fromByteBuffer(hColumn.getValueBytes());
									dlSpeedUnit = ((String)hColumn.getName());
									continue;
								}else if(((String)hColumn.getName()).equals("上行速率") || ((String)hColumn.getName()).equals("上行速率(Mbps)") || ((String)hColumn.getName()).equals("上行速率(Kbps)") || ((String)hColumn.getName()).equals("上行速率（Mbps）") || ((String)hColumn.getName()).equals("上行速率（Kbps）")){
									uploadSpeed = KeySpaceFactory.stringSerializer.fromByteBuffer(hColumn.getValueBytes());
									ulSpeedUnit = ((String)hColumn.getName());
									continue;
								}else if(((String)hColumn.getName()).equals("时延") || ((String)hColumn.getName()).equals("时延(ms)") || ((String)hColumn.getName()).equals("时延（ms）")){
									delaySpeedtest = KeySpaceFactory.stringSerializer.fromByteBuffer(hColumn.getValueBytes());
									continue;
								}								
								//获取速率及时延信息End
								
								//获取SPEEDTEST小区信息Start
								if(((String)hColumn.getName()).equals("网络(1)小区信息") || ((String)hColumn.getName()).equals("网络（1）小区信息")){
									cellInfo1 = KeySpaceFactory.stringSerializer.fromByteBuffer(hColumn.getValueBytes());
									continue;
								}else if(((String)hColumn.getName()).equals("网络(2)小区信息") || ((String)hColumn.getName()).equals("网络（2）小区信息")){
									cellInfo2 = KeySpaceFactory.stringSerializer.fromByteBuffer(hColumn.getValueBytes());
									continue;
								}else if(((String)hColumn.getName()).equals("小区信息")){
									cellInfo = KeySpaceFactory.stringSerializer.fromByteBuffer(hColumn.getValueBytes());
									continue;
								}
								if(((String)hColumn.getName()).toLowerCase().equals("tac/pci")){
									tac_pci = KeySpaceFactory.stringSerializer.fromByteBuffer(hColumn.getValueBytes());
									continue;
								}else if(((String)hColumn.getName()).toLowerCase().equals("lac/cid")){
									lac_cid = KeySpaceFactory.stringSerializer.fromByteBuffer(hColumn.getValueBytes());
									continue;
								}else if(((String)hColumn.getName()).toLowerCase().equals("lac")){
									lac = KeySpaceFactory.stringSerializer.fromByteBuffer(hColumn.getValueBytes());
									continue;
								}else if(((String)hColumn.getName()).toLowerCase().equals("cid")){
									cid = KeySpaceFactory.stringSerializer.fromByteBuffer(hColumn.getValueBytes());
									continue;
								}else if(((String)hColumn.getName()).toLowerCase().equals("tac")){
									tac = KeySpaceFactory.stringSerializer.fromByteBuffer(hColumn.getValueBytes());
									continue;
								}else if(((String)hColumn.getName()).toLowerCase().equals("pci")){
									pci = KeySpaceFactory.stringSerializer.fromByteBuffer(hColumn.getValueBytes());
									continue;
								}
								//获取速率及时延信息End
								
								//获取HTTP速率及时延信息Start
								if(((String)hColumn.getName()).equals("平均速率(Mbps)") || ((String)hColumn.getName()).equals("平均速率(Kbps)") || ((String)hColumn.getName()).equals("平均速率（Mbps）") || ((String)hColumn.getName()).equals("平均速率（Kbps）") || ((String)hColumn.getName()).equals("平均速率")){
									meanSpeedUnit = ((String)hColumn.getName());
									meanSpeed = KeySpaceFactory.stringSerializer.fromByteBuffer(hColumn.getValueBytes());
									continue;
								}else if(((String)hColumn.getName()).equals("最大速率(Mbps)") || ((String)hColumn.getName()).equals("最大速率(Kbps)") ||((String)hColumn.getName()).equals("最大速率（Mbps）") || ((String)hColumn.getName()).equals("最大速率（Kbps）") || ((String)hColumn.getName()).equals("最大速率")){
									maxSpeedUnit = ((String)hColumn.getName());
									maxSpeed = KeySpaceFactory.stringSerializer.fromByteBuffer(hColumn.getValueBytes());
									continue;
								}else if(((String)hColumn.getName()).equals("协议")){
									protocal = KeySpaceFactory.stringSerializer.fromByteBuffer(hColumn.getValueBytes());
									continue;
								}else if(((String)hColumn.getName()).equals("类型")){
									testType = KeySpaceFactory.stringSerializer.fromByteBuffer(hColumn.getValueBytes());
									continue;
								}else if(((String)hColumn.getName()).equals("平均时延(ms)") || ((String)hColumn.getName()).equals("平均时延（ms）") || ((String)hColumn.getName()).equals("平均时延") || ((String)hColumn.getName()).equals("时延") || ((String)hColumn.getName()).equals("时延(ms)")){
									delayHttp = KeySpaceFactory.stringSerializer.fromByteBuffer(hColumn.getValueBytes());
									continue;
								}							
								//获取速率及时延信息End
								
								//获取BROWSE时延信息Start
								if(((String)hColumn.getName()).equals("地址") || ((String)hColumn.getName()).equals("网址")){
									url = KeySpaceFactory.stringSerializer.fromByteBuffer(hColumn.getValueBytes()).trim();
									continue;
								}
								if(((String)hColumn.getName()).equals("平均时长(ms)") || ((String)hColumn.getName()).equals("平均时延(ms)") ||((String)hColumn.getName()).equals("平均时长（ms）") || ((String)hColumn.getName()).equals("平均时延（ms）")){
									delayBrowse = KeySpaceFactory.stringSerializer.fromByteBuffer(hColumn.getValueBytes()).trim();
									continue;
								}else if(((String)hColumn.getName()).equals("平均时延")){
									delayBrowse = KeySpaceFactory.stringSerializer.fromByteBuffer(hColumn.getValueBytes()).trim();
									continue;
								}else if(delayBrowse!=null && !delayBrowse.equals("") && (((String)hColumn.getName()).equals("时长(ms)") || ((String)hColumn.getName()).equals("时长（ms）"))){
									delayBrowse = KeySpaceFactory.stringSerializer.fromByteBuffer(hColumn.getValueBytes()).trim();
									continue;
								}							
								//获取时延信息End
								
								if(((String)hColumn.getName()).equals("cassandra_province")){
									province = KeySpaceFactory.stringSerializer.fromByteBuffer(hColumn.getValueBytes());
									continue;
								}
								if(((String)hColumn.getName()).equals("cassandra_city")){
									city = KeySpaceFactory.stringSerializer.fromByteBuffer(hColumn.getValueBytes());
									continue;
								}
								if(((String)hColumn.getName()).equals("cassandra_district")){
									district = KeySpaceFactory.stringSerializer.fromByteBuffer(hColumn.getValueBytes());
									continue;
								}
								if(((String)hColumn.getName()).equals("year_month_day")){
									year_month_day = KeySpaceFactory.stringSerializer.fromByteBuffer(hColumn.getValueBytes());
									continue;
								}
								
							}
							
							//数据格式整理
							String fileTypeResult = (String)sortColumnFamilies.get(j);
							String gpsStrResult = "";
							String[] gpsresult = new String[2];
							
							//SPEEDTEST
							String key = list.getKey();
							String netResult = "";
							String signalResult = "";
							String sinrResult = "";
							String dlSpeedResult = "";
							String ulSpeedResult = "";
							String delayResultSpeedtest = "";
							String cellResult = "";
							String cellResult2G = "";
							
							//BROWSE
							String urlResult = "";
							String delayResultBrowse = "";

							//HTTP
							String meanSpeedResult = "";
							String maxSpeedResult = "";
							String delayResultHttp = "";
							String protocalResult = "";
							String testTypeResult = "";
							
							String[] gps = new String[2];
							if(gpsStr != null && !gpsStr.trim().equals("--") && !gpsStr.trim().equals("-") && !gpsStr.trim().equals("")){
								gpsStrResult = gpsStr;
								if(gpsStr.contains(" ")){
									gps = transGpsPoint(gpsStr);
									if(gps!=null && gps[0]!=null && gps[1]!=null){
										gpsresult = gps;
									}else{
										continue;
									}
								}
							}else{
								continue;
							}
							
							if(NetType1!=null && !NetType1.equals("") && !NetType1.trim().equals("-") && !NetType1.trim().equals("--") && !NetType1.trim().equals("N/A")){
								netResult = NetType1.trim();
								signalResult = signalStength1.trim();
								cellResult = cellInfo1.trim();
							}else if(NetType2!=null && !NetType2.equals("") && !NetType2.trim().equals("-") && !NetType2.trim().equals("--") && !NetType2.trim().equals("N/A")){
								netResult = NetType2.trim();
								signalResult = signalStength2.trim();
								cellResult = cellInfo2.trim();
							}else if(NetType!=null && !NetType.equals("") && !NetType.trim().equals("-") && !NetType.trim().equals("--") && !NetType.trim().equals("N/A")){
								netResult = NetType.trim();
								signalResult = signalStength.trim();
								cellResult = cellInfo.trim();
							}
							if(sinr!=null && !sinr.equals("") && !sinr.trim().equals("-") && !sinr.trim().equals("--") && !sinr.trim().equals("N/A") ){
								sinrResult = sinr.trim();
							}
							if(cellResult!=null && !cellResult.equals("") && !cellResult.trim().equals("-") && !cellResult.trim().equals("--") && !cellResult.trim().equals("N/A") && !cellResult.trim().equals("N/A/N/A")){
								
							}else{
								if(netResult.equals("LTE") && tac_pci!=null && !tac_pci.equals("") && !tac_pci.trim().equals("-") && !tac_pci.trim().equals("--") && !tac_pci.trim().equals("N/A") && !tac_pci.trim().equals("N/A/N/A")){
									cellResult = tac_pci.trim();
								}else if(netResult.equals("LTE") && tac!=null && !tac.equals("") && !tac.trim().equals("-") && !tac.trim().equals("--") && !tac.trim().equals("N/A") && !(tac.trim().toLowerCase().indexOf("n/a")!=-1) && pci!=null && !pci.equals("") && !pci.trim().equals("-") && !pci.trim().equals("--") && !pci.trim().equals("N/A") && !(pci.trim().toLowerCase().indexOf("n/a")!=-1)){
									if(tac.toLowerCase().indexOf("tac")!=-1){
										tac = tac.toLowerCase().replace("tac=", "");
									}
									if(pci.toLowerCase().indexOf("pci")!=-1){
										pci = pci.toLowerCase().replace("pci=", "");
									}
									cellResult = tac+"/"+pci;
								}else if(lac_cid!=null && !lac_cid.equals("") && !lac_cid.trim().equals("-") && !lac_cid.trim().equals("--") && !lac_cid.trim().equals("N/A") && !lac_cid.trim().equals("N/A/N/A")){
									cellResult2G = lac_cid.trim();
								}else if(lac!=null && !lac.equals("") && !lac.trim().equals("-") && !lac.trim().equals("--") && !lac.trim().equals("N/A") && !(lac.trim().toLowerCase().indexOf("n/a")!=-1) && cid!=null && !cid.equals("") && !cid.trim().equals("-") && !cid.trim().equals("--") && !cid.trim().equals("N/A") && !(cid.trim().toLowerCase().indexOf("n/a")!=-1)){
									if(lac.toLowerCase().indexOf("lac")!=-1){
										lac = lac.toLowerCase().replace("lac=", "");
									}
									if(cid.toLowerCase().indexOf("cid")!=-1){
										lac = lac.toLowerCase().replace("cid=", "");
									}
									cellResult2G = lac+"/"+cid;
								}
							}
							
							
							if(downloadSpeed!=null && !downloadSpeed.equals("") && !downloadSpeed.trim().equals("-") && !downloadSpeed.trim().equals("--") && !downloadSpeed.trim().equals("N/A") ){
								dlSpeedResult = downloadSpeed.trim();
							}
							if(uploadSpeed!=null && !uploadSpeed.equals("") && !uploadSpeed.trim().equals("-") && !uploadSpeed.trim().equals("--") && !uploadSpeed.trim().equals("N/A")){
								ulSpeedResult = uploadSpeed.trim();
							}
							if(delaySpeedtest!=null && !delaySpeedtest.equals("") && !delaySpeedtest.trim().equals("-") && !delaySpeedtest.trim().equals("--") && !delaySpeedtest.trim().equals("N/A")){
								delayResultSpeedtest = delaySpeedtest.trim();
							}
							
							if(netResult!=null && !netResult.trim().equals("LTE") && cellResult!=null && !cellResult.equals("") && !cellResult.trim().equals("-") && !cellResult.trim().equals("--") && !cellResult.trim().equals("N/A")){
								cellResult = cellResult;
							}else if(netResult!=null && !netResult.trim().equals("LTE") && cellResult2G!=null && !cellResult2G.equals("") && !cellResult2G.trim().equals("-") && !cellResult2G.trim().equals("--") && !cellResult2G.trim().equals("N/A")){
								cellResult = cellResult2G;
							}else if(cellResult!=null && !cellResult.equals("") && !cellResult.trim().equals("-") && !cellResult.trim().equals("--") && !cellResult.trim().equals("N/A")){
								cellResult = cellResult;
							}else if(cellResult2G!=null && !cellResult2G.equals("") && !cellResult2G.trim().equals("-") && !cellResult2G.trim().equals("--") && !cellResult2G.trim().equals("N/A")){
								cellResult = cellResult2G;
							}
							
							//HTTP参数
							if(meanSpeed!=null && !meanSpeed.equals("") && !meanSpeed.trim().equals("-") && !meanSpeed.trim().equals("--") && !meanSpeed.trim().equals("N/A") ){
								meanSpeedResult = meanSpeed.trim();
							}
							if(maxSpeed!=null && !maxSpeed.equals("") && !maxSpeed.trim().equals("-") && !maxSpeed.trim().equals("--") && !maxSpeed.trim().equals("N/A")){
								maxSpeedResult = maxSpeed.trim();
							}
							if(delayHttp!=null && !delayHttp.equals("") && !delayHttp.trim().equals("-") && !delayHttp.trim().equals("--") && !delayHttp.trim().equals("N/A")){
								delayResultHttp = delayHttp.trim();
							}
							if(protocal!=null && !protocal.equals("") && !protocal.trim().equals("-") && !protocal.trim().equals("--") && !protocal.trim().equals("N/A")){
								protocalResult = protocal.trim();
							}
							if(testType!=null && !testType.equals("") && !testType.trim().equals("-") && !testType.trim().equals("--") && !testType.trim().equals("N/A")){
								testTypeResult = testType.trim();
							}

							//Browse参数
							if(url!=null && !url.equals("") && !url.trim().equals("-") && !url.trim().equals("--") && !url.trim().equals("N/A")){
								urlResult = url.trim();
							}
							
							if(delayBrowse!=null && !delayBrowse.equals("") && !delayBrowse.trim().equals("-") && !delayBrowse.trim().equals("--") && !delayBrowse.trim().equals("N/A")){
								delayResultBrowse = delayBrowse.trim();
							}
															
							ArrayList<String> myDataList = new ArrayList<String>();
							myDataList.add(fileTypeResult==null?"":fileTypeResult);
							myDataList.add(gpsStrResult==null?"":gpsStrResult);
							myDataList.add(gpsresult[0]==null?"":gpsresult[0]);
							myDataList.add(gpsresult[1]==null?"":gpsresult[1]);
							myDataList.add(key==null?"":key);
							myDataList.add(netResult==null?"":netResult);
							myDataList.add(signalResult==null?"":signalResult);
							myDataList.add(dlSpeedResult==null?"":dlSpeedResult);
							myDataList.add(ulSpeedResult==null?"":ulSpeedResult);
							myDataList.add(delayResultSpeedtest==null?"":delayResultSpeedtest);
							myDataList.add(cellResult==null?"":cellResult);
							myDataList.add(cellResult2G==null?"":cellResult2G);
							myDataList.add(urlResult==null?"":urlResult);
							myDataList.add(delayResultBrowse==null?"":delayResultBrowse);
							myDataList.add(meanSpeedResult==null?"":meanSpeedResult);
							myDataList.add(maxSpeedResult==null?"":maxSpeedResult);
							
							myDataList.add(delayResultHttp==null?"":delayResultHttp);
							myDataList.add(protocalResult==null?"":protocalResult);
							myDataList.add(testTypeResult==null?"":testTypeResult);
							myDataList.add(province==null?"":province);
							myDataList.add(city==null?"":city);
							myDataList.add(district==null?"":district);
							myDataList.add(year_month_day==null?"":year_month_day);
							myDataList.add(sinrResult==null?"":sinrResult);
							
							total++;
							csv.write(myDataList);
							
						}
					}
				}catch (Exception e){
					e.printStackTrace();
					continue;
				}	
			}
			csv.stop();
		}
		
		
		System.out.println("总处理数据量"+total);
		result.put("total", total);
		return result;
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
	//规整字符串开端和结尾
	public String subTxt(String str) {
		if (str.startsWith("{")) {
			str = str.substring(1, str.length());
		}
		if (str.endsWith("}")) {
			str = str.substring(0, str.length() - 1);
		}
		return str;
	}
	//获取文件大小信息
	private String convertFileSize(long fileS) {
		DecimalFormat df = new DecimalFormat("#.00");
		String fileSizeString = "";
		if (fileS < 1024) {
			fileSizeString = df.format((double) fileS) + "B";
		} else if (fileS < 1048576) {
			fileSizeString = df.format((double) fileS / 1024) + "K";
		} else if (fileS < 1073741824) {
			fileSizeString = df.format((double) fileS / 1048576) + "M";
		} else {
			fileSizeString = df.format((double) fileS / 1073741824) + "G";
		}
		return fileSizeString;
	}
	/**
	 * 获取文件最后更新时间
	 * @param file
	 * @return
	 */
	private String getLastModifies(File file){
		long modifiedTime = file.lastModified();
		Date date = new Date(modifiedTime);
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:MM");
		String dd = sdf.format(date);
		return dd;
	}
	public void test(){
		String src = ConfParser.srcPath;
		File file = new File(src);
		if(!file.exists()){
			file.mkdirs();
		}
		File[] fileList = file.listFiles();
		for (int i = 0; i < fileList.length; i++) {
			File sonFile = fileList[i];
			if(sonFile.isDirectory()){
				System.out.println("正在扫描第"+i+"个文件夹，共"+fileList.length+"个文件夹");
				getAllDataByDir(sonFile);
			}else{
				if(sonFile.getName().endsWith(".abstract.csv") || sonFile.getName().endsWith(".summary.csv")){
					getAllDataByFile(sonFile.getAbsoluteFile());	
				}
			}
		}
	}
	public void getAllDataByDir(File file){
		if(file.exists()&&file.isDirectory()){
			File[] fileList = file.listFiles();
			for (int i = 0; i < fileList.length; i++) {
				File sonFile = fileList[i];
				if(sonFile.isDirectory()){
					getAllDataByDir(sonFile.getAbsoluteFile());
				}else{
					if(sonFile.getName().endsWith(".abstract.csv") || sonFile.getName().endsWith(".summary.csv")){
						getAllDataByFile(sonFile.getAbsoluteFile());	
					}
				}
			}
		}
	}
	public void getAllDataByFile(File file){
		try {
			ArrayList<String[]> absHeader = new ArrayList<String[]>();
			ArrayList<String[]> absBody = new ArrayList<String[]>();
			ArrayList<String[]> testScenario = new ArrayList<String[]>();
			ArrayList<String[]> csvList = readCsv(file.getAbsoluteFile());
			String datetime = "";
			int dataEreaIndex = 0;
			for (int i = 0; i < csvList.size(); i++) {
				if (csvList.get(i) != null && !isBlankLine(csvList.get(i))) {
					if (csvList.get(i)[0] != null) {
						if ((csvList.get(i)[0].equals("测试时间")||csvList.get(i)[0].trim().toLowerCase().equals("time") || csvList.get(i)[0].trim().toLowerCase().equals("test time")) 
								&& csvList.size() > i + 1
								&& csvList.get(i + 1) != null
								&& csvList.get(i + 1).length >= 2
								&& csvList.get(i + 1)[0] != null
								&& (csvList.get(i + 1)[0].contains("型号") ||(csvList.get(i + 1)[0].trim().toLowerCase().contains("device type") || csvList.get(i + 1)[0].trim().toLowerCase().contains("terminal type") || csvList.get(i + 1)[0].trim().toLowerCase().contains("terminal mode") || csvList.get(i + 1)[0].trim().toLowerCase().contains("device model") || csvList.get(i + 1)[0].trim().toLowerCase().contains("terminal model")))) {

						} else if ((csvList.get(i)[0].equals("测试时间")
								|| csvList.get(i)[0].equals("测试开始时间")
								|| csvList.get(i)[0].equals("时间")
								|| (csvList.get(i)[0].equals("测速类型") || csvList.get(i)[0].equals("测试类型"))
								|| csvList.get(i)[0].equals("网络制式1时间")
								|| csvList.get(i)[0].equals("Network 1Time")
								|| csvList.get(i)[0].trim().toLowerCase().equals("time")
								|| csvList.get(i)[0].trim().toLowerCase().equals("duration") 
								|| csvList.get(i)[0].trim().toLowerCase().equals("start time")
								|| csvList.get(i)[0].trim().toLowerCase().equals("test start time")
								|| (csvList.get(i)[0].trim().toLowerCase().equals("time") &&(csvList.get(i)[1]!=null && csvList.get(i)[1].trim().toLowerCase().equals("network") || csvList.get(i)[1].trim().toLowerCase().equals("website") || csvList.get(i)[1].trim().toLowerCase().equals("address"))))
								&& csvList.get(i).length > 2 && csvList.get(i)[2]!=null && !csvList.get(i)[2].equals("")
								&& (csvList.size() == (i + 1) || csvList.get(i + 1) != null && csvList.get(i + 1).length > 2)){
							if ((csvList.get(i)[0].contains("测试时间") || csvList.get(i)[0].equals("测试开始时间")
								||csvList.get(i)[0].trim().toLowerCase().contains("time") || csvList.get(i)[0].trim().toLowerCase().equals("start time"))
								&& csvList.size() > (i + 1)
								&& csvList.get(i + 1) != null) {
								datetime = csvList.get(i + 1)[0];
							}
							dataEreaIndex = 1;
						} else if (csvList.get(i)[0].contains("Test Scenario")) {
							dataEreaIndex = 2;
						}
					}
					if (dataEreaIndex == 0) {
						absHeader.add(csvList.get(i));
					} else if (dataEreaIndex == 1) {
						absBody.add(csvList.get(i));
					} else if (dataEreaIndex == 2) {
						testScenario.add(csvList.get(i));
					}
				} else {
					dataEreaIndex++;
				}
			}
			// 获取测试类型Num
			Pattern pattern1 = Pattern.compile("[-_.]");
			Pattern pattern2 = Pattern.compile("\\d+");
			String numStrs[] = pattern1.split(file.getName());
			String dateTime = "";
			// 要生成的TXT的文件内容
			StringBuffer str = new StringBuffer();
			// 头部Map
			Map absHeaderMap = new HashMap();
			// 主体Map
			Map absBodyMap = new HashMap();
			// testScenario Map
			Map testScenarioMap = new HashMap();
			try {
				for (int i = 0; i < absHeader.size(); i++) {
					String key = absHeader.get(i)[0];
					try {
						if (absHeader.get(0)[0].contains("时间")
								|| absHeader.get(0)[0].contains("Time") && absHeader.get(0).length>1 
								&& absHeader.get(0)[1]!=null && absHeader.get(0)[1] != null) {
							if (datetime != null && !datetime.trim().equals("")) {
	
							} else {
								if(absHeader.get(0).length>=2){
									datetime = absHeader.get(0)[1];	
								}
							}
						}
						String value = "";
						if(absHeader.get(i).length>=2){
							value = absHeader.get(i)[1];
						}
						absHeaderMap.put(key, value);
					} catch (ArrayIndexOutOfBoundsException e) {
						absHeaderMap.put(key, "");
						e.printStackTrace();
					} catch (Exception e) {
						absHeaderMap.put(key, "");
						e.printStackTrace();
					}
				}
				absHeaderMap.put("测试类型Num", numStrs[1]);
				for (int i = 0; i < testScenario.size(); i++) {
					String key = testScenario.get(i)[0];
					try {
						if (key != null) {
							String value = "";
							if (i != 0) {
								if(testScenario.get(i).length>=2){
									value = testScenario.get(i)[1];	
								}
							}
							if (key.equals("")
									&& testScenario.get(i + 1).length >= 2
									&& testScenario.get(i + 1)[0] != null) {
								testScenarioMap.put(key, value);
							} else if (!key.equals("")) {
								testScenarioMap.put(key, value);
							}
						}
	
					} catch (ArrayIndexOutOfBoundsException e) {
						testScenarioMap.put(key, "");
						e.printStackTrace();
					} catch (Exception e) {
						testScenarioMap.put(key, "");
						e.printStackTrace();
					}
				}
				if (absBody != null && absBody.size() > 0) {
					String[] key = absBody.get(0);// 主体的表头
					if (key != null) {
						for (int i = 1; i < absBody.size(); i++) {// 主体的数据
							try{					// 第i排数据，i从1开始
								String[] value = absBody.get(i);
								if (!isBlankLine(value)) {
									Map map = new HashMap();
									for (int j = 0; j < key.length; j++) {
										String sonKey = key[j];
										String sonValue = "";
										try {
											sonValue = value[j];	
										} catch (Exception e) {
											sonValue = "";
										}
										map.put(sonKey, sonValue);
									}
									absBodyMap.put(i, map);
								}
							} catch (ArrayIndexOutOfBoundsException e) {
								absBodyMap.put(key, "");
								e.printStackTrace();
							} catch (Exception e) {
								absBodyMap.put(key, "");
								e.printStackTrace();
							}
						}
					}
				}
				if (absBodyMap != null) {
					if (absBodyMap.size() == 1) {
						Map nextMap = (Map) absBodyMap.get(1);
						String body = nextMap.toString();
						String header = absHeaderMap.toString();
						String testScenarip = testScenarioMap.toString();
	
						body = subTxt(body);
						header = subTxt(header);
						testScenarip = subTxt(testScenarip);
	
						str.append(body);
						if (!body.trim().equals("") && !header.trim().equals("")) {
							str.append(",");
						}
						str.append(header);
						if (!header.trim().equals("")
								&& !testScenarip.trim().equals("")) {
							str.append(",");
						}
						str.append(testScenarip);
					} else if (absBodyMap.size() > 1) {
						for (int i = 0; i < absBodyMap.size(); i++) {
							Map nextMap = (Map) absBodyMap.get(i + 1);
							String body = nextMap.toString();
							String header = absHeaderMap.toString();
							String testScenarip = testScenarioMap.toString();
	
							body = subTxt(body);
							header = subTxt(header);
							testScenarip = subTxt(testScenarip);
	
							str.append(body);
							if (!body.trim().equals("")
									&& !header.trim().equals("")) {
								str.append(",");
							}
							str.append(header);
							if (!header.trim().equals("")
									&& !testScenarip.trim().equals("")) {
								str.append(",");
							}
							str.append(testScenarip);
							str.append("\r\n");
						}
					} else {
						String header = absHeaderMap.toString();
						String testScenarip = testScenarioMap.toString();
	
						header = subTxt(header);
						testScenarip = subTxt(testScenarip);
	
						str.append(header);
						if (!header.trim().equals("")
								&& !testScenarip.trim().equals("")) {
							str.append(",");
						}
						str.append(testScenarip);
					}
				}
	
			} catch (Exception e) {
				e.printStackTrace();
			}
			try {
				//格式化时间
				String dataLong = "";
				if (datetime.isEmpty()) {
					dataLong = Long.toString(new Date().getTime());
				} else {
					String timeStrs[] = pattern2.split(datetime);
					StringBuffer rex = new StringBuffer("");
					for (int i = 0; i < timeStrs.length; i++) {
						rex.append(timeStrs[i]);
						rex.append(dateStr[i]);
					}
					Date date1 = null;
					SimpleDateFormat formatter = new SimpleDateFormat(
							rex.toString());
					try {
						date1 = formatter.parse(datetime);
					} catch (ParseException e) {
						e.printStackTrace();
					}
					dateTime = date1.toLocaleString();
					dataLong = Long.toString(date1.getTime());
				}
				
				String prefix = ConfParser.org_prefix;
				System.out.println(prefix);
				String org = file.getAbsolutePath().replace(prefix, "");
				org = org.substring(0, org.indexOf(File.separator));
	//			System.out.println("keyspace:" + org);
				String imei = file.getParentFile().getName();
				String filePath = file.getAbsolutePath().replace(ConfParser.org_prefix.replace(File.separator, "|"),"");
				String fileSize = convertFileSize(file.length());
				String file_lastModifies = getLastModifies(file);
				String fileName = file.getName();
				Map allMap = new HashMap();
				allMap.put("dateTime", dateTime);
				if (absBodyMap != null) {
					if (absBodyMap.size() == 1) {
						Map bodyNextMap = (Map) absBodyMap.get(1);
						String fileIndex = imei+"|"+file.getName()+ "_" + 0;
						allMap = setMapData(absHeaderMap, org, allMap, file,
								fileIndex, dataLong, imei);
						allMap = setMapData(testScenarioMap, org, allMap, file,
								fileIndex, dataLong, imei);
						allMap = setMapData(bodyNextMap, org, allMap, file,
								fileIndex, dataLong, imei);
						allMap.put("fileName", fileName);
						allMap.put("filePath", filePath);
						allMap.put("fileSize", fileSize);
						allMap.put("file_lastModifies", file_lastModifies);
						getAllDataByCsv(allMap,numStrs[1],org,fileIndex);
	//					insertCassandraDB(allMap, numType, file, keyspace,0);
					} else if (absBodyMap.size() > 1) {
						for (int i = 0; i < absBodyMap.size(); i++) {
							Map lMap = new HashMap();
							Map bodyNextMap = (Map) absBodyMap.get(i + 1);
							String fileIndex = imei+"|"+file.getName()+ "_" + i;
							lMap = setMapData(absHeaderMap, org, allMap, file,
									fileIndex, dataLong, imei);
							lMap = setMapData(testScenarioMap, org, allMap, file,
									fileIndex, dataLong, imei);
							lMap = setMapData(bodyNextMap, org, allMap, file,
									fileIndex, dataLong, imei);
							allMap.put("fileName", fileName);
							allMap.put("filePath", filePath);
							allMap.put("fileSize", fileSize);
							allMap.put("file_lastModifies", file_lastModifies);
							getAllDataByCsv(allMap,numStrs[1],org,fileIndex);
	//						insertCassandraDB(lMap, numType, file, keyspace,0);
						}
					}else{
						String fileIndex = imei+"|"+file.getName()+ "_" + 0;
						allMap = setMapData(absHeaderMap, org, allMap, file, fileIndex,
								dataLong, imei);
						allMap = setMapData(testScenarioMap, org, allMap, file,
								fileIndex, dataLong, imei);
						allMap.put("fileName", fileName);
						allMap.put("filePath", filePath);
						allMap.put("fileSize", fileSize);
						allMap.put("file_lastModifies", file_lastModifies);
						getAllDataByCsv(allMap,numStrs[1],org,fileIndex);
	//					insertCassandraDB(allMap, numType, file, keyspace,0);
					}
				} else {
					String fileIndex = imei+"|"+file.getName()+ "_" + 0;
					allMap = setMapData(absHeaderMap, org, allMap, file, fileIndex,
							dataLong, imei);
					allMap = setMapData(testScenarioMap, org, allMap, file,
							fileIndex, dataLong, imei);
					allMap.put("fileName", fileName);
					allMap.put("filePath", filePath);
					allMap.put("fileSize", fileSize);
					allMap.put("file_lastModifies", file_lastModifies);
					getAllDataByCsv(allMap,numStrs[1],org,fileIndex);
	//				insertCassandraDB(allMap, numType, file, keyspace,0);
				}
	
			} catch (Exception e) {
				e.printStackTrace();
			}
		} catch (Exception e) {
			System.out.println(file.getAbsolutePath());
//			if(file.getName().equals("00000000_02001.000_0-2014_06_14_17_50_28_714.abstract.csv")){
//				int j=0;
//				j++;
//			}
			e.getMessage();
			e.printStackTrace();
		}
	}
	private Map setMapData(Map DataMap, String numType, Map allMap, File file,
			String fileIndex, String dataLong, String imei) {
		Set set = DataMap.keySet();
		Iterator iter = set.iterator();
		while (iter.hasNext()) {
			String key = (String) iter.next();
			String value = "";
			if (key == null || key.isEmpty()) {
				key = "";
			}
			try {
				value = (String) DataMap.get(key);
			} catch (Exception e) {
			}
			allMap.put(key, value);
		}
		String prefix = ConfParser.org_prefix.replace(File.separator, "|");
		fileIndex = fileIndex.replace(prefix, "");
		Date date = new Date(Long.valueOf(dataLong));
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		String dateStr = sdf.format(date);
//		System.out.println("fileIndex:"+fileIndex+",dataLong:"+dataLong+",yearMonthDay:"+dateStr+",imei:"+imei);
		allMap.put("file_index", fileIndex);
		allMap.put("data_time", dataLong);
		allMap.put("year_month_day", dateStr);
		allMap.put("imei", imei);
		return allMap;
	}
	public static void main(String[] args) {
		try {
			SimpleDateFormat dateFormat = new SimpleDateFormat(
			"yyyy-MM-dd HH:mm:ss");
			Date start = new Date();
			String dateString = dateFormat.format(start);
			System.out.println(dateString + ">>> Start!");
			TestDataAnalyze test = new TestDataAnalyze();
			test.test();
			Date end = new Date();
			dateString = dateFormat.format(end);
			System.out.println("Start"+dateString+","+dateString + ">>> End!");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	/**
	 * 将从文件中读取到的数据写入到csv文件中
	 * @param map
	 * @param fileType
	 * @param keySpace
	 * @param fileIndex
	 */
	public void getAllDataByCsv(Map map,String fileType,String keySpace,String fileIndex){
		HashMap<String, String> myData = new HashMap<String, String>();
		List<Map<String, String>> columns = new ArrayList<Map<String, String>>() ;
		Set key = map.keySet();
		Iterator iter = key.iterator();
		while(iter.hasNext()){
			Map<String, String> hColumn = new HashMap<String,String>();
			try {
				String name = (String)iter.next();
				String value = (String)map.get(name);
				if(value!=null){
					hColumn.put("name",name);
					hColumn.put("value", value);
					columns.add(hColumn);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		int previGps = 0;
		int previNetType = 0;
		String dateTime = map.get("dateTime")==null?"":(String)map.get("dateTime");;
		String logversion = map.get("logversion")==null?"":(String)map.get("logversion");
		
		String gpsStr = "";
		String NetType1 = "";
		String NetType2 = "";
		String NetType = "";
		String signalStength1 = "";
		String signalStength2 = "";
		String signalStength = "";
		String sinr = "";
		
		//SIGNAL
		
		//SPEEDTEST
		String downloadSpeed = "";
		String uploadSpeed = "";
		String dlSpeedUnit = "";
		String ulSpeedUnit = "";
		String delaySpeedtest = "";
		String cellInfo1 = "";
		String cellInfo2 = "";
		String cellInfo = "";
		String lac = "";
		String cid = "";
		String tac = "";
		String pci = "";
		String tac_pci = "";
		String lac_cid = "";
		
		//HTTP
		String protocal = "";
		String testType = "";
		String meanSpeedUnit = "";
		String maxSpeedUnit = "";
		String meanSpeed = "";
		String maxSpeed = "";
		String delayHttp = "";
		
		//BROWSE
		String url = "";
		String delayBrowse = "";
		
		
		String province = "";
		String city = "";
		String district = "";
		String year_month_day = "";
		for (Map<String, String> hColumn : columns) {
			String name = hColumn.get("name");
			String value = hColumn.get("value");
			//获取网络制式及信号强度Start
			if((name.equals("网络(1)网络制式") || name.equals("网络（1）网络制式") || name.trim().toLowerCase().equals("network(1) type"))){
				NetType1 = value;
				continue;
			}else if(name.equals("网络(2)网络制式") || name.equals("网络（2）网络制式") || name.trim().toLowerCase().equals("network(2) type")){
				NetType2 = value;
				continue;
			}else if(name.equals("网络制式") || name.trim().toLowerCase().equals("network type")){
				if(previNetType<=2){
					previNetType = 2;
					NetType = value;
				}
				continue;
			}else if(name.equals("网络类型") || name.trim().toLowerCase().equals("network type")){
				if(previNetType<=1){
					previNetType = 1;
					NetType = value;
				}
				continue;
			}
			
			if(name.equals("网络(1)信号强度") || name.equals("网络（1）信号强度") || name.trim().toLowerCase().equals("network(1) signal strength")){
				signalStength1 = value;
				continue;
			}else if(name.equals("网络(2)信号强度") || name.equals("网络（2）信号强度") || name.trim().toLowerCase().equals("network(2) signal strength")){
				signalStength2 = value;
				continue;
			}else if(name.equals("信号强度") || name.trim().toLowerCase().equals("network signal strength")){
				if(previNetType<=1){
					previNetType = 1;
					signalStength = value;
				}
				continue;
			}
			//获取网络制式及信号强度End
			
			if(name.trim().toLowerCase().equals("sinr")){
				sinr = value;
				continue;
			}
			
			//获取GPS位置信息Start
			if(name.equals("GPS信息") || name.trim().toLowerCase().equals("gps")){
				if(previGps<5){
					previGps = 5;
					gpsStr = value;
				}
				continue;
			}else if(name.equals("GPS位置信息") || name.trim().toLowerCase().equals("gps")){
				if(previGps<4){
					previGps = 4;
					gpsStr = value;
				}
				continue;
			}else if(name.equals("GPS位置") || name.trim().toLowerCase().equals("gps")){
				if(previGps<3){
					previGps = 3;
					gpsStr = value;
				}
				continue;
			}else if(name.equals("测试位置") || name.trim().toLowerCase().equals("gps")){
				if(previGps<2){
					previGps = 3;
					gpsStr = value;
				}
				continue;
			}else if(name.equals("测试GPS位置") || name.trim().toLowerCase().equals("gps")){
				if(previGps<1){
					previGps = 3;
					gpsStr = value;
				}
				continue;
			}
			//获取GPS位置信息End
			
			//获取速率及时延信息Start
			if(name.equals("下行速率") || name.equals("下行速率(Mbps)") || name.equals("下行速率（Mbps）") || name.equals("下行速率(Kbps)") || name.equals("下行速率（Kbps）") || name.trim().toLowerCase().equals("down speed") || name.trim().toLowerCase().equals("down speed(kbps)") || name.trim().toLowerCase().equals("down speed(mbps)")){
				downloadSpeed = value;
				dlSpeedUnit = name;
				continue;
			}else if(name.equals("上行速率") || name.equals("上行速率(Mbps)") || name.equals("上行速率(Kbps)") || name.equals("上行速率（Mbps）") || name.equals("上行速率（Kbps）") || name.trim().toLowerCase().equals("up speed") || name.trim().toLowerCase().equals("up speed(kbps)") || name.trim().toLowerCase().equals("up speed(mbps)")){
				uploadSpeed = value;
				ulSpeedUnit = name;
				continue;
			}else if(name.equals("平均时延(ms)") || name.equals("平均时延（ms）") || name.equals("平均时延") || name.equals("时延") || name.equals("时延(ms)") || name.equals("时长(ms)") || name.equals("时长（ms）") || name.trim().toLowerCase().equals("avg time(ms)") || name.trim().toLowerCase().equals("latency") || name.trim().toLowerCase().equals("delay") || name.trim().toLowerCase().equals("average latency (ms)") ){
				if(!value.contains("(ms)")){
					value = value + "(ms)";
				}
				delaySpeedtest = value;
				delayHttp = value;
				delayBrowse = value;
				continue;
			}								
			//获取速率及时延信息End
			
			//获取SPEEDTEST小区信息Start
			if(name.equals("网络(1)小区信息") || name.equals("网络（1）小区信息") || name.trim().toLowerCase().equals("network(1) cell")){
				cellInfo1 = value;
				continue;
			}else if(name.equals("网络(2)小区信息") || name.equals("网络（2）小区信息") || name.trim().toLowerCase().equals("network(2) cell")){
				cellInfo2 = value;
				continue;
			}else if(name.equals("小区信息") || name.trim().toLowerCase().equals("network cell")){
				cellInfo = value;
				continue;
			}
			if(name.toLowerCase().equals("tac/pci")){
				tac_pci = value;
				continue;
			}else if(name.toLowerCase().equals("lac/cid")){
				lac_cid = value;
				continue;
			}else if(name.toLowerCase().equals("lac")){
				lac = value;
				continue;
			}else if(name.toLowerCase().equals("cid")){
				cid = value;
				continue;
			}else if(name.toLowerCase().equals("tac")){
				tac = value;
				continue;
			}else if(name.toLowerCase().equals("pci")){
				pci = value;
				continue;
			}
			//获取速率及时延信息End
			
			//获取HTTP速率及时延信息Start
			if(name.equals("平均速率(Mbps)") || name.equals("平均速率(Kbps)") || name.equals("平均速率（Mbps）") || name.equals("平均速率（Kbps）") || name.equals("平均速率") || name.trim().toLowerCase().equals("avg speed(kbps)") || name.trim().toLowerCase().equals("avg speed(mbps)")  || name.trim().toLowerCase().equals("avg speed") || name.trim().toLowerCase().equals("average speed (kbps)") || name.trim().toLowerCase().equals("average speed (mbps)")){
				meanSpeedUnit = name;
				meanSpeed = value;
				continue;
			}else if(name.equals("最大速率(Mbps)") || name.equals("最大速率(Kbps)") ||name.equals("最大速率（Mbps）") || name.equals("最大速率（Kbps）") || name.equals("最大速率") || name.trim().toLowerCase().equals("max speed(kbps)") || name.trim().toLowerCase().equals("max speed(mbps)") || name.trim().toLowerCase().equals("max speed")){
				maxSpeedUnit = name;
				maxSpeed = value;
				continue;
			}else if(name.equals("协议") || name.trim().toLowerCase().equals("protocol")){
				protocal = value;
				continue;
			}else if(name.equals("类型") || name.trim().toLowerCase().equals("type")){
				testType = value;
				continue;
			}							
			//获取速率及时延信息End
			
			//获取BROWSE时延信息Start
			//获取时延信息End
			if(name.equals("地址") || name.equals("网址") || name.trim().toLowerCase().equals("website") || name.trim().toLowerCase().equals("address") || name.trim().toLowerCase().equals("file")){
				url = value.trim();
				continue;
			}
			
			
			/*if(name.equals("cassandra_province")){
				province = value;
				continue;
			}
			if(name.equals("cassandra_city")){
				city = value;
				continue;
			}
			if(name.equals("cassandra_district")){
				district = value;
				continue;
			}
			if(name.equals("year_month_day")){
				year_month_day = value;
				continue;
			}*/
			
		}
		
		//数据格式整理
		String gpsStrResult = "";
		String[] gpsresult = new String[2];
		
		//SPEEDTEST
		String netResult1 = "";//网络1制式
		String netResult2 = "";//网络2制式
		String signalResult1 = "";//信号强度1
		String signalResult2 = "";//信号强度2
		String sinrResult = "";
		String dlSpeedResult = "";
		String ulSpeedResult = "";
		String delayResultSpeedtest = "";
		String cellResult1 = "";//小区信息1
		String cellResult2 = "";//小区信息2
		
		//BROWSE
		String urlResult = "";
		String delayResultBrowse = "";

		//HTTP
		String meanSpeedResult = "";
		String maxSpeedResult = "";
		String delayResultHttp = "";
		String protocalResult = "";
		String testTypeResult = "";
		
		String[] gps = new String[2];
		if(gpsStr != null && !gpsStr.trim().equals("--") && !gpsStr.trim().equals("-") && !gpsStr.trim().equals("")){
			gpsStrResult = gpsStr;
			if(gpsStr.contains(" ")){
				gps = transGpsPoint(gpsStr);
				if(gps!=null && gps[0]!=null && gps[1]!=null){
					gpsresult = gps;
				}
			}
		}
		boolean flag1 = false;
		boolean flag2 = false;
		
		if(NetType1!=null && !NetType1.equals("") && !NetType1.trim().equals("-") && !NetType1.trim().equals("--") && !NetType1.trim().equals("N/A")){
			flag1 = true;
			netResult1 = NetType1.trim();
			signalResult1 = signalStength1.trim();
			cellResult1 = cellInfo1.trim();
		}
		if(NetType2!=null && !NetType2.equals("") && !NetType2.trim().equals("-") && !NetType2.trim().equals("--") && !NetType2.trim().equals("N/A")){
			flag2 = true;
			netResult2 = NetType2.trim();
			signalResult2 = signalStength2.trim();
			cellResult2 = cellInfo2.trim();
		}
		if(!flag1 && !flag2){
			if(NetType!=null && !NetType.equals("") && !NetType.trim().equals("-") && !NetType.trim().equals("--") && !NetType.trim().equals("N/A")){
				netResult1 = NetType.trim();
				signalResult1 = signalStength.trim();
				cellResult1 = cellInfo.trim();
			}	
		}
		if(sinr!=null && !sinr.equals("") && !sinr.trim().equals("-") && !sinr.trim().equals("--") && !sinr.trim().equals("N/A") ){
			sinrResult = sinr.trim();
		}
		if(cellResult1!=null && !cellResult1.equals("") && !cellResult1.trim().equals("-") && !cellResult1.trim().equals("--") && !cellResult1.trim().equals("N/A") && !cellResult1.trim().equals("N/A/N/A")){
			
		}else{
			if(netResult1.equals("LTE") && tac_pci!=null && !tac_pci.equals("") && !tac_pci.trim().equals("-") && !tac_pci.trim().equals("--") && !tac_pci.trim().equals("N/A") && !tac_pci.trim().equals("N/A/N/A")){
				cellResult1 = tac_pci.trim();
			}else if(netResult1.equals("LTE") && tac!=null && !tac.equals("") && !tac.trim().equals("-") && !tac.trim().equals("--") && !tac.trim().equals("N/A") && !(tac.trim().toLowerCase().indexOf("n/a")!=-1) && pci!=null && !pci.equals("") && !pci.trim().equals("-") && !pci.trim().equals("--") && !pci.trim().equals("N/A") && !(pci.trim().toLowerCase().indexOf("n/a")!=-1)){
				if(tac.toLowerCase().indexOf("tac")!=-1){
					tac = tac.toLowerCase().replace("tac=", "");
				}
				if(pci.toLowerCase().indexOf("pci")!=-1){
					pci = pci.toLowerCase().replace("pci=", "");
				}
				cellResult1 = tac+"/"+pci;
			}else if(lac_cid!=null && !lac_cid.equals("") && !lac_cid.trim().equals("-") && !lac_cid.trim().equals("--") && !lac_cid.trim().equals("N/A") && !lac_cid.trim().equals("N/A/N/A")){
				cellResult1 = lac_cid.trim();
			}else if(lac!=null && !lac.equals("") && !lac.trim().equals("-") && !lac.trim().equals("--") && !lac.trim().equals("N/A") && !(lac.trim().toLowerCase().indexOf("n/a")!=-1) && cid!=null && !cid.equals("") && !cid.trim().equals("-") && !cid.trim().equals("--") && !cid.trim().equals("N/A") && !(cid.trim().toLowerCase().indexOf("n/a")!=-1)){
				if(lac.toLowerCase().indexOf("lac")!=-1){
					lac = lac.toLowerCase().replace("lac=", "");
				}
				if(cid.toLowerCase().indexOf("cid")!=-1){
					lac = lac.toLowerCase().replace("cid=", "");
				}
				cellResult1 = lac+"/"+cid;
			}
		}
		if(cellResult2!=null && !cellResult2.equals("") && !cellResult2.trim().equals("-") && !cellResult2.trim().equals("--") && !cellResult2.trim().equals("N/A") && !cellResult2.trim().equals("N/A/N/A")){
			
		}else{
			if(netResult2.equals("LTE") && tac_pci!=null && !tac_pci.equals("") && !tac_pci.trim().equals("-") && !tac_pci.trim().equals("--") && !tac_pci.trim().equals("N/A") && !tac_pci.trim().equals("N/A/N/A")){
				cellResult2 = tac_pci.trim();
			}else if(netResult2.equals("LTE") && tac!=null && !tac.equals("") && !tac.trim().equals("-") && !tac.trim().equals("--") && !tac.trim().equals("N/A") && !(tac.trim().toLowerCase().indexOf("n/a")!=-1) && pci!=null && !pci.equals("") && !pci.trim().equals("-") && !pci.trim().equals("--") && !pci.trim().equals("N/A") && !(pci.trim().toLowerCase().indexOf("n/a")!=-1)){
				if(tac.toLowerCase().indexOf("tac")!=-1){
					tac = tac.toLowerCase().replace("tac=", "");
				}
				if(pci.toLowerCase().indexOf("pci")!=-1){
					pci = pci.toLowerCase().replace("pci=", "");
				}
				cellResult2 = tac+"/"+pci;
			}else if(lac_cid!=null && !lac_cid.equals("") && !lac_cid.trim().equals("-") && !lac_cid.trim().equals("--") && !lac_cid.trim().equals("N/A") && !lac_cid.trim().equals("N/A/N/A")){
				cellResult2 = lac_cid.trim();
			}else if(lac!=null && !lac.equals("") && !lac.trim().equals("-") && !lac.trim().equals("--") && !lac.trim().equals("N/A") && !(lac.trim().toLowerCase().indexOf("n/a")!=-1) && cid!=null && !cid.equals("") && !cid.trim().equals("-") && !cid.trim().equals("--") && !cid.trim().equals("N/A") && !(cid.trim().toLowerCase().indexOf("n/a")!=-1)){
				if(lac.toLowerCase().indexOf("lac")!=-1){
					lac = lac.toLowerCase().replace("lac=", "");
				}
				if(cid.toLowerCase().indexOf("cid")!=-1){
					lac = lac.toLowerCase().replace("cid=", "");
				}
				cellResult2 = lac+"/"+cid;
			}
		}
//		if(cellResult!=null && !cellResult.equals("") && !cellResult.trim().equals("-") && !cellResult.trim().equals("--") && !cellResult.trim().equals("N/A") && !cellResult.trim().equals("N/A/N/A")){
//			
//		}else{
//			if(netResult.equals("LTE") && tac_pci!=null && !tac_pci.equals("") && !tac_pci.trim().equals("-") && !tac_pci.trim().equals("--") && !tac_pci.trim().equals("N/A") && !tac_pci.trim().equals("N/A/N/A")){
//				cellResult = tac_pci.trim();
//			}else if(netResult.equals("LTE") && tac!=null && !tac.equals("") && !tac.trim().equals("-") && !tac.trim().equals("--") && !tac.trim().equals("N/A") && !(tac.trim().toLowerCase().indexOf("n/a")!=-1) && pci!=null && !pci.equals("") && !pci.trim().equals("-") && !pci.trim().equals("--") && !pci.trim().equals("N/A") && !(pci.trim().toLowerCase().indexOf("n/a")!=-1)){
//				if(tac.toLowerCase().indexOf("tac")!=-1){
//					tac = tac.toLowerCase().replace("tac=", "");
//				}
//				if(pci.toLowerCase().indexOf("pci")!=-1){
//					pci = pci.toLowerCase().replace("pci=", "");
//				}
//				cellResult = tac+"/"+pci;
//			}else if(lac_cid!=null && !lac_cid.equals("") && !lac_cid.trim().equals("-") && !lac_cid.trim().equals("--") && !lac_cid.trim().equals("N/A") && !lac_cid.trim().equals("N/A/N/A")){
//				cellResult2G = lac_cid.trim();
//			}else if(lac!=null && !lac.equals("") && !lac.trim().equals("-") && !lac.trim().equals("--") && !lac.trim().equals("N/A") && !(lac.trim().toLowerCase().indexOf("n/a")!=-1) && cid!=null && !cid.equals("") && !cid.trim().equals("-") && !cid.trim().equals("--") && !cid.trim().equals("N/A") && !(cid.trim().toLowerCase().indexOf("n/a")!=-1)){
//				if(lac.toLowerCase().indexOf("lac")!=-1){
//					lac = lac.toLowerCase().replace("lac=", "");
//				}
//				if(cid.toLowerCase().indexOf("cid")!=-1){
//					lac = lac.toLowerCase().replace("cid=", "");
//				}
//				cellResult2G = lac+"/"+cid;
//			}
//		}
//		if(netResult!=null && !netResult.trim().equals("LTE") && cellResult!=null && !cellResult.equals("") && !cellResult.trim().equals("-") && !cellResult.trim().equals("--") && !cellResult.trim().equals("N/A")){
//			cellResult = cellResult;
//		}else if(netResult!=null && !netResult.trim().equals("LTE") && cellResult2G!=null && !cellResult2G.equals("") && !cellResult2G.trim().equals("-") && !cellResult2G.trim().equals("--") && !cellResult2G.trim().equals("N/A")){
//			cellResult = cellResult2G;
//		}else if(cellResult!=null && !cellResult.equals("") && !cellResult.trim().equals("-") && !cellResult.trim().equals("--") && !cellResult.trim().equals("N/A")){
//			cellResult = cellResult;
//		}else if(cellResult2G!=null && !cellResult2G.equals("") && !cellResult2G.trim().equals("-") && !cellResult2G.trim().equals("--") && !cellResult2G.trim().equals("N/A")){
//			cellResult = cellResult2G;
//		}
		
		if(downloadSpeed!=null && !downloadSpeed.equals("") && !downloadSpeed.trim().equals("-") && !downloadSpeed.trim().equals("--") && !downloadSpeed.trim().equals("N/A") ){
			dlSpeedResult = downloadSpeed.trim();
		}
		if(uploadSpeed!=null && !uploadSpeed.equals("") && !uploadSpeed.trim().equals("-") && !uploadSpeed.trim().equals("--") && !uploadSpeed.trim().equals("N/A")){
			ulSpeedResult = uploadSpeed.trim();
		}
		if(delaySpeedtest!=null && !delaySpeedtest.equals("") && !delaySpeedtest.trim().equals("-") && !delaySpeedtest.trim().equals("--") && !delaySpeedtest.trim().equals("N/A")){
			delayResultSpeedtest = delaySpeedtest.trim();
		}
		
		//HTTP参数
		if(meanSpeed!=null && !meanSpeed.equals("") && !meanSpeed.trim().equals("-") && !meanSpeed.trim().equals("--") && !meanSpeed.trim().equals("N/A") ){
			meanSpeedResult = meanSpeed.trim();
		}
		if(maxSpeed!=null && !maxSpeed.equals("") && !maxSpeed.trim().equals("-") && !maxSpeed.trim().equals("--") && !maxSpeed.trim().equals("N/A")){
			maxSpeedResult = maxSpeed.trim();
		}
		if(delayHttp!=null && !delayHttp.equals("") && !delayHttp.trim().equals("-") && !delayHttp.trim().equals("--") && !delayHttp.trim().equals("N/A")){
			delayResultHttp = delayHttp.trim();
		}
		if(protocal!=null && !protocal.equals("") && !protocal.trim().equals("-") && !protocal.trim().equals("--") && !protocal.trim().equals("N/A")){
			protocalResult = protocal.trim();
		}
		if(testType!=null && !testType.equals("") && !testType.trim().equals("-") && !testType.trim().equals("--") && !testType.trim().equals("N/A")){
			testTypeResult = testType.trim();
		}

		//Browse参数
		if(url!=null && !url.equals("") && !url.trim().equals("-") && !url.trim().equals("--") && !url.trim().equals("N/A")){
			urlResult = url.trim();
		}
		
		if(delayBrowse!=null && !delayBrowse.equals("") && !delayBrowse.trim().equals("-") && !delayBrowse.trim().equals("--") && !delayBrowse.trim().equals("N/A")){
			delayResultBrowse = delayBrowse.trim();
		}
										
		ArrayList<String> myDataList = new ArrayList<String>();
		myDataList.add(dateTime==null?"":dateTime);
		myDataList.add(logversion==null?"":logversion);
		
		myDataList.add(fileType==null?"":fileType);
		myDataList.add(gpsStrResult==null?"":gpsStrResult);
		myDataList.add(gpsresult[0]==null?"":gpsresult[0]);
		myDataList.add(gpsresult[1]==null?"":gpsresult[1]);
		myDataList.add(fileIndex==null?"":fileIndex);
		myDataList.add(netResult1==null?"":netResult1);
		myDataList.add(netResult2==null?"":netResult2);
		myDataList.add(signalResult1==null?"":signalResult1);
		myDataList.add(signalResult2==null?"":signalResult2);
		myDataList.add(dlSpeedResult==null?"":dlSpeedResult);
		myDataList.add(ulSpeedResult==null?"":ulSpeedResult);
		myDataList.add(delayResultSpeedtest==null?"":delayResultSpeedtest);
		myDataList.add(cellResult1==null?"":cellResult1);
		myDataList.add(cellResult2==null?"":cellResult2);
		myDataList.add(urlResult==null?"":urlResult);
		myDataList.add(delayResultBrowse==null?"":delayResultBrowse);
		myDataList.add(meanSpeedResult==null?"":meanSpeedResult);
		myDataList.add(maxSpeedResult==null?"":maxSpeedResult);
		
		myDataList.add(delayResultHttp==null?"":delayResultHttp);
		myDataList.add(protocalResult==null?"":protocalResult);
		myDataList.add(testTypeResult==null?"":testTypeResult);
		myDataList.add(province==null?"":province);
		myDataList.add(city==null?"":city);
		myDataList.add(district==null?"":district);
		myDataList.add(year_month_day==null?"":year_month_day);
		myDataList.add(sinrResult==null?"":sinrResult);
		String prefix = ConfParser.org_prefix;
		File csvFile = new File(ConfParser.csvwritepath+File.separator+keySpace+".csv");
		BufferedWriter writer = null;
		CsvWriter cwriter = null;
		try {
			if(!csvFile.exists()){
				csvFile.getParentFile().mkdirs();
				csvFile.createNewFile();
			}
			writer = new BufferedWriter(new FileWriter(csvFile,true));
			cwriter = new CsvWriter(writer,',');
		} catch (FileNotFoundException e){
			e.printStackTrace();
//			try {
//				Thread.sleep(100000000);
//			} catch (InterruptedException e1) {
//				e1.printStackTrace();
//			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		try {
			List list = new ArrayList();
            for(int i = 0;i < myDataList.size();i++){
            	String str = myDataList.get(i).toString();
            	list.add(i, str);
            }
            String myStr = "";
            for (int i = 0; i < list.size(); i++) {
				String string = (String)list.get(i);
				myStr = myStr + string;
				if(i==list.size()-1){
					myStr = myStr + "";
				}else{
					myStr = myStr + ",";
				}
            }
            logger.debug(myStr);
            cwriter.writeRecord(myStr.split(","), true);
		} catch (IOException e) {
			e.printStackTrace();
		}finally{
			if(writer!=null){
				try {
					writer.flush();
					writer.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			if(cwriter!=null){
				cwriter.flush();
				cwriter.close();
			}
		}
	}
	
	/**
	 * 判断起始时间的跨度，0代表在同一天，1代表隔天，2代表相隔多天
	 * @param startDate
	 * @param endDate
	 * @return
	 */
	private int inOneDay(Date startDate, Date endDate) throws Exception{
		SimpleDateFormat dateFormat = new SimpleDateFormat(
				"yyyyMMdd");
		String startStr = dateFormat.format(startDate);
		long startDay = (dateFormat.parse(startStr)).getTime();

		String endStr = dateFormat.format(endDate);
		long endDay = (dateFormat.parse(endStr)).getTime();
		if((endDay-startDay)<24*3600*1000){
			return 0;
		}else if((endDay-startDay)>24*3600*1000){
			return 2;
		}
		return 1;
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
	/*public static void main(String[] args) {
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
		
		SimpleDateFormat dateFormat = new SimpleDateFormat(
				"yyyy-MM-dd HH:mm:ss");
		String dateString = dateFormat.format(new Date());
		
		TestDataAnalyze testData = new TestDataAnalyze();
		JSONObject jsonObject = new JSONObject();
		jsonObject.put("province", "北京市");
		jsonObject.put("start_time", "2014-11-14 00:00:00");
		jsonObject.put("end_time", "2014-11-20 21:00:00");
		JSONArray jsonarray = new JSONArray();
		
		jsonarray.add(0,"CMCC_NEIMENGGU_ZHIDONGBOCE");
		
		jsonObject.put("orgs", jsonarray);
		JSONArray jsonarray1 = new JSONArray();
//		jsonarray1.add(0,"signal_strength_3G"); //3G制式的为-85标准
//		jsonarray1.add(0,"02001");//GSM制式的为-90标准  无省份的过滤才能通过
		jsonarray1.add(0,"all");//GSM制式的为-90标准  无省份的过滤才能通过
		
		jsonObject.put("test_types", jsonarray1);
		try {
			//List list = testData.getTestDataByLocation(jsonObject);
			//JSONArray keyspaces, JSONArray columnFamilies, String startKey, int count, boolean isDetail
			testData.getAllData((JSONArray)jsonObject.get("orgs"), (JSONArray)jsonObject.get("test_types"), "", -1, true);
				
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		System.out.println(dateString + ">>> Start!");
		
		dateString = dateFormat.format(new Date());
		System.out.println(dateString + ">>> End!");
	}*/
	
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
		try{
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
		}catch(Exception e){
			e.printStackTrace();
		}
		
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
