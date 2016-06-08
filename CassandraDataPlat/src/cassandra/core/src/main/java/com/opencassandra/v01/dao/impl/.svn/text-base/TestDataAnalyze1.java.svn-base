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
import me.prettyprint.hector.api.factory.HFactory;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

public class TestDataAnalyze1 {
	JSONObject requestBody = null;
	
	public TestDataAnalyze1(JSONObject requestBody){
		this.requestBody = requestBody;
	} 
	public TestDataAnalyze1(){
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
				return this.getGpsDataByLocationProvince(province, orgs, testTypes, startDate, endDate);
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
	
	/**
	 * 根据省份获取测试数据
	 * @author Ocean
	 * @param province
	 * @param city
	 * @param district
	 * @param orgs
	 * @param testTypes
	 */
	public List<Map<String, String>> getReportDataByLocationProvince(String province, JSONArray orgs, JSONArray testTypes, Date startDate, Date endDate) throws Exception{
		List<QueryExpression> qe = null;
		long startTime = startDate.getTime();
		long endTime = endDate.getTime();
		SimpleDateFormat dateFormat = new SimpleDateFormat(
				"yyyyMMdd");
		String startStr = dateFormat.format(startDate);
		String endStr = dateFormat.format(endDate);
		
		List<String> days = this.parseDays(startDate, endDate);
		if(days.size()==1){
			QueryExpression myQEDay = new QueryExpression("year_month_day",IndexOperator.EQ,startStr);
			QueryExpression myQETime = new QueryExpression("data_time",IndexOperator.GTE,""+startTime);
			QueryExpression myQEProvince = new QueryExpression("cassandra_province",IndexOperator.EQ,""+startTime);
		}
		QueryExpression myQE = new QueryExpression("",IndexOperator.EQ,"");
		//getTestData();
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
	public List<Map<String, String>> getGpsDataByLocationProvince(String province, JSONArray orgs, JSONArray testTypes, Date startDate, Date endDate) throws Exception{
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
			QueryExpression myQEProvince = new QueryExpression("cassandra_province",IndexOperator.EQ,""+province);
			qe.add(myQEDay);
			qe.add(myQETime);
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
	
	public List<Map<String, String>> getTestDataByLocationCity(String province, String city, String district, JSONArray orgs, JSONArray testTypes){
		return null;
	}
	public List<Map<String, String>> getTestDataByLocationDistrict(String province, String city, String district, JSONArray orgs, JSONArray testTypes){
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
									if(netValue != null && !netValue.equals("N/A") && !netValue.equals("")){
										int value = Integer.parseInt(netValue.toLowerCase().replace("dbm", ""));
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
									if(netValue != null && !netValue.equals("N/A") && !netValue.equals("")){
										int value = Integer.parseInt(netValue.toLowerCase().replace("dbm", ""));
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
									if(netValue != null && !netValue.equals("N/A") && !netValue.equals("")){
										int value = Integer.parseInt(netValue.toLowerCase().replace("dbm", ""));
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
									if(netValue != null && !netValue.equals("N/A") && !netValue.equals("")){
										int value = Integer.parseInt(netValue.toLowerCase().replace("dbm", ""));
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
									if(netValue != null && !netValue.equals("N/A") && !netValue.equals("")){
										int value = Integer.parseInt(netValue.toLowerCase().replace("dbm", ""));
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
									if(netValue != null && !netValue.equals("N/A") && !netValue.equals("")){
										int value = Integer.parseInt(netValue.toLowerCase().replace("dbm", ""));
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
							System.out.println(list.getKey());
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
	public List<Map<String, String>> get3GTestData(JSONArray keyspaces, JSONArray columnFamilies, List<ArrayList<QueryExpression>> qes, int count){
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
		
		int allCount = 0;//所有符合3G网络制式条件的数据数量
		int resultCount = 0;//所有符合网络信号>=-85的数据数量
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
//								System.out.println(""+columnName+" : "+columnValue);
							}
							String netValue = "";
							//判断条件
							String netWorkType1 = "";
							String netWorkType2 = "";
							
							String singel = columnFamilyStr.substring(columnFamilyStr.lastIndexOf("_")+1, columnFamilyStr.length());
							if (columnMap.containsKey("网络(2)网络制式") && columnMap.containsKey("网络(1)网络制式"))
							{
								netWorkType1 = (String)columnMap.get("网络(1)网络制式");
								netWorkType2 = (String)columnMap.get("网络(2)网络制式");
								if(netWorkType1.equals(singel)){
									allCount++;
									netValue = (String)columnMap.get("网络(1)信号强度");
									if(netValue != null && !netValue.equals("N/A") && !netValue.equals("")){
										int value = Integer.parseInt(netValue.toLowerCase().replace("dbm", ""));
										System.out.println((String)columnMap.get("网络(1)信号强度")+","+value +","+ (value>-85));
										if(value>=-85){
											resultCount++;
										}
									}	
								}
								if(netWorkType2.equals(singel)){
									allCount++;
									netValue = (String)columnMap.get("网络(2)信号强度"); 
									if(netValue != null && !netValue.equals("N/A") && !netValue.equals("")){
										int value = Integer.parseInt(netValue.toLowerCase().replace("dbm", ""));
										System.out.println((String)columnMap.get("网络(1)信号强度")+","+value +","+ (value>-85));
										if(value>=-85){
											resultCount++;
										}
									}	
								}
							} else if (columnMap.containsKey("网络(2)网络制式")){
								netWorkType1 = (String)columnMap.get("网络(2)网络制式");
								if(netWorkType1.equals(singel)){
									allCount++;
									netValue = (String)columnMap.get("网络(2)信号强度"); 
									if(netValue != null && !netValue.equals("N/A") && !netValue.equals("")){
										int value = Integer.parseInt(netValue.toLowerCase().replace("dbm", ""));
										System.out.println((String)columnMap.get("网络(1)信号强度")+","+value +","+ (value>-85));
										if(value>-85){
											resultCount++;
										}
									}
								}
							} else if (columnMap.containsKey("网络(1)网络制式")){
								netWorkType1 = (String)columnMap.get("网络(1)网络制式");
								if(netWorkType1.equals(singel)){
									allCount++;
									netValue = (String)columnMap.get("网络(1)信号强度"); 
									if(netValue != null && !netValue.equals("N/A") && !netValue.equals("")){
										int value = Integer.parseInt(netValue.toLowerCase().replace("dbm", ""));
										System.out.println((String)columnMap.get("网络(1)信号强度")+","+value +","+ (value>-85));
										if(value>=-85){
											resultCount++;
										}
									}
								}
							} else if (columnMap.containsKey("网络制式")){
								netWorkType1 = (String)columnMap.get("网络制式");
								if(netWorkType1.equals(singel)){
									allCount++;
									netValue = (String)columnMap.get("信号强度"); 
									if(netValue != null && !netValue.equals("N/A") && !netValue.equals("")){
										int value = Integer.parseInt(netValue.toLowerCase().replace("dbm", ""));
										System.out.println((String)columnMap.get("网络(1)信号强度")+","+value +","+ (value>-85));
										if(value>=-85){
											resultCount++;
										}
									}
								}
							} else if (columnMap.containsKey("网络类型")){
								netWorkType1 = (String)columnMap.get("网络类型");
								if(netWorkType1.equals(singel)){
									allCount++;
									netValue = (String)columnMap.get("信号强度"); 
									if(netValue != null && !netValue.equals("N/A") && !netValue.equals("")){
										int value = Integer.parseInt(netValue.toLowerCase().replace("dbm", ""));
										System.out.println((String)columnMap.get("网络(1)信号强度")+","+value +","+ (value>-85));
										if(value>=-85){
											resultCount++;
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
		
		System.out.println("总量："+allCount);
		System.out.println("符合量"+resultCount);
		System.out.println("总处理数据量"+total);
		
		return result;
	}
	
	public List<Map<String, String>> getTestData(JSONArray keyspaces, JSONArray columnFamilies, List<ArrayList<QueryExpression>> qes, String startKey, int count){
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
							Map<String, String> myData = new HashMap<String, String>();
							Collection<String> element = list.getColumnNames();
							Object[] columns = element.toArray();
							stepCount++;
							if(total==count){
								break;
							}
							for(int r=0; r<columns.length; r++){
								
								String gpsStr = "";
								if(((String)columns[r]).contains("GPS位置")){
									gpsStr = list.getString(""+columns[r]);
								}else if(((String)columns[r]).contains("GPS位置信息")){
									gpsStr = list.getString(""+columns[r]);
								}else if(((String)columns[r]).contains("GPS信息")){
									gpsStr = list.getString(""+columns[r]);
								}else if(((String)columns[r]).contains("测试位置")){
									gpsStr = list.getString(""+columns[r]);
								}else if(((String)columns[r]).contains("测试GPS位置")){
									gpsStr = list.getString(""+columns[r]);
								}else{
									gpsStr = "";
								}
								if(gpsStr != null && !gpsStr.trim().equals("--") && !gpsStr.trim().equals("")){
									if(gpsStr.contains(" ")){
										String[] gps = transGpsPoint(gpsStr);
										if(gps!=null && gps[0]!=null && gps[1]!=null){
											myData.put("GPSLocation", gps[0]+" "+gps[1]);
											myData.put("imgUrl", "1.png");
											result.add(myData);
											total++;
										}
									}
								}else{
									continue;
								}
								System.out.println(""+columns[r]+" : "+list.getString(""+columns[r]));
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
		
		
		System.out.println("总处理数据量"+total);
		
		return result;
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
	public static void main(String[] args) {
		TestDataAnalyze1 testData = new TestDataAnalyze1();
		JSONObject jsonObject = new JSONObject();
		jsonObject.put("province", "北京市");
		jsonObject.put("start_time", "2014-06-27 00:00:00");
		jsonObject.put("end_time", "2014-11-18 00:00:00");
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
	}
	
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
