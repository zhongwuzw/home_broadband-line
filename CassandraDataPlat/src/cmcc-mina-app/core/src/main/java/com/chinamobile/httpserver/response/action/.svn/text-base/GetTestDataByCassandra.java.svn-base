package com.chinamobile.httpserver.response.action;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.apache.log4j.Logger;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.example.httpserver.codec.HttpRequestMessage;
import org.apache.mina.example.httpserver.codec.HttpResponseMessage;
import org.apache.mina.example.httpserver.uploadfactory.UploadCodeSet;

import cn.speed.history.service.MySelectService;

import com.chinamobile.httpserver.data.DataParser;
import com.chinamobile.httpserver.response.CmriHttpResponse;
import com.chinamobile.httpserver.response.CmriJsonResponse;
import com.chinamobile.util.UpdateApplyRespXML;
import com.opencassandra.v01.dao.impl.TestDataAnalyze;

public class GetTestDataByCassandra implements CmriJsonResponse{

	private static GetTestDataByCassandra instance = null;
	static Logger logger = Logger.getLogger(GetTestDataByCassandra.class);
	
	public static GetTestDataByCassandra getInstance() {
		if(instance == null){
			return new GetTestDataByCassandra();
		}
        return instance;
    }
	
	public GetTestDataByCassandra() {}
	
	@Override
	public HttpResponseMessage execute(IoSession session,Object message) throws IOException {		
		
		JSONArray jsonArray = null;
		JSONObject jsonObject = null;
		String jsonData = "";
		
		String requestInterface = ((HttpRequestMessage)message).getContext();
		String requestBody = ((HttpRequestMessage)message).getBody();
		JSONObject requestBodyJson = null;
		try{
			requestBodyJson = JSONObject.fromObject(requestBody);

			String startTime = (String)requestBodyJson.get("start_time");
			String endTime = (String)requestBodyJson.get("end_time");
			SimpleDateFormat dateFormat = new SimpleDateFormat(
					"yyyy-MM-dd HH:mm:ss");
			Date startDate = dateFormat.parse(startTime);
			Date endDate = dateFormat.parse(endTime);
			if((startDate.getTime()-endDate.getTime())>0){
				throw new Exception("Bad Date Time Params");
			}
		}catch(Exception e){
			e.printStackTrace();
			HttpResponseMessage response = new HttpResponseMessage();
			response.setResponseCode(HttpResponseMessage.HTTP_STATUS_NOT_ALLOWED);
			
			JSONObject requestMessage = new JSONObject();
			requestMessage.put("state", 403);
			requestMessage.put("message", "Bad Params");
			response.appendBody(requestMessage.toString());
			return response;
		}
		
		if(requestInterface.equals("queryTestDataByLocation.cas")){
			TestDataAnalyze myTDA = new TestDataAnalyze(requestBodyJson);
			List<Map<String, String>> list = null;
			try {
				list = myTDA.getTestDataByLocation();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
			jsonArray = JSONArray.fromObject(list);
			jsonData = jsonArray.toString();
		}
		
		if(requestInterface.equals("queryTestDataGpsByLocation.cas")){
			TestDataAnalyze myTDA = new TestDataAnalyze(requestBodyJson);
			List<Map<String, String>> list = null;
			try {
				list = myTDA.getGpsDataByLocation();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
			jsonArray = JSONArray.fromObject(list);
			jsonData = jsonArray.toString();
		}
		
		if(requestInterface.equals("queryTestDataReportByLocation.cas")){
			TestDataAnalyze myTDA = new TestDataAnalyze(requestBodyJson);
			Map<String, Object> list = null;
			try {
				list = myTDA.getReportDataByLocation();
				Map<String,String> details = (HashMap<String,String>)list.get("detail");
				List<JSONObject> transDetails = new ArrayList<JSONObject>();
				if (details != null && !details.isEmpty()) {
					Set<String> keys = details.keySet();
					for (String key : keys) {
						JSONObject result = new JSONObject();
						result.put("name", key);
						result.put("count", details.get(key));
						transDetails.add(result);
					}
				}
				list.put("detail", transDetails);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
			jsonObject = JSONObject.fromObject(list);
			jsonData = jsonObject.toString();
		}
		

		if(requestInterface.equals("queryTestDataReportGpsByLocation.cas")){
			TestDataAnalyze myTDA = new TestDataAnalyze(requestBodyJson);
			Map<String, Object> list = null;
			try {
				list = myTDA.getReportGpsDataByLocation();
				Map<String,String> details = (HashMap<String,String>)list.get("detail");
				List<JSONObject> transDetails = new ArrayList<JSONObject>();
				if (details != null && !details.isEmpty()) {
					Set<String> keys = details.keySet();
					for (String key : keys) {
						JSONObject result = new JSONObject();
						result.put("name", key);
						result.put("count", details.get(key));
						transDetails.add(result);
					}
				}
				list.put("detail", transDetails);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
			jsonObject = JSONObject.fromObject(list);
			jsonData = jsonObject.toString();
		}
		
		if(requestInterface.equals("queryTestDataSignalByLocation.cas")){
			TestDataAnalyze myTDA = new TestDataAnalyze(requestBodyJson);
			Map<String, Object> list = null;
			try {
				list = myTDA.getSignalDataByLocation();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
			jsonObject = JSONObject.fromObject(list);
			jsonData = jsonObject.toString();
		}
		
		if(requestInterface.equals("queryTestDataSpeedtestByLocation.cas")){
			TestDataAnalyze myTDA = new TestDataAnalyze(requestBodyJson);
			Map<String, Object> list = null;
			try {
				list = myTDA.getSpeedtestDataByLocation();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
			jsonObject = JSONObject.fromObject(list);
			jsonData = jsonObject.toString();
		}
		
		if(requestInterface.equals("queryTestDataHttpTestByLocation.cas")){
			TestDataAnalyze myTDA = new TestDataAnalyze(requestBodyJson);
			Map<String, Object> list = null;
			try {
				list = myTDA.getHttpTestDataByLocation();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
			jsonObject = JSONObject.fromObject(list);
			jsonData = jsonObject.toString();
		}
		
		if(requestInterface.equals("queryTestDataBrowseTestByLocation.cas")){
			TestDataAnalyze myTDA = new TestDataAnalyze(requestBodyJson);
			Map<String, Object> list = null;
			try {
				list = myTDA.getBrowseTestDataByLocation();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
			jsonObject = JSONObject.fromObject(list);
			jsonData = jsonObject.toString();
		}
		
		
		
		HttpResponseMessage response = new HttpResponseMessage();
		response.setResponseCode(HttpResponseMessage.HTTP_STATUS_SUCCESS);
		response.appendBody(jsonData);
		return response;
	}

}
