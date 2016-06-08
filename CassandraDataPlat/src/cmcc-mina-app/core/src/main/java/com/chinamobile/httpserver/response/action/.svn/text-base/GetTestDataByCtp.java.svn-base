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
import com.opencassandra.v01.dao.impl.IntoCSVFromDB;
import com.opencassandra.v01.dao.impl.IntoCSVFromDBByTestType;
import com.opencassandra.v01.dao.impl.TestDataAnalyze;
import com.opencassandra.v01.dao.impl.TestDataChase;
import com.opencassandra.v01.dao.impl.TestDataChaseEng;

public class GetTestDataByCtp implements CmriJsonResponse{

	private static GetTestDataByCtp instance = null;
	static Logger logger = Logger.getLogger(GetTestDataByCtp.class);
	
	public static GetTestDataByCtp getInstance() {
		if(instance == null){
			return new GetTestDataByCtp();
		}
        return instance;
    }
	
	public GetTestDataByCtp() {}
	
	@Override
	public HttpResponseMessage execute(IoSession session,Object message) throws IOException {		
		
		JSONObject jsonObject = null;
		String jsonData = "";
		
		String requestInterface = ((HttpRequestMessage)message).getContext();
		String requestBody = ((HttpRequestMessage)message).getBody();
		JSONObject requestBodyJson = null;
		try{
			requestBodyJson = JSONObject.fromObject(requestBody);
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
		
		if(requestInterface.equals("queryCassandraByOrg.cas")){
			TestDataChase myTDA = new TestDataChase(requestBodyJson);
			Map<String, Object> list = null;
			try {
				list = myTDA.getTestDataByOrg();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
			jsonObject = JSONObject.fromObject(list);
			jsonData = jsonObject.toString();
		}
		if(requestInterface.equals("queryCassandraByOrgEng.cas")){
			TestDataChaseEng myTDA = new TestDataChaseEng(requestBodyJson);
			Map<String, Object> list = null;
			try {
				list = myTDA.getTestDataByOrg();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
			jsonObject = JSONObject.fromObject(list);
			jsonData = jsonObject.toString();
		}
		
		// wuxiaofeng add begin
				if(requestInterface.equals("intoCSVFromDB.cas")){
					IntoCSVFromDB intoCSVFromDB = new IntoCSVFromDB(requestBodyJson);
					Map<String, Object> list = null;
					try {
						System.out.println("======================");
						list = intoCSVFromDB.searchFromDB();
						System.out.println("======================");
					} catch (Exception e) {
						System.out.println("=========出错了=============");
						e.printStackTrace();
					} 
					jsonObject = JSONObject.fromObject(list);
					jsonData = jsonObject.toString();
				}
				
				if(requestInterface.equals("intoCSVFromDBByTestType.cas")){
					IntoCSVFromDBByTestType intoCSVFromDBByTestType = new IntoCSVFromDBByTestType(requestBodyJson);
					Map<String, Object> list = null;
					try {
						System.out.println("======================");
						list = intoCSVFromDBByTestType.searchFromDB();
						System.out.println("======================");
					} catch (Exception e) {
						System.out.println("=========出错了=============");
						e.printStackTrace();
					} 
					jsonObject = JSONObject.fromObject(list);
					jsonData = jsonObject.toString();
				}
				// wuxiaofeng add end
		
		HttpResponseMessage response = new HttpResponseMessage();
		response.setResponseCode(HttpResponseMessage.HTTP_STATUS_SUCCESS);
		response.appendBody(jsonData);
		return response;
	}

}
