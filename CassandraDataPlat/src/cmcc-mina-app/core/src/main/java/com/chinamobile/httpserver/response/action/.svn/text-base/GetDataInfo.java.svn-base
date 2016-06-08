package com.chinamobile.httpserver.response.action;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.util.Date;
import java.util.List;
import java.util.Map;

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

public class GetDataInfo implements CmriJsonResponse{

	private static GetDataInfo instance = null;
	static Logger logger = Logger.getLogger(GetDataInfo.class);
	
	public static GetDataInfo getInstance() {
		if(instance == null){
			return new GetDataInfo();
		}
        return instance;
    }
	
	public GetDataInfo() {}
	
	@Override
	public HttpResponseMessage execute(IoSession session,Object message) throws IOException {		
		MySelectService service = new MySelectService();
		List<Map<String, Object>> list = service.test();
		JSONArray jsonArray = JSONArray.fromObject(list);
		String jsonData = jsonArray.toString();
		System.out.println(jsonData);
		
		HttpResponseMessage response = new HttpResponseMessage();
		response.setResponseCode(HttpResponseMessage.HTTP_STATUS_SUCCESS);
		response.appendBody(jsonData);
		return response;
	}

}
