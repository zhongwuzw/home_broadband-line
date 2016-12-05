package com.chinamobile.httpserver.response.xml;

import java.io.IOException;
import java.net.URLDecoder;

import net.sf.json.JSONObject;

import org.apache.mina.example.httpserver.codec.HttpRequestMessage;
import org.apache.mina.example.httpserver.codec.HttpResponseMessage;

import com.chinamobile.httpserver.data.OttMessageQueue;
import com.chinamobile.httpserver.response.CmriHttpResponse;

public class GetOttData implements CmriHttpResponse{

	private static GetOttData instance = null;
	
	public static GetOttData getInstance() {
		if(instance == null){
			return new GetOttData();
		}
        return instance;
    }
	
	public GetOttData() {}
	
	@Override
	public HttpResponseMessage execute(Object message) throws IOException {
		String callbackparam = ((HttpRequestMessage)message).getParameter("callback");
		
//		String context = ((HttpRequestMessage)message).getContext().trim();
//		String urlInfo = URLDecoder.decode(context, "UTF-8");
//		String[] params = null;
//		if(urlInfo != null){
//			String[] urlParser = urlInfo.split("?");
//			if(urlParser!=null && urlParser.length!=0){
//				String postParam = urlParser[urlParser.length-1];
//				if(postParam!=null){
//					params = postParam.split("&");
//				}
//			}
//			
//		}
//		
//		System.out.println("context is:"+urlInfo);

		HttpResponseMessage response = new HttpResponseMessage();
		response.setResponseCode(HttpResponseMessage.HTTP_STATUS_SUCCESS);
		
		byte[] messageData = OttMessageQueue.getInstance().poll("gt-9100");
		
		JSONObject jsonData = new JSONObject();  
		
		response.appendBody(callbackparam !=null?(callbackparam+"("):"");
		if(messageData!=null){
			jsonData.put("ottdata",new String(messageData)); 
			response.appendBody(jsonData.toString());
		}else{
			jsonData.put("ottdata","0.000000, TCP, 192.168.18.231, 8484, 218.206.179.20, 60098, NONE\n");   
			response.appendBody(jsonData.toString());
		}

		response.appendBody(callbackparam !=null?(callbackparam+")"):"");
		return response;
	}

}
