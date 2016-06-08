package com.chinamobile.httpserver.response.xml;

import java.io.IOException;
import java.net.URLDecoder;
import java.util.Map;

import net.sf.json.JSONObject;

import org.apache.mina.example.httpserver.codec.HttpRequestMessage;
import org.apache.mina.example.httpserver.codec.HttpResponseMessage;

import com.chinamobile.httpserver.data.DataParser;
import com.chinamobile.httpserver.data.OttMessageQueue;
import com.chinamobile.httpserver.response.CmriHttpResponse;

public class VerifyOttCache implements CmriHttpResponse{

	private static VerifyOttCache instance = null;
	
	public static VerifyOttCache getInstance() {
		if(instance == null){
			return new VerifyOttCache();
		}
        return instance;
    }
	
	public VerifyOttCache() {}
	
	@Override
	public HttpResponseMessage execute(Object message) throws IOException {
		String xmlMessage = ((HttpRequestMessage)message).getBody();
		
		String output = URLDecoder.decode(xmlMessage, "UTF-8");
		//Map<String,Object> list2insert = DataParser.getInstance().parseXMLContent(output);
		
		
		boolean result = false;
		if(output.contains("10086")){
			result = true;
		}
		HttpResponseMessage response = new HttpResponseMessage();
		if(result == true){
			response.setResponseCode(HttpResponseMessage.HTTP_STATUS_SUCCESS);
			response.appendBody("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
			response.appendBody("<VerifyCacheResponse Status=\"200\">");
			response.appendBody("<Message>Verify Successed!</Message>");
			response.appendBody("</VerifyCacheResponse>");
		}else{
			response.setResponseCode(HttpResponseMessage.HTTP_STATUS_NOT_FOUND);
			response.appendBody("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
			response.appendBody("<VerifyCacheResponse Status=\"404\">");
			response.appendBody("<Message>Verify Failed! Illegal Cache ID!</Message>");
			response.appendBody("</VerifyCacheResponse>");
		}
		return response;
	}

}
