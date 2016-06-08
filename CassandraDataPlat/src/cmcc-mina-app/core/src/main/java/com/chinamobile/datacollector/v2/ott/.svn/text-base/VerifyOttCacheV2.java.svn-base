package com.chinamobile.datacollector.v2.ott;

import java.io.IOException;
import java.net.URLDecoder;
import java.util.Map;

import net.sf.json.JSONObject;

import org.apache.mina.example.httpserver.codec.HttpRequestMessage;
import org.apache.mina.example.httpserver.codec.HttpResponseMessage;

import com.chinamobile.activemq.queue.QueueList;
import com.chinamobile.httpserver.data.DataParser;
import com.chinamobile.httpserver.data.OttMessageQueue;
import com.chinamobile.httpserver.response.CmriHttpResponse;

public class VerifyOttCacheV2 implements CmriHttpResponse{

	private static VerifyOttCacheV2 instance = null;
	
	public static VerifyOttCacheV2 getInstance() {
		if(instance == null){
			return new VerifyOttCacheV2();
		}
        return instance;
    }
	
	public VerifyOttCacheV2() {}
	
	@Override
	public HttpResponseMessage execute(Object message) throws IOException {
		String code = ((HttpRequestMessage) message).getParameter("code");
		
		boolean result = false;
		if(!QueueList.getInstance().contains(code))
			result = true;
		
		HttpResponseMessage response = new HttpResponseMessage();
		if(result == true){
			QueueList.getInstance().get(code);
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
