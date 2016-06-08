package com.chinamobile.httpserver.response.xml;

import java.io.IOException;

import org.apache.mina.example.httpserver.codec.HttpRequestMessage;
import org.apache.mina.example.httpserver.codec.HttpResponseMessage;

import com.chinamobile.httpserver.data.OttMessageQueue;
import com.chinamobile.httpserver.response.CmriHttpResponse;

public class PostOttData implements CmriHttpResponse{

	private static PostOttData instance = null;
	
	public static PostOttData getInstance() {
		if(instance == null){
			return new PostOttData();
		}
        return instance;
    }
	
	public PostOttData() {}
	
	@Override
	public HttpResponseMessage execute(Object message) throws IOException {
		String context = ((HttpRequestMessage)message).getContext().trim();
		System.out.println("context is:"+context);
		
		byte[] contentsMessage = ((HttpRequestMessage)message).getBodybytes();

		if(contentsMessage != null){
			OttMessageQueue.getInstance().offer(contentsMessage,"gt-9100");
		}
		
		HttpResponseMessage response = new HttpResponseMessage();
		response.setResponseCode(HttpResponseMessage.HTTP_STATUS_SUCCESS);
//		response.appendBody("LiuGang: "+output);
		return response;
	}

}
