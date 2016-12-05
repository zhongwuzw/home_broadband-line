package com.chinamobile.httpserver.response.xml;

import java.io.IOException;
import java.net.URLDecoder;

import org.apache.mina.example.httpserver.codec.HttpRequestMessage;
import org.apache.mina.example.httpserver.codec.HttpResponseMessage;

import com.chinamobile.httpserver.data.MessageQueue;
import com.chinamobile.httpserver.response.CmriHttpResponse;

public class UeJsonMessage implements CmriHttpResponse{

	private static UeJsonMessage instance = null;
	
	public static UeJsonMessage getInstance() {
		if(instance == null){
			return new UeJsonMessage();
		}
        return instance;
    }
	
	public UeJsonMessage() {}
	
	@Override
	public HttpResponseMessage execute(Object message) throws IOException {
		String xmlMessage = ((HttpRequestMessage)message).getBody();
//		System.out.println("NoDecode is:"+xmlMessage);
		String output = URLDecoder.decode(xmlMessage, "UTF-8");
		System.out.println("PostBody is:"+output);

		MessageQueue.getInstance().offer(output,"ue");
		
		HttpResponseMessage response = new HttpResponseMessage();
		response.setResponseCode(HttpResponseMessage.HTTP_STATUS_SUCCESS);
//		response.appendBody("LiuGang: "+output);
		return response;
	}

}
