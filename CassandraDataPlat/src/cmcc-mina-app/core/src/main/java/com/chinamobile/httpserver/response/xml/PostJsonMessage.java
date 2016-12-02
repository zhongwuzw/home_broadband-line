package com.chinamobile.httpserver.response.xml;

import java.io.IOException;
import java.net.URLDecoder;

import net.sf.json.JSONObject;

import org.apache.mina.example.httpserver.codec.HttpRequestMessage;
import org.apache.mina.example.httpserver.codec.HttpResponseMessage;

import com.chinamobile.httpserver.data.MessageQueue;
import com.chinamobile.httpserver.data.TestMessageBean;
import com.chinamobile.httpserver.response.CmriHttpResponse;

public class PostJsonMessage implements CmriHttpResponse{

	private static PostJsonMessage instance = null;
	
	public static PostJsonMessage getInstance() {
		if(instance == null){
			return new PostJsonMessage();
		}
        return instance;
    }
	
	public PostJsonMessage() {}
	
	@Override
	public HttpResponseMessage execute(Object message) throws IOException {
		String xmlMessage = ((HttpRequestMessage)message).getParameter("postmessage");
		String output = URLDecoder.decode(xmlMessage, "UTF-8");
		System.out.println("解码后的："+output);
		
		JSONObject job = JSONObject.fromObject(output);
		int uid = job.getInt("uid");
		float lng = Float.parseFloat(job.getString("lng"));
		float lat = Float.parseFloat(job.getString("lat"));
		String contents = job.getString("contents");
		
		TestMessageBean testMessage = TestMessageBean.getInstance(uid);
		testMessage.setInstance(uid, lng, lat, contents);
		System.out.println("解析成功："+testMessage.toString());
		testMessage.writetofile(testMessage.toString());
		
		HttpResponseMessage response = new HttpResponseMessage();
		response.setResponseCode(HttpResponseMessage.HTTP_STATUS_SUCCESS);
		response.appendBody("received: "+output);
		return response;
	}

}

