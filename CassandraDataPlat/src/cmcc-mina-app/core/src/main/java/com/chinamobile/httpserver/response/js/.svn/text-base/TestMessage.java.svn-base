package com.chinamobile.httpserver.response.js;

import java.io.IOException;
import java.util.ArrayList;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.apache.mina.core.future.IoFutureListener;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.example.httpserver.codec.HttpResponseMessage;

import com.chinamobile.httpserver.data.MessageQueue;
import com.chinamobile.httpserver.data.TestMessageBean;
import com.chinamobile.httpserver.response.CmriJsonResponse;

public class TestMessage  implements CmriJsonResponse{
	private static TestMessage instance = null;
	
	public static TestMessage getInstance() {
		if(instance == null){
			return new TestMessage();
		}
        return instance;
    }
	
	public TestMessage() {}
	
	@Override
	public HttpResponseMessage execute(IoSession session,Object message) throws IOException {
		HttpResponseMessage response = new HttpResponseMessage();
		response.setResponseCode(HttpResponseMessage.HTTP_STATUS_SUCCESS);
				
		ArrayList<TestMessageBean> mybean = TestMessageBean.getAllInstance();
		JSONArray job = JSONArray.fromObject(mybean);
   	 	System.out.println(job.toString());
   	 	
   	 	response.appendBody(job.toString());
   	 	
		return response;
	}
}
