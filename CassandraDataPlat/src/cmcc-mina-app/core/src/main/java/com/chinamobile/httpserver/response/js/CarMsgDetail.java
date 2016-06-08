package com.chinamobile.httpserver.response.js;

import java.io.IOException;

import org.apache.mina.core.session.IoSession;
import org.apache.mina.example.httpserver.codec.HttpResponseMessage;

import com.chinamobile.httpserver.data.MessageQueue;
import com.chinamobile.httpserver.response.CmriJsonResponse;

public class CarMsgDetail implements CmriJsonResponse{
	private static CarMsgDetail instance = null;
	static String responseStr = "";
	
	static public void setResponseStr(String responseLog) {
		responseStr = responseLog;
	}

	public static CarMsgDetail getInstance() {
		if(instance == null){
			return new CarMsgDetail();
		}
        return instance;
    }
	
	public CarMsgDetail() {}
	
	@Override
	public HttpResponseMessage execute(IoSession session,Object message) throws IOException {
		HttpResponseMessage response = new HttpResponseMessage();
		response.setResponseCode(HttpResponseMessage.HTTP_STATUS_SUCCESS);
		
//		String message_json = "";
//		message_json = MessageQueue.getInstance().peek("car");
   	 	
//   	 	response.appendBody(message_json);
		response.appendBody(responseStr);
   	 	
		return response;
	}
}