package com.chinamobile.httpserver.response.js;

import java.io.IOException;

import org.apache.mina.core.session.IoSession;
import org.apache.mina.example.httpserver.codec.HttpResponseMessage;

import com.chinamobile.httpserver.data.MessageQueue;
import com.chinamobile.httpserver.response.CmriJsonResponse;

public class CarMsgShow implements CmriJsonResponse{
	private static CarMsgShow instance = null;
	
	public static CarMsgShow getInstance() {
		if(instance == null){
			return new CarMsgShow();
		}
        return instance;
    }
	
	public CarMsgShow() {}
	
	@Override
	public HttpResponseMessage execute(IoSession session,Object message) throws IOException {
		HttpResponseMessage response = new HttpResponseMessage();
		response.setResponseCode(HttpResponseMessage.HTTP_STATUS_SUCCESS);
		
		String message_json = "";
		message_json = MessageQueue.getInstance().poll("car");
		
   	 	response.appendBody(message_json);
   	 	CarMsgDetail.setResponseStr(message_json);
   	 	
		return response;
	}
}