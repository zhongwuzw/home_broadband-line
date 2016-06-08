package com.chinamobile.httpserver.response.js;

import java.io.IOException;

import org.apache.mina.core.session.IoSession;
import org.apache.mina.example.httpserver.codec.HttpResponseMessage;

import com.chinamobile.httpserver.data.MessageQueue;
import com.chinamobile.httpserver.response.CmriJsonResponse;

public class UeMsgShow implements CmriJsonResponse{
	private static UeMsgShow instance = null;
	
	public static UeMsgShow getInstance() {
		if(instance == null){
			return new UeMsgShow();
		}
        return instance;
    }
	
	public UeMsgShow() {}
	
	@Override
	public HttpResponseMessage execute(IoSession session,Object message) throws IOException {
		HttpResponseMessage response = new HttpResponseMessage();
		response.setResponseCode(HttpResponseMessage.HTTP_STATUS_SUCCESS);
		
		String message_json = "";
		message_json = MessageQueue.getInstance().poll("ue");
   	 	
   	 	response.appendBody(message_json);
   	 	UeMsgDetail.setResponseStr(message_json);
   	 	
		return response;
	}
}
