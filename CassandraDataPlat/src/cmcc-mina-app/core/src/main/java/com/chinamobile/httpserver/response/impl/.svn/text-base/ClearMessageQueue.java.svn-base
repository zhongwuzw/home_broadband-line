package com.chinamobile.httpserver.response.impl;

import java.io.IOException;

import org.apache.mina.example.httpserver.codec.HttpResponseMessage;

import com.chinamobile.httpserver.data.MessageQueue;
import com.chinamobile.httpserver.response.CmriHttpResponse;

public class ClearMessageQueue implements CmriHttpResponse {

	private static ClearMessageQueue instance = null;

	public static ClearMessageQueue getInstance() {
		if (instance == null) {
			return new ClearMessageQueue();
		}
		return instance;
	}

	public ClearMessageQueue() {
	}

	@Override
	public HttpResponseMessage execute(Object message) throws IOException {
		HttpResponseMessage response = new HttpResponseMessage();
		boolean isOK = MessageQueue.getInstance().removeAll();
		if(isOK){
			response.setResponseCode(HttpResponseMessage.HTTP_STATUS_SUCCESS);
		}else{
			boolean isOK2 = MessageQueue.getInstance().removeAll();
			if(isOK2){
				response.setResponseCode(HttpResponseMessage.HTTP_STATUS_SUCCESS);
			}else{
				response.setResponseCode(HttpResponseMessage.HTTP_STATUS_NOT_FOUND);
			}
		}
		return response;
	}

}
