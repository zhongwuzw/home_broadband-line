package com.chinamobile.httpserver.response.js;

import java.io.IOException;

import org.apache.mina.core.future.IoFutureListener;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.example.httpserver.codec.HttpResponseMessage;

import com.chinamobile.httpserver.response.CmriJsonResponse;

public class RequestFail  implements CmriJsonResponse{
	private static RequestFail instance = null;
	
	public static RequestFail getInstance() {
		if(instance == null){
			return new RequestFail();
		}
        return instance;
    }
	
	public RequestFail() {}
	
	@Override
	public HttpResponseMessage execute(IoSession session,Object message) throws IOException {
		HttpResponseMessage response = new HttpResponseMessage();
		response.setResponseCode(HttpResponseMessage.HTTP_STATUS_NOT_FOUND);
		return response;
	}

}
