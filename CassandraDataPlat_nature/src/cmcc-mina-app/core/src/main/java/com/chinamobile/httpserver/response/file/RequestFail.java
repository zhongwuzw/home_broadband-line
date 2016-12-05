package com.chinamobile.httpserver.response.file;

import java.io.IOException;

import org.apache.mina.example.httpserver.codec.HttpResponseMessage;

import com.chinamobile.httpserver.response.CmriFileResponse;

public class RequestFail implements CmriFileResponse{

	private static RequestFail instance = null;
	
	public static RequestFail getInstance() {
		if(instance == null){
			return new RequestFail();
		}
        return instance;
    }
	
	private RequestFail() {}
	
	@Override
	public HttpResponseMessage execute(Object message,String fileName) throws IOException {
		HttpResponseMessage response = new HttpResponseMessage();
		response.setResponseCode(HttpResponseMessage.HTTP_STATUS_SUCCESS);
		response.appendBody("<html><body><h1>无法找到您所请求的文件，请确认地址后重试！</h1></body></html>\n");
		
		return response;
	}

}
