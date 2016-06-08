package com.chinamobile.httpserver.response.xml;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.apache.mina.example.httpserver.codec.HttpRequestMessage;
import org.apache.mina.example.httpserver.codec.HttpResponseMessage;

import com.chinamobile.httpserver.data.DataParser;
import com.chinamobile.httpserver.response.CmriHttpResponse;

public class PostXml implements CmriHttpResponse{

	private static PostXml instance = null;
	
	public static PostXml getInstance() {
		if(instance == null){
			return new PostXml();
		}
        return instance;
    }
	
	public PostXml() {}
	
	@Override
	public HttpResponseMessage execute(Object message) throws IOException {		
		HttpResponseMessage response = new HttpResponseMessage();
		response.setResponseCode(HttpResponseMessage.HTTP_STATUS_SUCCESS);
		response.setContentType("text/xml; charset=utf-8");
		try {
            String line = null;
            BufferedReader reader = new BufferedReader(new FileReader("D:/Develop/MINA_Workspace/HttpClientDemo/res/messagemodule.xml"));
            while ((line = reader.readLine()) != null) {
            	response.appendBody(line+"\n");
            }
            reader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
		return response;
	}

}

