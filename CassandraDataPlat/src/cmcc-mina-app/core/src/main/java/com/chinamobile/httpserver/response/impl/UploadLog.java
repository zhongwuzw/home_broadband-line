package com.chinamobile.httpserver.response.impl;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.mina.example.httpserver.codec.HttpResponseMessage;

import com.chinamobile.httpserver.response.CmriHttpResponse;


public class UploadLog implements CmriHttpResponse{

	private static UploadLog instance = null;
	
	public static UploadLog getInstance() {
		if(instance == null){
			return new UploadLog();
		}
        return instance;
    }
	
	public UploadLog() {}
	
	@Override
	public HttpResponseMessage execute(Object message) throws IOException {
		// TODO Auto-generated method stub
		HttpResponseMessage response = new HttpResponseMessage();
		response.setResponseCode(HttpResponseMessage.HTTP_STATUS_SUCCESS);
			
		try {
            String line = null;
//            BufferedReader reader = new BufferedReader(new FileReader("res/html/log.html"));
            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream("res/html/log.html"),"UTF-8"));

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
