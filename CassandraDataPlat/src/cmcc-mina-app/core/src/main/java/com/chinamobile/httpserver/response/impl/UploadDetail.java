package com.chinamobile.httpserver.response.impl;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.mina.example.httpserver.codec.HttpResponseMessage;

import com.chinamobile.httpserver.response.CmriHttpResponse;


public class UploadDetail implements CmriHttpResponse{

	private static UploadDetail instance = null;
	
	public static UploadDetail getInstance() {
		if(instance == null){
			return new UploadDetail();
		}
        return instance;
    }
	
	public UploadDetail() {}
	
	@Override
	public HttpResponseMessage execute(Object message) throws IOException {
		// TODO Auto-generated method stub
		HttpResponseMessage response = new HttpResponseMessage();
		response.setResponseCode(HttpResponseMessage.HTTP_STATUS_SUCCESS);
		
		try {
            String line = null;
//            BufferedReader reader = new BufferedReader(new FileReader("res/html/uedetail.html"));
            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream("res/html/uedetail.html"),"UTF-8"));
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
