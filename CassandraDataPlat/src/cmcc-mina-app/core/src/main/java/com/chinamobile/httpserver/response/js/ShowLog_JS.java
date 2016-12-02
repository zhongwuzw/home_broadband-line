package com.chinamobile.httpserver.response.js;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.mina.example.httpserver.codec.HttpResponseMessage;

import com.chinamobile.httpserver.response.CmriHttpResponse;

public class ShowLog_JS implements CmriHttpResponse{

	private static ShowLog_JS instance = null;
	
	public static ShowLog_JS getInstance() {
		if(instance == null){
			return new ShowLog_JS();
		}
        return instance;
    }
	
	public ShowLog_JS() {}
	
	@Override
	public HttpResponseMessage execute(Object message) throws IOException {
		HttpResponseMessage response = new HttpResponseMessage();
		response.setResponseCode(HttpResponseMessage.HTTP_STATUS_SUCCESS);
		
		try {
            String line = null;
//            BufferedReader reader = new BufferedReader(new FileReader("res/js/showlog.js"));
            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream("res/js/showlog.js"),"UTF-8"));
            
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
