package com.chinamobile.httpserver.response.file;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.mina.example.httpserver.codec.HttpResponseMessage;

import com.chinamobile.httpserver.response.CmriFileResponse;

public class RequestJs  implements CmriFileResponse{

	private static RequestJs instance = null;
	private static String default_root_dict = "res/js";
	
	public static RequestJs getInstance() {
		if(instance == null){
			return new RequestJs();
		}
        return instance;
    }
	
	public RequestJs() {}
	
	@Override
	public HttpResponseMessage execute(Object message,String fileName) throws IOException {		
		HttpResponseMessage response = new HttpResponseMessage();
		response.setResponseCode(HttpResponseMessage.HTTP_STATUS_SUCCESS);
		response.setContentType("text/javascript");	
		try {
			File jsFile = new File(default_root_dict+System.getProperty("file.separator")+fileName);
			if(jsFile.exists()){
				String line = null;
				BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(jsFile),"UTF-8"));
	            while ((line = reader.readLine()) != null) {
	            	response.appendBody(line+"\n");
	            }
	            reader.close();
			}else{
				response.setResponseCode(HttpResponseMessage.HTTP_STATUS_NOT_FOUND);
				response.appendBody("File is not exist!"+"\n");
			}
        } catch (Exception e) {
            e.printStackTrace();
        }
		
		return response;
	}
}
