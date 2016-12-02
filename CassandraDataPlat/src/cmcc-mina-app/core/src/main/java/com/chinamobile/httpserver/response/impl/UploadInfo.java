package com.chinamobile.httpserver.response.impl;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.mina.example.httpserver.codec.HttpResponseMessage;

import com.chinamobile.httpserver.response.CmriHttpResponse;


public class UploadInfo implements CmriHttpResponse{

	private static UploadInfo instance = null;
	
	public static UploadInfo getInstance() {
		if(instance == null){
			return new UploadInfo();
		}
        return instance;
    }
	
	public UploadInfo() {}
	
	@Override
	public HttpResponseMessage execute(Object message) throws IOException {
		// TODO Auto-generated method stub
		HttpResponseMessage response = new HttpResponseMessage();
		response.setResponseCode(HttpResponseMessage.HTTP_STATUS_SUCCESS);
			
		try {
            String line = null;
//            BufferedReader reader = new BufferedReader(new FileReader("res/html/index.html"));
            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream("res/html/index.html"),"UTF-8"));
            while ((line = reader.readLine()) != null) {
            	response.appendBody(line+"\n");
            }
            reader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
		/*
		 * 
		 * response.appendBody("<!DOCTYPE html>\n");
		response.appendBody("<html>\n");
		response.appendBody("<head>\n");
		response.appendBody("<meta http-equiv=\"Content-Type\" content=\"text/html; charset=gb2312\" />\n");
		response.appendBody("<title>在标注上打开信息窗</title>\n");
		response.appendBody("<script type=\"text/javascript\" src=\"http://api.map.baidu.com/api?v=1.3\"></script>\n");
		
		response.appendBody("<script type=\"text/javascript\" src=\"http://code.jquery.com/jquery-1.7.2.min.js\"></script>\n");
		
		response.appendBody("</head>\n");
		
		response.appendBody("<body>\n");
		response.appendBody("<div style=\"width:520px;height:340px;border:1px solid gray\" id=\"container\"></div>\n");
		response.appendBody("</body>\n");
				
		response.appendBody("</html>\n");
		response.appendBody("<script type=\"text/javascript\" src=\"../map.js\"></script>\n");
		
		 */
	  
		return response;
	}
	
}
