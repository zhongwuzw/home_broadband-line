package com.chinamobile.httpserver.response.impl;

import java.io.IOException;

import org.apache.mina.example.httpserver.codec.HttpRequestMessage;
import org.apache.mina.example.httpserver.codec.HttpResponseMessage;

import com.chinamobile.httpserver.response.CmriHttpResponse;

public class RequestSuccess implements CmriHttpResponse{

	private static RequestSuccess instance = null;
	
	public static RequestSuccess getInstance() {
		if(instance == null){
			return new RequestSuccess();
		}
        return instance;
    }
	
	public RequestSuccess() {}
	
	@Override
	public HttpResponseMessage execute(Object message) throws IOException {
		HttpResponseMessage response = new HttpResponseMessage();
        response.setResponseCode(HttpResponseMessage.HTTP_STATUS_SUCCESS);
        response.appendBody("<HTML>");
        response.appendBody("<style type=\"text/css\">");
        response.appendBody(".bodytitle{margin-top:50px; margin-bottom:50px; font-size:38px; color:blue;}");
        response.appendBody("body{margin-top:150px; text-align:center; front-size:12px;}");
        response.appendBody("</style>");

        response.appendBody("<HEAD>");
        response.appendBody("<META HTTP-EQUIV=\"Content-Type\" CONTENT=\"text/html; charset=UTF-8\">");
        response.appendBody("<TITLE>测试用例配置</TITLE>");
        response.appendBody("</HEAD>");
        response.appendBody("<BODY>");
        response.appendBody("<FORM ACTION=\"SetAttachAndDetach\" NAME=\"form\" METHOD=\"post\">");
        response.appendBody("<H3>Attach&Detach测试用例参数编辑</H3>");
        response.appendBody("<HR>");
		
        response.appendBody("Attach&Detach次数：<INPUT TYPE=\"text\" NAME=\"numOfAttachTimes\" VALUE=\"123\"> <BR>");
        response.appendBody("<HR>");
        response.appendBody("<H3><INPUT TYPE=\"submit\" NAME=\"submit\" VALUE=\"确定\"> ");
        response.appendBody("<INPUT TYPE=\"submit\" NAME=\"cancel\" VALUE=\"重置\"> </H3>");
		  
        response.appendBody("</FORM>");
        response.appendBody("</BODY>");
        response.appendBody("</HTML>");
        
		return response;
	}

}
