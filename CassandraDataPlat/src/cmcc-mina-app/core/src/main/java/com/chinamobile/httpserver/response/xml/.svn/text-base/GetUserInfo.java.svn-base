package com.chinamobile.httpserver.response.xml;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.util.Date;

import net.sf.json.JSONObject;

import org.apache.log4j.Logger;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.example.httpserver.codec.HttpRequestMessage;
import org.apache.mina.example.httpserver.codec.HttpResponseMessage;
import org.apache.mina.example.httpserver.uploadfactory.UploadCodeSet;

import com.chinamobile.httpserver.data.DataParser;
import com.chinamobile.httpserver.response.CmriHttpResponse;
import com.chinamobile.httpserver.response.CmriJsonResponse;
import com.chinamobile.util.UpdateApplyRespXML;

public class GetUserInfo implements CmriJsonResponse{

	private static GetUserInfo instance = null;
	static Logger logger = Logger.getLogger(GetUserInfo.class);
	
	public static GetUserInfo getInstance() {
		if(instance == null){
			return new GetUserInfo();
		}
        return instance;
    }
	
	public GetUserInfo() {}
	
	@Override
	public HttpResponseMessage execute(IoSession session,Object message) throws IOException {		
		JSONObject userInfo = new JSONObject();
		userInfo.element("IP", ((InetSocketAddress)session.getRemoteAddress()).getAddress().getHostAddress());
		userInfo.element("PORT", Integer.toString(((InetSocketAddress)session.getRemoteAddress()).getPort()));
		userInfo.element("HOSTNAME", ((InetSocketAddress)session.getRemoteAddress()).getAddress().getHostName());
		HttpResponseMessage response = new HttpResponseMessage();
		response.setResponseCode(HttpResponseMessage.HTTP_STATUS_SUCCESS);
		response.appendBody(userInfo.toString());
		return response;
	}

}
