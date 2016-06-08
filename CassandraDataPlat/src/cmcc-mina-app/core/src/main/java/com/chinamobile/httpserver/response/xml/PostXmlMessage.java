package com.chinamobile.httpserver.response.xml;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URLDecoder;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.example.httpserver.codec.HttpRequestMessage;
import org.apache.mina.example.httpserver.codec.HttpResponseMessage;
import org.apache.mina.example.httpserver.uploadfactory.UploadCodeSet;

import cn.speed.history.service.ResultUploadService;

import com.chinamobile.httpserver.data.DataParser;
import com.chinamobile.httpserver.response.CmriHttpResponse;
import com.chinamobile.httpserver.response.CmriJsonResponse;
import com.chinamobile.util.UpdateApplyRespXML;

public class PostXmlMessage implements CmriJsonResponse{

	private static PostXmlMessage instance = null;
	static Logger logger = Logger.getLogger(PostXmlMessage.class);
	
	public static PostXmlMessage getInstance() {
		if(instance == null){
			return new PostXmlMessage();
		}
        return instance;
    }
	
	public PostXmlMessage() {}
	
	@Override
	public HttpResponseMessage execute(IoSession session,Object message) throws IOException {		
		String xmlMessage = ((HttpRequestMessage)message).getBody();
				
		//logger.debug("未解析：  "+xmlMessage);
		String output = URLDecoder.decode(xmlMessage, "UTF-8");
		logger.debug("解析完成：  "+output);

		Map<String,Object> list2insert = DataParser.getInstance().parseXMLContent(output);
		//logger.debug(list2insert.toString());
		String respBody = UpdateApplyRespXML.BuildXML("200", (String)list2insert.get("store_path_rela"), (String)list2insert.get("update_code"), "Success");
		logger.info(respBody);
		UploadCodeSet.add((String)list2insert.get("update_code"), list2insert);
		HttpResponseMessage response = new HttpResponseMessage();
		response.setResponseCode(HttpResponseMessage.HTTP_STATUS_SUCCESS);
		response.appendBody(respBody);
		return response;
	}

}
