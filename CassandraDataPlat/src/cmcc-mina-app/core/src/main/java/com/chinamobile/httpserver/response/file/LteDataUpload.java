package com.chinamobile.httpserver.response.file;

import java.io.IOException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.mina.example.httpserver.codec.HttpRequestMessage;
import org.apache.mina.example.httpserver.codec.HttpResponseMessage;
import org.apache.mina.example.httpserver.uploadfactory.MultiPartProtocal;
import org.apache.mina.example.httpserver.uploadfactory.UploadCodeSet;
import org.apache.mina.example.httpserver.uploadfactory.UploadLogSet;

import cn.speed.history.service.ResultUploadService;
import cn.speed.history.service.UploadFiletInsert;

import com.chinamobile.httpserver.response.CmriUploadResponse;
import com.chinamobile.util.DateParser;


public class LteDataUpload implements CmriUploadResponse{

	private static LteDataUpload instance = null;
	static Logger logger = Logger.getLogger(LteDataUpload.class);
	
	public static LteDataUpload getInstance() {
		if(instance == null){
			return new LteDataUpload();
		}
        return instance;
    }
	
	public LteDataUpload() {}
	
	@Override
	public HttpResponseMessage execute(Object message) throws IOException {
		// TODO Auto-generated method stub
		Map<String,Object> list2insert = new HashMap<String,Object>();
		
		String context = ((HttpRequestMessage)message).getContext().replace(".upload", "").trim();
		if(!UploadCodeSet.contains(context)){
			return null;
		}else{
			list2insert = UploadCodeSet.get(context);
		}
		list2insert.put("apply_time",DateParser.getNowTime("yyyy-MM-dd HH:mm:ss"));
				
		String xmlMessage = ((HttpRequestMessage)message).getBody();
		String output = URLDecoder.decode(xmlMessage, "UTF-8");
//		logger.debug("BODY:  "+output);
//		logger.debug("CONTENT-TYPE:   "+((HttpRequestMessage)message).getHeader("Content-Type")[0]);
//		logger.debug("CONTENT-LENGTH:   "+((HttpRequestMessage)message).getHeader("Content-Length")[0]);

		boolean result = MultiPartProtocal.doParse((HttpRequestMessage)message,list2insert);
		
		try{
			if(result){
				try{
					ResultUploadService service = new ResultUploadService();
					service.insert(list2insert);
				}catch(RuntimeException e){
					e.printStackTrace();
					UploadLogSet.add(list2insert);
				}catch(Exception e1){
					e1.printStackTrace();
					UploadLogSet.add(list2insert);
				}
			}
		}catch(Exception e){
			e.printStackTrace();
		}
		
		try{
			if(result){
//				UploadFiletInsert myUFI = new UploadFiletInsert(list2insert);
//				myUFI.start();
			}
		}catch(Exception e){
			e.printStackTrace();
		}
		
		HttpResponseMessage response = new HttpResponseMessage();
		if(result == true){
			response.setResponseCode(HttpResponseMessage.HTTP_STATUS_SUCCESS);
			response.appendBody("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
			response.appendBody("< UploadFileResponse Status=\"200\">");
			response.appendBody("<Message>Upload Successed!</Message>");
			response.appendBody("</UploadFileResponse>");
		}else{
			response.setResponseCode(HttpResponseMessage.HTTP_STATUS_NOT_FOUND);
			response.appendBody("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
			response.appendBody("< UploadFileResponse Status=\"404\">");
			response.appendBody("<Message>Upload Failed! Illegal Post Contents!</Message>");
			response.appendBody("</UploadFileResponse>");
		}
		
		UploadCodeSet.remove(context);
		
		return response;
	}
	
}
