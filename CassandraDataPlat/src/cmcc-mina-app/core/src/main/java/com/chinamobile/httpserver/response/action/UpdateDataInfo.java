package com.chinamobile.httpserver.response.action;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.List;
import net.sf.json.JSONObject;

import org.apache.log4j.Logger;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.example.httpserver.codec.HttpRequestMessage;
import org.apache.mina.example.httpserver.codec.HttpResponseMessage;

import cn.speed.history.service.MySelectService;

import com.chinamobile.httpserver.response.CmriJsonResponse;
import com.chinamobile.util.DateParser;

public class UpdateDataInfo implements CmriJsonResponse{

	private static UpdateDataInfo instance = null;
	static Logger logger = Logger.getLogger(UpdateDataInfo.class);
	
	public static UpdateDataInfo getInstance() {
		if(instance == null){
			return new UpdateDataInfo();
		}
        return instance;
    }
	
	public UpdateDataInfo() {}
	
	@Override
	public HttpResponseMessage execute(IoSession session,Object message) throws IOException {		
		String jsonMessage = ((HttpRequestMessage)message).getBody();
		String output = URLDecoder.decode(jsonMessage, "UTF-8");
		System.out.println("PostBody is:"+output);
		
		JSONObject job = JSONObject.fromObject(output);
		String prottype = job.getString("prottype");
		String upthread = job.getString("upthread");
		String downthread = job.getString("downthread");
		String upspeed = job.getString("upspeed");
		String downspeed = job.getString("downspeed");
		String upspeedavg = job.getString("upspeedavg");
		String downspeedavg = job.getString("downspeedavg");
		
		
		MySelectService service = new MySelectService();
		List<String> list2insert = new ArrayList<String>();
		list2insert.add(DateParser.getNowTime("yyyy-MM-dd HH:mm:ss"));
		list2insert.add(((InetSocketAddress)session.getRemoteAddress()).getAddress().getHostAddress());
		list2insert.add(prottype);
		list2insert.add(upthread);
		list2insert.add(downthread);
		list2insert.add(upspeed);
		list2insert.add(downspeed);
		list2insert.add(upspeedavg);
		list2insert.add(downspeedavg);
		service.testUpdate(list2insert);
		
		HttpResponseMessage response = new HttpResponseMessage();
		response.setResponseCode(HttpResponseMessage.HTTP_STATUS_SUCCESS);
		return response;
	}

}
