package com.chinamobile.datacollector.v2.ott;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import net.sf.json.JSONObject;

import org.apache.mina.example.httpserver.codec.HttpRequestMessage;
import org.apache.mina.example.httpserver.codec.HttpResponseMessage;

import com.chinamobile.activemq.queue.ActiveQueue;
import com.chinamobile.activemq.queue.QueueList;
import com.chinamobile.httpserver.response.CmriHttpResponse;

public class PostOttDataV2 implements CmriHttpResponse{

	private static PostOttDataV2 instance = null;
	
	public static PostOttDataV2 getInstance() {
		if(instance == null){
			return new PostOttDataV2();
		}
        return instance;
    }
	
	public PostOttDataV2() {}
	
	@Override
	public HttpResponseMessage execute(Object message) throws IOException {
		int responseCode = HttpResponseMessage.HTTP_STATUS_SERVER_ERROR;
		JSONObject jsonData = new JSONObject();
		jsonData.put("stutus", 1);
		jsonData.put("message", "操作失败");
		
		try{
			String code = "";
			String contents = "";
			String contentsMessage = ((HttpRequestMessage)message).getBody();

			Map<String, Object> map = new HashMap<String, Object> ();
			if(contentsMessage != null){
				JSONObject json = JSONObject.fromObject(contentsMessage);
				code = json.getString("code");
				contents = json.getString("value");
				ActiveQueue aq = QueueList.getInstance().get(code);
				map.put("value", contents);
				aq.send(map);
			}
			responseCode = HttpResponseMessage.HTTP_STATUS_SUCCESS;
			jsonData.put("stutus", 0);
			jsonData.put("message", "操作成功");
		}catch (Exception e){
			e.printStackTrace();
		}
		
		HttpResponseMessage response = new HttpResponseMessage();
		response.setResponseCode(responseCode);
		response.appendBody(jsonData.toString());
		return response;
	}

}

