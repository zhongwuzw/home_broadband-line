package com.chinamobile.datacollector.v2.ott;

import java.io.IOException;

import javax.jms.MapMessage;

import net.sf.json.JSONObject;

import org.apache.mina.example.httpserver.codec.HttpRequestMessage;
import org.apache.mina.example.httpserver.codec.HttpResponseMessage;

import com.chinamobile.activemq.queue.ActiveQueue;
import com.chinamobile.activemq.queue.QueueList;
import com.chinamobile.httpserver.response.CmriHttpResponse;

public class GetOttDataV2 implements CmriHttpResponse {

	private static GetOttDataV2 instance = null;

	public static GetOttDataV2 getInstance() {
		if (instance == null) {
			return new GetOttDataV2();
		}
		return instance;
	}

	public GetOttDataV2() {
	}

	@Override
	public HttpResponseMessage execute(Object message) throws IOException {
		int responseCode = HttpResponseMessage.HTTP_STATUS_SUCCESS;
		JSONObject jsonData = new JSONObject();
		jsonData.put("stutus", 1);
		jsonData.put("message", "操作失败");
		
		String callbackparam = ((HttpRequestMessage) message)
				.getParameter("callback");
		String code = ((HttpRequestMessage) message).getParameter("code");
		String messageData = "";
		HttpResponseMessage response = new HttpResponseMessage();
		try {
			ActiveQueue aq = QueueList.getInstance().get(code);

			MapMessage messageContent = (MapMessage) aq.recive();

			if(messageContent==null){
				jsonData.put("stutus", 0);
				jsonData.put("message", "队列无消息，返回默认值");
			}else{
				messageData = messageContent.getString("value");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		response.setResponseCode(responseCode);
		jsonData.put("stutus", 0);
		jsonData.put("message", "操作成功");
		response.appendBody(callbackparam != null ? (callbackparam + "(") : "");
		if (messageData != null && !"".equals(messageData)) {
			jsonData.put("ottdata", messageData);
			response.appendBody(jsonData.toString());
		} else {
			jsonData.put("ottdata",
					"0.000000, TCP, 192.168.18.231, 8484, 218.206.179.20, 60098, NONE\n");
			response.appendBody(jsonData.toString());
		}

		response.appendBody(callbackparam != null ? (callbackparam + ")") : "");
		return response;
	}

}
