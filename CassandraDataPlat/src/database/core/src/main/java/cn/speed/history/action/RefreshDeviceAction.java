package cn.speed.history.action;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.apache.log4j.Logger;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.example.httpserver.codec.HttpRequestMessage;
import org.apache.mina.example.httpserver.codec.HttpResponseMessage;

import cn.speed.history.service.DeviceSelectService;
import cn.speed.history.service.MySelectService;
import cn.speed.jdbc.utils.SearchValue;

import com.chinamobile.httpserver.response.CmriJsonResponse;
import com.chinamobile.util.DateParser;

public class RefreshDeviceAction implements CmriJsonResponse{

	private static RefreshDeviceAction instance = null;
	static Logger logger = Logger.getLogger(RefreshDeviceAction.class);
	
	public static RefreshDeviceAction getInstance() {
		if(instance == null){
			return new RefreshDeviceAction();
		}
        return instance;
    }
	
	public RefreshDeviceAction() {}
	
	@Override
	public HttpResponseMessage execute(IoSession session,Object message) throws IOException {	
		String[] searchString = new String[6];
		String a= ((HttpRequestMessage)message).getParameter("deviceType");
		searchString[0] = ((HttpRequestMessage)message).getParameter("deviceType")!=null?URLDecoder.decode(((HttpRequestMessage)message).getParameter("deviceType"), "UTF-8"):"";
		searchString[1] = ((HttpRequestMessage)message).getParameter("deviceMfr")!=null?URLDecoder.decode(((HttpRequestMessage)message).getParameter("deviceMfr"), "UTF-8"):"";
		searchString[2] = ((HttpRequestMessage)message).getParameter("deviceModel")!=null?URLDecoder.decode(((HttpRequestMessage)message).getParameter("deviceModel"), "UTF-8"):"";
		searchString[3] = ((HttpRequestMessage)message).getParameter("deviceLocation")!=null?URLDecoder.decode(((HttpRequestMessage)message).getParameter("deviceLocation"), "UTF-8"):"";
		searchString[4] = ((HttpRequestMessage)message).getParameter("deviceState")!=null?URLDecoder.decode(((HttpRequestMessage)message).getParameter("deviceState"), "UTF-8"):"";
		searchString[5] = ((HttpRequestMessage)message).getParameter("deviceTestState")!=null?URLDecoder.decode(((HttpRequestMessage)message).getParameter("deviceTestState"), "UTF-8"):"";
		
		SearchValue mySearchV = new SearchValue();
		mySearchV.parse(searchString);
		
		DeviceSelectService service = new DeviceSelectService();
		List<Map<String, Object>> list = service.find(mySearchV);
		
		JSONArray jsonArray = JSONArray.fromObject(list);
		String jsonData = jsonArray.toString();
		System.out.println(jsonData);
		
		HttpResponseMessage response = new HttpResponseMessage();
		response.setResponseCode(HttpResponseMessage.HTTP_STATUS_SUCCESS);
		response.appendBody(jsonData);
		return response;
	}

}
