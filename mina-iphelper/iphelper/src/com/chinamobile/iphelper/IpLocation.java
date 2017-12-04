package com.chinamobile.iphelper;

import org.apache.log4j.Logger;
import org.apache.mina.core.session.IoSession;
public class IpLocation {

	static Logger logger = Logger.getLogger(IpLocation.class);
	static public HttpResponseMessage getIPLoc(String context, IoSession session, Object message) {

//		logger.info("context-----------" + context + "---session:" + session);
		//解析参数ip字段

		String ip = ((HttpRequestMessage)message).getParameter("ip");
		long ip2long = IPUtil.ipToLong(ip);
		
		//二分法寻找索引
		int index = DichotomySearch.search_list(Server.startips, ip2long);
		
		String result = Server.locations.get(index);
		//直接返回json
		HttpResponseMessage response = new HttpResponseMessage();
		response.appendBody(result);
		return response;
	}
}
