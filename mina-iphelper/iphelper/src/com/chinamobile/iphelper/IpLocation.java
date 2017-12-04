package com.chinamobile.iphelper;

import org.apache.log4j.Logger;
import org.apache.mina.core.session.IoSession;
public class IpLocation {

	static Logger logger = Logger.getLogger(IpLocation.class);
	static public HttpResponseMessage getIPLoc(String context, IoSession session, Object message) {

//		logger.info("context-----------" + context + "---session:" + session);
		//��������ip�ֶ�

		String ip = ((HttpRequestMessage)message).getParameter("ip");
		long ip2long = IPUtil.ipToLong(ip);
		
		//���ַ�Ѱ������
		int index = DichotomySearch.search_list(Server.startips, ip2long);
		
		String result = Server.locations.get(index);
		//ֱ�ӷ���json
		HttpResponseMessage response = new HttpResponseMessage();
		response.appendBody(result);
		return response;
	}
}
