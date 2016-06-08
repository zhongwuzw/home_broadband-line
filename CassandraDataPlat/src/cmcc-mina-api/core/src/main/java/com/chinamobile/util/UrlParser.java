package com.chinamobile.util;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class UrlParser {
	// 获取当天时间
	static public String[] parseParams(String context) {
		Map<String, String> parameters = new HashMap<String, String>();
		String urlInfo = null;
		try {
			urlInfo = URLDecoder.decode(context, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		String[] params = null;
		if(urlInfo != null){
			String[] urlParser = urlInfo.split("?");
			if(urlParser!=null && urlParser.length!=0){
				String postParam = urlParser[urlParser.length-1];
				if(postParam!=null){
					params = postParam.split("&");
				}
			}
		}
		for(int i=0; i<params.length; i++){
			if(params[i]==null || "".equals(params[i])){
				continue;
			}
			String[] keyValue = params[i].split("=");
			
		}
		return params;
	}
}
