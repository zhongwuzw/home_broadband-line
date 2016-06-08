package com.cmri.ctp.service;

import java.io.File;
import java.util.Properties;

import com.cmri.base.utils.LoadProperties;

import net.sf.json.JSONObject;

public class SSOLoginService {

	/**
	 * 单点登录服务业务类
	 */
	
	private static JSONObject json = new JSONObject();
	static {
		Properties properties = LoadProperties.loadProperties("config"+File.separator+"sso.properties");
		json.put("service", properties.getProperty("sso.service"));
		json.put("jump", properties.getProperty("sso.jumpUrl"));
	}
	
	/**
	 * 获取单点登录服务配置
	 */
	public static JSONObject getSSOConfig(){
		return json;
	}
}
