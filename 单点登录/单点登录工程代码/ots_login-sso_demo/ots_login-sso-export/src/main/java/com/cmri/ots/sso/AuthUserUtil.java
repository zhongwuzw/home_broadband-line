package com.cmri.ots.sso;


import java.net.URI;
import java.net.URLEncoder;

import com.cmri.ots.sso.util.SSOUtil;

public class AuthUserUtil {
	public static String uri = SSOUtil.geturl();
	
	public static  String publicLogin(String username,String pwd,String referer,String UserAgent,String RemoteAddr) throws Exception{
		referer = URLEncoder.encode(referer,"UTF-8");
		UserAgent = URLEncoder.encode(UserAgent,"UTF-8");
		RemoteAddr = URLEncoder.encode(RemoteAddr,"UTF-8");
		String url = uri+"action/publicLogin.do?username="+username+"&password="+pwd+"&referer="+referer+"&UserAgent="+UserAgent+"&RemoteAddr="+RemoteAddr;
		return SSOUtil.sendPost(url,"");
	}
	public static  String validLogin(String key){
		String url = uri+"action/validLogin.do?key="+key;
		return SSOUtil.sendPost(url,"");
	}
	public static String getOrgInfo(String key){
		String url = uri+"action/getOrgInfo.do?key="+key;
		return SSOUtil.sendPost(url,"");
	}
	public static String getInfo(String key){
		String url = uri+"action/getInfo.do?key="+key;
		return SSOUtil.sendPost(url,"");
	}
	public static String getOrgInfotwo(String key){
		String url = uri+"action/getOrgInfotwo.do?key="+key;
		return SSOUtil.sendPost(url,"");
	}
	public static String modifyPassword(String key,String jsonParam){
		String url = uri+"action/modifyPassword.do?key="+key+"&jsonParam="+jsonParam;
		return SSOUtil.sendPost(url,"");
	}
	public static String changeOrg(String key,String jsonParam){
		String url = uri+"action/changeOrg.do?key="+key+"&jsonParam="+jsonParam;
		return SSOUtil.sendPost(url,"");
	}
	public static String logoutAction(String key){
		String url = uri+"action/logoutAction.do?key="+key;
		return SSOUtil.sendPost(url,"");
	}
}
