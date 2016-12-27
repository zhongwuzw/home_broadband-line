package com.cmri.ots.sso;


import java.io.UnsupportedEncodingException;

import com.cmri.base.bean.SessionUser;


public interface AuthService {
	
	public String publicLogin(String username,String pwd,String referer,String userAgentstr,String RemoteAddr);
	
	public String validLogin(String key);
	
	public String getOrgInfo(String key);

	public String getUserInfo(String apikey);

	public String queryByUserGroup(String key);

	public String getInfo(String key);

	public String modifyPassword(String jsonParam) throws UnsupportedEncodingException;

	public void changeOrg(String jsonParam, String key);

	public void logout(String key);
	
}
