package com.cmri.ctp.action;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.http.HttpSessionEvent;

import redis.JedisCilent;

import com.cmri.base.bean.SessionUser;
import com.qualitymap.base.BaseAction;

public class SysuserAction extends BaseAction  {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	static int pos;
	static List<String> historicalData;// 历史数据
	private String jsonParam = null;
	private String username = null;
	private String password = null;
	private Integer orgId = null;
	private String orgName = null;
	private String newpassword = null;
	private boolean forceClose;
	private String wholeOrgId;
	static {
		pos = 0;
		historicalData = new ArrayList<String>();
	}


	/**
	 * @return the wholeOrgId
	 */
	public String getWholeOrgId() {
		return wholeOrgId;
	}

	/**
	 * @param wholeOrgId
	 *            the wholeOrgId to set
	 */
	public void setWholeOrgId(String wholeOrgId) {
		this.wholeOrgId = wholeOrgId;
	}

	public static List<SessionUser> existsList = new ArrayList<SessionUser>();

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public boolean isForceClose() {
		return forceClose;
	}

	public void setForceClose(boolean forceClose) {
		this.forceClose = forceClose;
	}
	
	public void sessionCreated(HttpSessionEvent arg0) {
//		logger.debug("sessioncreate:-------------------");
	}

	public Integer getOrgId() {
		return orgId;
	}

	public void setOrgId(Integer orgId) {
		this.orgId = orgId;
	}

	public String getOrgName() {
		return orgName;
	}

	public void setOrgName(String orgName) {
		this.orgName = orgName;
	}

	public String getNewpassword() {
		return newpassword;
	}

	public void setNewpassword(String newpassword) {
		this.newpassword = newpassword;
	}

	public void sessionDestroyed(HttpSessionEvent arg0) {
//		logger.debug("sessionDestroyed:-------------------");
		// 取得不活动时的sessionId,并根据其删除相应logonAccounts中的用户
		String sessionId = arg0.getSession().getId();
		for (int i = 0; i < existsList.size(); i++) {
			SessionUser user = (SessionUser) existsList.get(i);
			if (sessionId.equals(user.getSessionId())) {
				existsList.remove(user);
			}
		}
	}

	/*public void getSessionId() {//获取sessionId
		super.getSessionId();
	}*/
	
	/**
	 * 登录验证
	 */
	public void validLogin() throws IOException {
		super.validLogin();
	}
	/**
	 * 退出系统
	 * @throws IOException
	 */
	public void logout() throws IOException {
		SessionUser user = returnSessionUser();
		for (int i = 0; i < existsList.size(); i++) {
			// findbugs:两个对象用==比较不正确user.getSessionId()==existsList.get(i).getSessionId()
			if (user.getSessionId().equals(existsList.get(i).getSessionId())) {
				existsList.remove(i);
				break;
			}
		}
		String uuid =session.get("key").toString();
		try {
			JedisCilent.delObj(uuid);
		} finally {
			
		}
		
		session.put("key", null);
		session.put("userInfo", null);
	}


 }
