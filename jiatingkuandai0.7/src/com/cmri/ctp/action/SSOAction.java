package com.cmri.ctp.action;

import com.cmri.ctp.service.SSOLoginService;
import com.qualitymap.base.BaseAction;
public class SSOAction extends BaseAction {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	/**
	 * 单点登录
	 */
	
	public String getSSOConfig(){
		try {
			this.servletResponse.getWriter().print(SSOLoginService.getSSOConfig());
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		return null;
	}
	
}
