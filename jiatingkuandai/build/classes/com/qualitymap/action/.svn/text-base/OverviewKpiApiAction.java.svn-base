package com.qualitymap.action;

import javax.annotation.Resource;

import com.qualitymap.base.BaseAction;
import com.qualitymap.service.OverviewKpiApiService;

/**
 * 概览信息统计
 * 
 * @author：kxc
 * @date：Apr 12, 2016
 */
public class OverviewKpiApiAction extends BaseAction {

	/**
	 * serialVersionUID long OverviewKpiAction.java
	 */
	private static final long serialVersionUID = 1L;

	@Resource
	private OverviewKpiApiService kpiApiService;

	/**
	 * 
	 * 
	 * 
	 * 获取累计注册用户数及增长百分比
	 * 
	 * @param month
	 * @param groupid
	 * @return
	 * @return String
	 */
	public void getRegusernameNum() {
		try {
			String month = this.servletRequest.getParameter("month");
			//String uuid = this.servletRequest.getParameter("key");
			String userName = this.servletRequest.getParameter("userName");
			//String groupid = this.getUserGroup(uuid);

			String cregusename_num = kpiApiService.getRegusernameNum(month, userName);
			printWriter(cregusename_num);
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}

	/**
	 * 获取累计使用用户数及增长百分比
	 * 
	 * @param month
	 * @param groupid
	 * @return
	 * @return String
	 */
	public void getCustomersNum() {
		try {
			String month = this.servletRequest.getParameter("month");
//			String uuid = this.servletRequest.getParameter("key");
//			String groupid = this.getUserGroup(uuid);
			String userName = this.servletRequest.getParameter("userName");
			String customers_num = kpiApiService.getCustomersNum(month, userName);
			printWriter(customers_num);
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}

	
	
		
	
}
