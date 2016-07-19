package com.qualitymap.action;

import javax.annotation.Resource;

import com.qualitymap.base.BaseAction;
import com.qualitymap.service.ServicequalityOrgidPingService;

public class ServicequalityOrgidPingAction extends BaseAction {

	@Resource
	ServicequalityOrgidPingService servicequalityOrgidPingService;

	/**
	 * 获取城市list
	 * 
	 * @return void
	 */
	public void getCityList() {
		try {
			String groupid = this.servletRequest.getParameter("groupid");
			String month = this.servletRequest.getParameter("month");
			String broadType = this.servletRequest.getParameter("broadType");//宽带类型
			String tendencyData = servicequalityOrgidPingService.getCityList(month,groupid,broadType);
			printWriter(tendencyData);
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}
	
	/**
	 * 获取报告页面数据明细
	 */
	
	public void getPingReportItem() {
		String month = this.servletRequest.getParameter("month");
		String groupid = this.servletRequest.getParameter("groupid");
		String broadband_type = this.servletRequest.getParameter("broadband_type");

		String datajson = servicequalityOrgidPingService.getPingReportItem(groupid, month, broadband_type);
		printWriter(datajson);
	}
	/**
	 * 根据groupid获取账户数(报告中的地区字段)
	 * 
	 * @return void
	 */
	public void getOrgnumByGroupId() {

		try {
			String month = this.servletRequest.getParameter("month");
			String groupid = this.servletRequest.getParameter("groupid");  //页面获取指定groupid
			String broadType = this.servletRequest.getParameter("broadType");//宽带类型
			String org_num = servicequalityOrgidPingService.getOrgnumByGroupId(month, groupid,broadType);
			printWriter(org_num);
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}
	
}
