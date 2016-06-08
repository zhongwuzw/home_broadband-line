package com.qualitymap.action;

import javax.annotation.Resource;

import com.qualitymap.base.BaseAction;
import com.qualitymap.service.ServicequalityOrgidPingService;
import com.qualitymap.service.ServicequalityOrgidWebbrowsingService;

public class ServicequalityOrgidWebbrowsingAction extends BaseAction {

	@Resource
	ServicequalityOrgidWebbrowsingService orgidWebbrowsingService;

	/**
	 * 获取宽带品质监测报告详情
	 */
	
	public void getPingReportItem() {
		String month = this.servletRequest.getParameter("month");
		String groupid = this.servletRequest.getParameter("groupid");
		String broadband_type = this.servletRequest.getParameter("broadband_type");

		String datajson = orgidWebbrowsingService.getPingReportItem(groupid, month, broadband_type);
		printWriter(datajson);
	}
	
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
			String tendencyData = orgidWebbrowsingService.getCityList(month,groupid,broadType);
			printWriter(tendencyData);
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}
	/**
	 * 根据groupid获取有效样本
	 * 
	 * @return void
	 */
	public void getValidSampleNum() {
		try {
			String month = this.servletRequest.getParameter("month");
			String groupid = this.servletRequest.getParameter("groupid");  //页面获取指定groupid
			String broadType = this.servletRequest.getParameter("broadType");
			String page_test_times = orgidWebbrowsingService.getValidSampleNum(month, groupid,broadType);
			printWriter(page_test_times);
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
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
			String org_num = orgidWebbrowsingService.getOrgnumByGroupId(month, groupid,broadType);
			printWriter(org_num);
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}
	
}
