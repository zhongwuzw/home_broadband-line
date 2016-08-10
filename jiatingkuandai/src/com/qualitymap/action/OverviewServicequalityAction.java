package com.qualitymap.action;

import javax.annotation.Resource;

import com.qualitymap.base.BaseAction;
import com.qualitymap.service.OverviewServicequalityService;

/**
 * Description: OTS统计平台业务质量
 * 
 * @author zqh 2016-4-7: PM 03:25:11
 */
public class OverviewServicequalityAction extends BaseAction {

	@Resource
	private OverviewServicequalityService overviewServicequalityService;

	/**
	 * 根据时间查询本月的测试次数
	 * 
	 * @return void
	 */
	public void getUserIndicator() {

		try {

			String month = this.servletRequest.getParameter("month");
			/*
			 * String uuid = this.servletRequest.getParameter("key"); String
			 * groupid = this.getUserGroup(uuid);
			 */
			String groupid = this.servletRequest.getParameter("groupid");
			String broadband_type = this.servletRequest.getParameter("broadband_type");
			String datajson = overviewServicequalityService.getUserIndicator(groupid, month, broadband_type);
			printWriter(datajson);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 获取地域级别用户指标统计
	 * 
	 * @return void
	 */
	public void getTerritoryData() {

		try {

			String month = this.servletRequest.getParameter("month");
			String groupid = this.servletRequest.getParameter("groupid");
			String broadband_type = this.servletRequest.getParameter("broadband_type");
			String datajson = overviewServicequalityService.getTerritoryData(groupid, month, broadband_type);
			printWriter(datajson);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 分地域指标统计
	 */
	public void getCityData() {

		try {

			String month = this.servletRequest.getParameter("month");
			String groupid = this.servletRequest.getParameter("groupid");
			String broadband_type = this.servletRequest.getParameter("broadband_type");
			String datajson = overviewServicequalityService.getCityData(groupid, month, broadband_type);
			printWriter(datajson);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
