package com.qualitymap.action;

import javax.annotation.Resource;

import com.qualitymap.base.BaseAction;
import com.qualitymap.service.ServicequalityGroupidHttpDownloadService;

public class ServicequalityGroupidHttpDownloadAction extends BaseAction {

	@Resource
	ServicequalityGroupidHttpDownloadService groupidHttpDownloadService;

	/**
	 * 获取平均时延上下月数据
	 */
	public void getHttpDownloadRate() {

		try {
			String yearMonth = this.servletRequest.getParameter("month");
			String uuid = this.servletRequest.getParameter("key");
			String groupid = this.getUserGroup(uuid);
			// String groupid = "50091,50067";
			String avgdata = groupidHttpDownloadService.getHttpDownloadRate(yearMonth, groupid);
			printWriter(avgdata);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
	
	/**
	 * 获取下载速率趋势数据
	 */
	public void getDownloadRateData() {

		try {
			String probetype = this.servletRequest.getParameter("probetype");
			String uuid = this.servletRequest.getParameter("key");
			String groupid = this.getUserGroup(uuid);
			 //String groupid = "50071,50074,50067";
			String avgdelayData = groupidHttpDownloadService.getDownloadRateData(groupid, probetype);
			printWriter(avgdelayData);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 获取下载速率的排名
	 * 
	 * @return void
	 */
	public void getDownloadRateOrder() {

		try {
			String probetype = this.servletRequest.getParameter("probetype");
			String month = this.servletRequest.getParameter("month");
			String uuid = this.servletRequest.getParameter("key");
			String groupid = this.getUserGroup(uuid);
			// String groupid = "50071,50074,50067";
			String delayOrder = groupidHttpDownloadService.getDownloadRateOrder(month, groupid, probetype);
			printWriter(delayOrder);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
