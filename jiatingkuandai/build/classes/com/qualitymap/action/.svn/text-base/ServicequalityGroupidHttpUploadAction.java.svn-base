package com.qualitymap.action;

import javax.annotation.Resource;

import com.qualitymap.base.BaseAction;
import com.qualitymap.service.ServicequalityGroupidHttpUploadService;

public class ServicequalityGroupidHttpUploadAction extends BaseAction {

	@Resource
	ServicequalityGroupidHttpUploadService groupidHttpUploadService;

	
	/**
	 * 获取平均时延上下月数据
	 */
	public void getHttpUploadRate() {

		try {
			String yearMonth = this.servletRequest.getParameter("month");
			String uuid = this.servletRequest.getParameter("key");
			String groupid = this.getUserGroup(uuid);
			//String groupid = "50091,50067";
			String avgdata = groupidHttpUploadService.getHttpUploadRate(yearMonth, groupid);
			printWriter(avgdata);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
	

	/**
	 * 获取上传速率趋势数据
	 */
	public void getUploadRateData() {

		try {
			String probetype = this.servletRequest.getParameter("probetype");
			String uuid = this.servletRequest.getParameter("key");
			String groupid = this.getUserGroup(uuid);
			 //String groupid = "50071,50074,50067";
			String avgdelayData = groupidHttpUploadService.getUploadRateData(groupid, probetype);
			printWriter(avgdelayData);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 获取上传速率的排名
	 * 
	 * @return void
	 */
	public void getUploadRateOrder() {

		try {
			String probetype = this.servletRequest.getParameter("probetype");
			String month = this.servletRequest.getParameter("month");
			String uuid = this.servletRequest.getParameter("key");
			String groupid = this.getUserGroup(uuid);
			 //String groupid = "50071,50074,50067";
			String delayOrder = groupidHttpUploadService.getUploadRateOrder(month, groupid, probetype);
			printWriter(delayOrder);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
