package com.qualitymap.action;

import javax.annotation.Resource;

import com.qualitymap.base.BaseAction;
import com.qualitymap.service.ServicequalityGroupidHttpDownloadService;
import com.qualitymap.service.ServicequalityperiodService;

public class ServicequalityperiodAction extends BaseAction {

	@Resource
	ServicequalityperiodService servicequalityperiodService;

	
	/**
	 * 获取分时段统计数据
	 * 
	 * @return void
	 */
	public void getDownloadRateOrder() {

		try {
			String broadband_type = this.servletRequest.getParameter("broadband_type");
			String month = this.servletRequest.getParameter("month");
			String uuid = this.servletRequest.getParameter("key");
			String groupid = this.getUserGroup(uuid);
			 //String groupid = "50071,50074,50067";
			String delayOrder = servicequalityperiodService.getperiodData(groupid, month, broadband_type);
			printWriter(delayOrder);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
