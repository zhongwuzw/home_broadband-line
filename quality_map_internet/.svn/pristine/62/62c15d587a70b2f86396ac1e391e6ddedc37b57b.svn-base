package com.qualitymap.action;

import javax.annotation.Resource;

import com.qualitymap.base.BaseAction;
import com.qualitymap.service.NetworkOperatorService;

/**
 * 网络分析数据统计
 * 
 * @author：kxc
 * @date：Apr 11, 2016
 */
public class NetworkOperatorAction extends BaseAction {

	@Resource
	private NetworkOperatorService operatorService;

	/**
	 * 获取运营商占比
	 * 
	 * @return void
	 */
	public void getOperatorDatas() {

		try {

			String month = this.servletRequest.getParameter("month");
			String uuid = this.servletRequest.getParameter("key");
			String groupid = this.getUserGroup(uuid);

			String operatorData = operatorService.getOperatorData(month, groupid);
			printWriter(operatorData);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 获取运营商数据明细信息
	 * 
	 * @return void
	 */
	public void getDetailData() {
		try {
			String month = this.servletRequest.getParameter("month");
			String uuid = this.servletRequest.getParameter("key");
			String groupid = this.getUserGroup(uuid);

			String operatorData = operatorService.getDetailData(month, groupid);
			printWriter(operatorData);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
