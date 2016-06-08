package com.qualitymap.action;

import javax.annotation.Resource;

import com.qualitymap.base.BaseAction;
import com.qualitymap.service.OverviewServicequalityProvinceService;

/**
 * 业务质量数据统计
 * 
 * @author：kxc
 * @date：Apr 11, 2016
 */
public class OverviewServicequalityProvinceAction extends BaseAction {

	@Resource
	private OverviewServicequalityProvinceService overviewServicequalityProvinceService;

	/**
	 * 获取平均时延趋势
	 * 
	 * @return void
	 */
	public void getAvgdelayData() {

		try {

			String uuid = this.servletRequest.getParameter("key");
			String groupid = this.getUserGroup(uuid);
			//String groupid = "10,13";
			String avgdelayData = overviewServicequalityProvinceService.getAvgDelayDatas(groupid);
			printWriter(avgdelayData);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 获取时延平均排名
	 * 
	 * @return void
	 */
	public void getAvgdelayOrder() {

		try {
			String month = this.servletRequest.getParameter("month");
			String uuid = this.servletRequest.getParameter("key");
			String groupid = this.getUserGroup(uuid);
			//String groupid = "10,13";
			String delayOrder = overviewServicequalityProvinceService.getAvgdelayOrder(month, groupid);
			printWriter(delayOrder);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 获取页面打开元素成功率趋势
	 * 
	 * @return void
	 */
	public void getPageSuccessData() {

		try {
			String uuid = this.servletRequest.getParameter("key");
			String groupid = this.getUserGroup(uuid);
			//String groupid = "10,13";
			String avgdelayData = overviewServicequalityProvinceService.getPageSuccessData(groupid);
			printWriter(avgdelayData);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 获取页面元素打开成功率；排名
	 * 
	 * @return void
	 */
	public void getPageSuccessOrder() {

		try {
			String month = this.servletRequest.getParameter("month");
			String uuid = this.servletRequest.getParameter("key");
			String groupid = this.getUserGroup(uuid);
			//String groupid = "10,13";
			String delayOrder = overviewServicequalityProvinceService.getPageSuccessOrder(month, groupid);
			printWriter(delayOrder);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
