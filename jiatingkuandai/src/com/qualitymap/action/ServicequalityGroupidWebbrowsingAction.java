package com.qualitymap.action;

import javax.annotation.Resource;

import com.qualitymap.base.BaseAction;
import com.qualitymap.service.ServicequalityGroupidWebbrowsingService;

public class ServicequalityGroupidWebbrowsingAction extends BaseAction {

	@Resource
	ServicequalityGroupidWebbrowsingService groupidWebbrowsingService;

	/**
	 * 获取本月指标情况
	 */
	public void getWebbrowsingKPIbyGroupid(){
		
		try {
			String month = this.servletRequest.getParameter("month");
			String groupid = this.servletRequest.getParameter("groupid");
			String broadband_type = this.servletRequest.getParameter("broadband_type");
			String msg = groupidWebbrowsingService.getWebbrowsingKPIbyGroupid(groupid, month, broadband_type);
			printWriter(msg);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	

	/**
	 * 获取质量分析数据上下月数据的增减情况
	 */
	public void getWebbrowsingServiceQualityCompare(){

		try {
			String month = this.servletRequest.getParameter("month");
			String groupid = this.servletRequest.getParameter("groupid");
			String broadband_type = this.servletRequest.getParameter("broadband_type");
			String msg = groupidWebbrowsingService.getWebbrowsingServiceQualityCompare(groupid, month, broadband_type);
			printWriter(msg);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 获取页面访问成功率本月上月
	 */
	public void getPageBrowseSuccess() {

		try {
			String yearMonth = this.servletRequest.getParameter("month");
			String uuid = this.servletRequest.getParameter("key");
			String groupid = this.getUserGroup(uuid);
			//String groupid = "50091,50067";
			String avgdata = groupidWebbrowsingService.getPageBrowseSuccess(yearMonth, groupid);
			printWriter(avgdata);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
	
	/**
	 * 获取平均时延上下月数据
	 */
	public void getAvgDelay() {

		try {
			String yearMonth = this.servletRequest.getParameter("month");
			String uuid = this.servletRequest.getParameter("key");
			String groupid = this.getUserGroup(uuid);
			//String groupid = "50091,50067";
			String avgdata = groupidWebbrowsingService.getAvgDelay(yearMonth, groupid);
			printWriter(avgdata);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
	
	/**
	 * 获取页面浏览访问成功率趋势
	 * 
	 * @return void
	 */
	public void getPageBrowseSuccessData() {

		try {
			String uuid = this.servletRequest.getParameter("key");
			String probetype = this.servletRequest.getParameter("probetype");
			String groupid = this.getUserGroup(uuid);
			//String groupid = "50071,50074,50067,50068";
			String avgdelayData = groupidWebbrowsingService.getPageBrowseSuccessData(groupid,probetype);
			printWriter(avgdelayData);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 获取页面浏览访问成功率；排名
	 * 
	 * @return void
	 */
	public void getPageBrowseSuccessOrder() {

		try {
			String month = this.servletRequest.getParameter("month");
			String probetype = this.servletRequest.getParameter("probetype");
			String uuid = this.servletRequest.getParameter("key");
			String groupid = this.getUserGroup(uuid);
			//String groupid = "50071,50074,50067,50068";
			String delayOrder = groupidWebbrowsingService.getPageBrowseSuccessOrder(month, groupid,probetype);
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
			String probetype = this.servletRequest.getParameter("probetype");
			String groupid = this.getUserGroup(uuid);
			//String groupid = "50071,50074,50067,50068";
			String avgdelayData = groupidWebbrowsingService.getPageSuccessData(groupid,probetype);
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
			String probetype = this.servletRequest.getParameter("probetype");
			String uuid = this.servletRequest.getParameter("key");
			String groupid = this.getUserGroup(uuid);
			//String groupid = "50071,50074,50067,50068";
			String delayOrder = groupidWebbrowsingService.getPageSuccessOrder(month, groupid,probetype);
			printWriter(delayOrder);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	/**
	 * 获取页面显示时延趋势
	 * 
	 * @return void
	 */
	public void getPageDelayData() {
		
		try {
			String uuid = this.servletRequest.getParameter("key");
			String probetype = this.servletRequest.getParameter("probetype");
			String groupid = this.getUserGroup(uuid);
			//String groupid = "50071,50074,50067,50068";
			String avgdelayData = groupidWebbrowsingService.getPageDelayData(groupid, probetype);
			printWriter(avgdelayData);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 获取页面显示时延排名
	 * 
	 * @return void
	 */
	public void getPageDelayOrder() {
		
		try {
			String month = this.servletRequest.getParameter("month");
			String probetype = this.servletRequest.getParameter("probetype");
			String uuid = this.servletRequest.getParameter("key");
			String groupid = this.getUserGroup(uuid);
			//String groupid = "10,13";
			String delayOrder = groupidWebbrowsingService.getPageDelayOrder(month, groupid, probetype);
			printWriter(delayOrder);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 平均页面元素打开成功率上下月数据
	 */
	public void getAvgPageSuccessRate() {
		try {
			String yearMonth = this.servletRequest.getParameter("month");
			String uuid = this.servletRequest.getParameter("key");
			String groupid = this.getUserGroup(uuid);
			String msg = groupidWebbrowsingService.getAvgPageSuccessRate(yearMonth, groupid);
			printWriter(msg);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 页面元素打开达标率上下月数据
	 */
	public void getPageStandardRate() {

		try {
			String yearMonth = this.servletRequest.getParameter("month");
			String uuid = this.servletRequest.getParameter("key");
			String groupid = this.getUserGroup(uuid);
			//String groupid = "50091,50067";
			String msg = groupidWebbrowsingService.getPageStandardRate(yearMonth, groupid);
			printWriter(msg);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 获取90%用户页面时延上下月数据
	 */
	public void getTop90PageDelay() {

		try {
			String yearMonth = this.servletRequest.getParameter("month");
			String uuid = this.servletRequest.getParameter("key");
			String groupid = this.getUserGroup(uuid);
			String msg = groupidWebbrowsingService.getTop90PageDelay(yearMonth, groupid);
			printWriter(msg);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 90%用户页面元素打开成功率上下月数据
	 */
	public void getTop90PageSuccessRate() {

		try {
			String yearMonth = this.servletRequest.getParameter("month");
			String uuid = this.servletRequest.getParameter("key");
			String groupid = this.getUserGroup(uuid);
			String msg = groupidWebbrowsingService.getTop90PageSuccessRate(yearMonth, groupid);
			printWriter(msg);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
