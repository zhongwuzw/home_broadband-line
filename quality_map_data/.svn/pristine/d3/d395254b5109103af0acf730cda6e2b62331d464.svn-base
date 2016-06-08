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
	 * 获取平均时延上下月数据
	 */
	public void getAvgDelay() {

		try {
			String yearMonth = this.servletRequest.getParameter("month");
			String uuid = this.servletRequest.getParameter("key");
			String groupid = this.getUserGroup(uuid);
			String avgdata = overviewServicequalityService.getAvgDelay(yearMonth, groupid);
			printWriter(avgdata);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	/**
	 * 获取时延达标率上下月数据
	 */
	public void getDelayStandardRate() {

		try {
			String yearMonth = this.servletRequest.getParameter("month");
			String uuid = this.servletRequest.getParameter("key");
			String groupid = this.getUserGroup(uuid);
			String msg = overviewServicequalityService.getDelayStandardRate(yearMonth, groupid);
			printWriter(msg);
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
			String msg = overviewServicequalityService.getAvgPageSuccessRate(yearMonth, groupid);
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
			String msg = overviewServicequalityService.getPageStandardRate(yearMonth, groupid);
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
			String msg = overviewServicequalityService.getTop90PageDelay(yearMonth, groupid);
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
			String msg = overviewServicequalityService.getTop90PageSuccessRate(yearMonth, groupid);
			printWriter(msg);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 平均ping时延(MS)上下月数据
	 */
	public void getAvgPingDelay() {

		try {
			String yearMonth = this.servletRequest.getParameter("month");
			String uuid = this.servletRequest.getParameter("key");
			String groupid = this.getUserGroup(uuid);
			String msg = overviewServicequalityService.getAvgPingDelay(yearMonth, groupid);
			printWriter(msg);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	/**
	 * 90%用户ping时延上下月数据
	 */
	public void getTop90PingDelay() {

		try {
			String yearMonth = this.servletRequest.getParameter("month");
			String uuid = this.servletRequest.getParameter("key");
			String groupid = this.getUserGroup(uuid);
			String msg = overviewServicequalityService.getTop90PingDelay(yearMonth, groupid);
			printWriter(msg);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	/**
	 * ping丢包率上下月数据
	 */
	public void getPingLossRate() {

		try {
			String yearMonth = this.servletRequest.getParameter("month");
			String uuid = this.servletRequest.getParameter("key");
			String groupid = this.getUserGroup(uuid);
			String msg = overviewServicequalityService.getPingLossRate(yearMonth, groupid);
			printWriter(msg);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	/**
	 * 90%用户ping丢包率上下月数据
	 */
	public void getTop90PingLossRate() {

		try {
			String yearMonth = this.servletRequest.getParameter("month");
			String uuid = this.servletRequest.getParameter("key");
			String groupid = this.getUserGroup(uuid);
			String msg = overviewServicequalityService.getTop90PingLossRate(yearMonth, groupid);
			printWriter(msg);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
}
