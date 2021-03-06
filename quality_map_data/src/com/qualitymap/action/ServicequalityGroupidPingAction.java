package com.qualitymap.action;

import javax.annotation.Resource;

import com.qualitymap.base.BaseAction;
import com.qualitymap.service.ServicequalityGroupidPingService;

public class ServicequalityGroupidPingAction extends BaseAction {

	@Resource
	ServicequalityGroupidPingService groupidPingService;

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
			String avgdelayData = groupidPingService.getAvgDelayDatas(groupid);
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
			String delayOrder = groupidPingService.getAvgdelayOrder(month, groupid);
			printWriter(delayOrder);
		} catch (Exception e) {
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
			String broadType = this.servletRequest.getParameter("broadType");//宽带类型
			String ping_test_times = groupidPingService.getValidSampleNum(month, groupid,broadType);
			printWriter(ping_test_times);
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}

	/**
	 * 获取宽带品质监测报告详情
	 */
	
/*	public void getPingReportItem() {
		String month = this.servletRequest.getParameter("month");
		String groupid = this.servletRequest.getParameter("groupid");
		String broadband_type = this.servletRequest.getParameter("broadband_type");

		String datajson = groupidPingService.getPingReportItem(groupid, month, broadband_type);
		printWriter(datajson);
	}
	*/
	
	/**
	 * 获取平均时延上下月数据
	 */
	public void getAvgDelay() {

		try {
			String yearMonth = this.servletRequest.getParameter("month");
			String uuid = this.servletRequest.getParameter("key");
			String groupid = this.getUserGroup(uuid);
			String avgdata = groupidPingService.getAvgDelay(yearMonth, groupid);
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
			String msg = groupidPingService.getDelayStandardRate(yearMonth, groupid);
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
			String msg = groupidPingService.getAvgPingDelay(yearMonth, groupid);
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
			String msg = groupidPingService.getTop90PingDelay(yearMonth, groupid);
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
			String msg = groupidPingService.getPingLossRate(yearMonth, groupid);
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
			String msg = groupidPingService.getTop90PingLossRate(yearMonth, groupid);
			printWriter(msg);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	/**
	 * 获取质量分析数据上下月数据的增减情况
	 */
	public void getPingServiceQualityCompare(){

		try {
			String month = this.servletRequest.getParameter("month");
			String groupid = this.servletRequest.getParameter("groupid");
			String broadband_type = this.servletRequest.getParameter("broadband_type");
			String msg = groupidPingService.getPingServiceQualityCompare(groupid, month, broadband_type);
			printWriter(msg);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	/**
	 * 获取本月指标情况
	 */
	public void getPingKPIbyGroupid(){
		
		try {
			String month = this.servletRequest.getParameter("month");
			String groupid = this.servletRequest.getParameter("groupid");
			String broadband_type = this.servletRequest.getParameter("broadband_type");
			String msg = groupidPingService.getPingKPIbyGroupid(groupid, month, broadband_type);
			printWriter(msg);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
