package com.qualitymap.action;

import javax.annotation.Resource;
import com.qualitymap.base.BaseAction;
import com.qualitymap.service.TerminalOsService;

public class TerminalOsAction extends BaseAction {

	@Resource
	private TerminalOsService terminalOsService;

	/**
	 * 获取本月平台测试次数占比
	 */
	public void getMonthPlatTesttimeProportion() {

		try {
			String month = this.servletRequest.getParameter("month");
			String uuid = this.servletRequest.getParameter("key");
			String groupid = this.getUserGroup(uuid);
			String msg = terminalOsService.getPlatformPercent(month,groupid);
			printWriter(msg);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	/**
	 * 获取用户数平台累计和本月
	 */
	public void getPlatData() {

		try {
			String month = this.servletRequest.getParameter("month");
			String uuid = this.servletRequest.getParameter("key");
			String groupid = this.getUserGroup(uuid);
			String msg = terminalOsService.getPlatData(month,groupid);
			printWriter(msg);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 获取平台分布数据
	 */
	public void getPlatformDistribution() {

		try {
			String month = this.servletRequest.getParameter("month");
			String uuid = this.servletRequest.getParameter("key");
			String groupid = this.getUserGroup(uuid);
			String msg = terminalOsService.getPlatformDistribution(month, groupid);
			printWriter(msg);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 获取平台数据详细信息
	 */
	public void getPlatformDetailData() {
		
		try {
			String uuid = this.servletRequest.getParameter("key");
			//String groupid = "10,12";
			String groupid = this.getUserGroup(uuid);
			String msg = terminalOsService.getPlatformDetailData(groupid);
			printWriter(msg);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
