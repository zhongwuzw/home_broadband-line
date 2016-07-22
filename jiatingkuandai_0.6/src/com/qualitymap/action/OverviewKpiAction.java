package com.qualitymap.action;

import javax.annotation.Resource;

import com.qualitymap.base.BaseAction;
import com.qualitymap.service.OverviewKpiService;

/**
 * 概览信息统计
 * 
 * @author：kxc
 * @date：Apr 12, 2016
 */
public class OverviewKpiAction extends BaseAction {

	/**
	 * serialVersionUID long OverviewKpiAction.java
	 */
	private static final long serialVersionUID = 1L;

	@Resource
	private OverviewKpiService kpiService;

	/**
	 * 获取累计用户数
	 * 
	 * @return void
	 */
	public void getAccumulativnum() {

		String uuid = servletRequest.getParameter("key");
		System.out.println(this.getUserGroup(uuid));
		
		String month =this.servletRequest.getParameter("month");
		String groupid = this.getUserGroup(uuid);
		
		String strAccumulativnum = kpiService.getAccumulativnum(month, groupid);
		printWriter(strAccumulativnum);
	}

	/**
	 * 获取参测省份数
	 * 
	 * @return void
	 */
	public void getProvincenum() {
		try {

			String month = this.servletRequest.getParameter("month");
			String uuid = this.servletRequest.getParameter("key");
			String groupid = this.getUserGroup(uuid);

			String province_num = kpiService.getProvincenum(month, groupid);
			printWriter(province_num);
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}

	/**
	 * 获取本月新增用户数
	 * 
	 * @return void
	 */
	public void getOrgnum() {

		try {
			String month = this.servletRequest.getParameter("month");
			String uuid = this.servletRequest.getParameter("key");
			String groupid = this.getUserGroup(uuid);

			String org_num = kpiService.getOrgnum(month, groupid);
			printWriter(org_num);
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}	
	
	/**
	 * 获取渠道数
	 * 
	 * @return void
	 */
	public void getChannelnum() {
		try {
			String month = this.servletRequest.getParameter("month");
			String uuid = this.servletRequest.getParameter("key");
			String groupid = this.getUserGroup(uuid);

			String channel_num = kpiService.getChannelnum(month, groupid);
			printWriter(channel_num);
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}

	/**
	 * 获取终端总数及百分比
	 * 
	 * @return void
	 */
	public void getTerminalNum() {
		try {
			String month = this.servletRequest.getParameter("month");
			String uuid = this.servletRequest.getParameter("key");
			String groupid = this.getUserGroup(uuid);

			String channel_num = kpiService.getTerminalNum(month, groupid);
			printWriter(channel_num);
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}

	/**
	 * 
	 * 
	 * 
	 * 获取累计注册用户数及增长百分比
	 * 
	 * @param month
	 * @param groupid
	 * @return
	 * @return String
	 */
	public void getRegusernameNum() {
		try {
			String month = this.servletRequest.getParameter("month");
			String uuid = this.servletRequest.getParameter("key");
			String groupid = this.getUserGroup(uuid);

			String cregusename_num = kpiService.getRegusernameNum(month, groupid);
			printWriter(cregusename_num);
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}

	/**
	 * 获取累计使用用户数及增长百分比
	 * 
	 * @param month
	 * @param groupid
	 * @return
	 * @return String
	 */
	public void getCustomersNum() {
		try {
			String month = this.servletRequest.getParameter("month");
			String uuid = this.servletRequest.getParameter("key");
			String groupid = this.getUserGroup(uuid);

			String customers_num = kpiService.getCustomersNum(month, groupid);
			printWriter(customers_num);
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}

	/**
	 * 获取新增启动数及增长百分比
	 * 
	 * @param month
	 * @param groupid
	 * @return
	 * @return String
	 */
	public void getNewlyIncreaseNum() {
		try {
			String month = this.servletRequest.getParameter("month");
			String uuid = this.servletRequest.getParameter("key");
			String groupid = this.getUserGroup(uuid);

			String increase_num = kpiService.getNewlyIncreaseNum(month, groupid);
			printWriter(increase_num);
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}

	/**
	 * 获取用户数趋势
	 * 
	 * @return void
	 */
	public void getUserTendencyData() {
		try {
			String uuid = this.servletRequest.getParameter("key");
			String groupid = this.getUserGroup(uuid);
			String tendencyData = kpiService.getUserTendencyData(groupid);
			printWriter(tendencyData);
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}
	
	/**
	 * 获取测试次数
	 * 
	 * @return void
	 */
	public void getTesttimes() {
		try {
			String month = this.servletRequest.getParameter("month");
			String uuid = this.servletRequest.getParameter("key");
			String groupid = this.getUserGroup(uuid);

			String thismonthTesttimes = kpiService.getThismonthTesttimes(month, groupid);
			printWriter(thismonthTesttimes);
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}
		
	
}