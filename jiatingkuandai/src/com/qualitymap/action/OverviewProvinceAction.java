package com.qualitymap.action;

import javax.annotation.Resource;

import com.qualitymap.base.BaseAction;
import com.qualitymap.service.OverviewProvinceService;

/**
 * 省级纬度数据分析
 * 
 * @author：kxc
 * @date：Apr 12, 2016
 */
public class OverviewProvinceAction extends BaseAction {

	/**
	 * serialVersionUID long OverviewProvinceAction.java
	 */
	private static final long serialVersionUID = 1L;
	@Resource
	private OverviewProvinceService provinceService;

	/**
	 * 根据时间查询本月的测试次数
	 * 
	 * @return void
	 */
	public void findByMonth() {

		try {

			String month = this.servletRequest.getParameter("month");
			String uuid = this.servletRequest.getParameter("key");
			String groupid = this.getUserGroup(uuid);
			String datajson = provinceService.findByMonth(month, groupid);
			printWriter(datajson);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 查询所有的数据
	 * 
	 * @return void
	 */
	public void findAll() {
		String uuid = this.servletRequest.getParameter("key");
		String month = this.servletRequest.getParameter("month");
		String groupid = this.getUserGroup(uuid);
		String datajson = provinceService.findProvice(groupid,month);
		printWriter(datajson);
	}

	/**
	 * 获取签约带宽占比
	 * 
	 * @return void
	 */
	public void getBroadbandData() {

		try {
			String month = this.servletRequest.getParameter("month");
			String uuid = this.servletRequest.getParameter("key");
			String groupid = this.getUserGroup(uuid);
			String broadbandData = provinceService.getBroadbandData(month, groupid);
			printWriter(broadbandData);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 获取各省用户占比
	 * 
	 * @return void
	 */
	public void getProvinceUserPercent() {

		try {
			String month = this.servletRequest.getParameter("month");
			String uuid = this.servletRequest.getParameter("key");
			String groupid = this.getUserGroup(uuid);
			//String groupid = "10,12";
			String provinceUserData = provinceService.getProvinceUserPercent(month, groupid);
			printWriter(provinceUserData);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 获取宽带类型累计和本月用户数
	 */
	public void getBroadbandTypeData() {

		try {
			String month = this.servletRequest.getParameter("month");
			String uuid = this.servletRequest.getParameter("key");
			String groupid = this.getUserGroup(uuid);
			String msg = provinceService.getBroadbandTypeData(month,groupid);
			printWriter(msg);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 根据groupid，month获取样本总数
	 * 
	 * @return void
	 */
	public void getTotalSampleNum() {
		try {
			String month = this.servletRequest.getParameter("month");
			String groupid = this.servletRequest.getParameter("groupid");  //页面获取指定groupid
			String broadType = this.servletRequest.getParameter("broadType");
			String new_sample_num = provinceService.getTotalSampleNum(month, groupid,broadType);
			printWriter(new_sample_num);
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}
	/**
	 * 根据groupid获取用户(报告中的用户字段) 
	 * 
	 * @return void
	 */
	public void getUsernumByGroupId() {

		try {
			String month = this.servletRequest.getParameter("month");
			String groupid = this.servletRequest.getParameter("groupid");  //页面获取指定groupid
			if(groupid.isEmpty()){
				String uuid = this.servletRequest.getParameter("key");
				groupid = this.getUserGroup(uuid);
			}
			String broadType = this.servletRequest.getParameter("broadType"); //宽带类型
			String new_user_num = provinceService.getUsernumByGroupId(month, groupid,broadType);
			printWriter(new_user_num);
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}
	
	/**
	 * 根据groupid获取终端(报告中的终端字段) 
	 * 
	 * @return void
	 */
	public void getTerminalnumByGroupId() {

		try {
			String month = this.servletRequest.getParameter("month");
			String groupid = this.servletRequest.getParameter("groupid");  //页面获取指定groupid
			if(groupid.isEmpty()){
				String uuid = this.servletRequest.getParameter("key");
				groupid = this.getUserGroup(uuid);
			}
			String broadType = this.servletRequest.getParameter("broadType");//宽带类型
			String new_terminal_num = provinceService.getTerminalnumByGroupId(month, groupid,broadType);
			printWriter(new_terminal_num);
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}
	
	/**
	 * 获取页面测试报告结果详细信息
	 */
	public void getReportItem() {
		
		try {
			String month = this.servletRequest.getParameter("month");
			String groupid = this.servletRequest.getParameter("groupid");  //页面获取指定groupid
			String broadType = this.servletRequest.getParameter("broadType");//宽带类型
			String new_terminal_num = provinceService.getReportItem(month, groupid,broadType);
			printWriter(new_terminal_num);
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}
	
	/**
	 * 获取页面测试报告结果以探针类型维度的详细信息
	 * @return
	 */
	public void getProbetypeReportItem() {
		
		try {
			String month = this.servletRequest.getParameter("month");
			String groupid = this.servletRequest.getParameter("groupid");  //页面获取指定groupid
			String broadType = this.servletRequest.getParameter("broadType");//宽带类型
			String new_terminal_num = provinceService.getProbetypeReportItem(month, groupid,broadType);
			printWriter(new_terminal_num);
		} catch (Exception e) {
			// TODO: handle exception
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
			if(groupid.isEmpty()){
				String uuid = this.servletRequest.getParameter("key");
				groupid = this.getUserGroup(uuid);
			}
			String broadType = this.servletRequest.getParameter("broadType");
			String ping_test_times = provinceService.getValidSampleNum(month, groupid,broadType);
			printWriter(ping_test_times);
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}
	/**
	 * 获取质量分析 即 上下月中数据的增减情况
	 * 
	 * @return void
	 */
	public void getServiceQualityCompare() {
		try {
			String month = this.servletRequest.getParameter("month");
			String groupid = this.servletRequest.getParameter("groupid");  //页面获取指定groupid
			if(groupid.isEmpty()){
				String uuid = this.servletRequest.getParameter("key");
				groupid = this.getUserGroup(uuid);
			}
			String broadType = this.servletRequest.getParameter("broadType");
			String ping_test_times = provinceService.getServiceQualityCompare(month, groupid, broadType);
			printWriter(ping_test_times);
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}
	/**
	 * 获取质量分析 即 上下月中数据的增减情况
	 * 
	 * @return void
	 */
	public void getKPIbyGroupid() {
		try {
			String month = this.servletRequest.getParameter("month");
			String groupid = this.servletRequest.getParameter("groupid");  //页面获取指定groupid
			if(groupid.isEmpty()){
				String uuid = this.servletRequest.getParameter("key");
				groupid = this.getUserGroup(uuid);
			}
			String broadType = this.servletRequest.getParameter("broadType");
			//String parameter = this.servletRequest.getParameter("parameter");
			String ping_test_times = provinceService.getKPIbyGroupid( month, groupid, broadType);
			printWriter(ping_test_times);
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}
	/**
	 * 获取质量分析 即 上下月中数据的增减情况
	 * 
	 * @return void
	 */
	public void getProvinceName() {
		try {
			String uuid = this.servletRequest.getParameter("key");
			String groupid = this.getUserGroup(uuid);  //页面获取指定groupid
			//String groupid = "50093,50094";
			String groupname = provinceService.getProvinceName(groupid);
			printWriter(groupname);
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}

}
