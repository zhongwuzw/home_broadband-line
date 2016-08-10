package com.qualitymap.service;



/**
 * 
 * 
 * @author：kxc
 * @date：Apr 8, 2016
 */
public interface OverviewKpiService {

	/**
	 * 获取累计用户数
	 * @param month
	 * @param groupid
	 * @return
	 * @return String
	 */
	String getAccumulativnum(String month,String groupid);
	
	/**
	 * 获取参测省份数
	 * @param month
	 * @param groupid
	 * @return
	 * @return String
	 */
	String getProvincenum(String month,String groupid);
	
	/**
	 * 获取本月新增用户数
	 * @param month
	 * @param groupid
	 * @return
	 * @return String
	 */
	String getOrgnum(String month,String groupid);
	
	
	/**
	 * 获取渠道数
	 * @param month
	 * @param groupid
	 * @return
	 * @return String
	 */
	String getChannelnum(String month,String groupid);
	
	/**
	 * 获取终端总数及增长百分比
	 * @param month
	 * @param groupid
	 * @return
	 * @return String
	 */
	String getTerminalNum(String month,String groupid);
	/**
	 * 获取累计注册用户数及增长百分比
	 * @param month
	 * @param groupid
	 * @return
	 * @return String
	 */
	String getRegusernameNum(String month,String groupid);
	/**
	 * 获取累计使用用户数及增长百分比
	 * @param month
	 * @param groupid
	 * @return
	 * @return String
	 */
	String getCustomersNum(String month,String groupid);
	/**
	 * 获取新增启动数及增长百分比
	 * @param month
	 * @param groupid
	 * @return
	 * @return String
	 */
	String getNewlyIncreaseNum(String month,String groupid);
	
	/**
	 * 获取用户趋势数据
	 * @return
	 * @return List<Map<String,Object>>
	 */
	String getUserTendencyData(String groupid);
	
	/**
	 * 获取本月测试次数
	 * @param month
	 * @param groupid
	 * @return
	 * @return String
	 */
	String getThismonthTesttimes(String month,String groupid);
	
	
}