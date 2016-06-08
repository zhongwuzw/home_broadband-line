package com.qualitymap.service;

/**
 * 
 * 
 * @author：kxc
 * @date：Apr 8, 2016
 */
public interface OverviewKpiApiService {

	
	/**
	 * 获取累计注册用户数及增长百分比
	 * @param month
	 * @param groupid
	 * @return
	 * @return String
	 */
	String getRegusernameNum(String month,String userName);
	/**
	 * 获取累计使用用户数及增长百分比
	 * @param month
	 * @param groupid
	 * @return
	 * @return String
	 */
	String getCustomersNum(String month,String userName);
}
