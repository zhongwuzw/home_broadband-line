package com.qualitymap.service;



/**
 * Description:  OTS统计平台业务质量
 * @author zqh
 * 2016-4-7: PM 03:29:11
 */
public interface OverviewServicequalityService {
	
	/**
	 * 获取用户指标统计
	 * @param groupid
	 * @param month
	 * @param broadband_type
	 * @return
	 */
	String getUserIndicator(String groupid, String month, String broadband_type);

	
	/**
	 * 获取地域级别用户指标统计
	 * @param groupid
	 * @param month
	 * @param broadband_type
	 * @return
	 */
	String getTerritoryData(String groupid, String month, String broadband_type);
	
	/**
	 * 分地域指标统计
	 * @param groupid
	 * @param month
	 * @param broadband_type
	 * @return
	 */
	String getCityData(String groupid, String month, String broadband_type);
}
