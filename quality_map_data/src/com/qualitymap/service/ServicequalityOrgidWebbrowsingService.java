package com.qualitymap.service;


/**
 * 
 * @author kongxiangchun
 *
 */
public interface ServicequalityOrgidWebbrowsingService {

	/**
	 * 获取宽带品质监测报告详情
	 * @param groupid
	 * @param month
	 * @param broadband_type
	 * @return
	 */
	String getPingReportItem(String groupid,String month,String broadband_type);
	/**
	 * 获取城市list
	 * @param month
	 * @param groupid
	 * @return
	 * @return String
	 */
	String getCityList(String month,String groupid,String broadType);
	
	/**
	 * 根据groupid获取有效样本
	 * @param month
	 * @param groupid
	 * @return
	 * @return String
	 */
	String getValidSampleNum(String month,String groupid,String broadType);
	/**
	 * 根据groupid获取账户数(报告中的地区字段)
	 * @param month
	 * @param groupid
	 * @return
	 * @return String
	 */
	String getOrgnumByGroupId(String month,String groupid,String broadType);
}
