package com.qualitymap.service;

/**
 * 网络分析的数据统计
 * @author：kxc
 * @date：Apr 11, 2016
 */
public interface NetworkOperatorService {
	/**
	 * 根据月份及groupid获取运营商
	 * @param month
	 * @param groupid
	 * @return
	 * @return String
	 */
	String getOperatorData(String month,String groupid);
	
	/**
	 * 获取运营商的详细信息
	 * @param month
	 * @param groupid
	 * @return
	 * @return String
	 */
	String getDetailData(String month,String groupid);
}
