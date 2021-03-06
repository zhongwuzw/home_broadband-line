package com.qualitymap.dao;

import java.util.List;
import java.util.Map;

/**
 * 
 * @author kongxiangchun
 *
 */
public interface ServicequalityOrgidPingDao {

	/**
	 * 获取宽带品质监测报告详情
	 * @param groupid
	 * @param month
	 * @param broadband_type
	 * @return
	 */
	List<Map<String, Object>> getPingReportItem(String groupid,String month,String broadband_type);
	
	
	/**
	 * 获取城市list
	 * @param month
	 * @param groupid
	 * @return
	 * @return List<Map<String,Object>>
	 */
	List<Map<String, Object>> getCityList(String month,String groupid,String broadType);
	
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
