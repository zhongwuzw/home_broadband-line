package com.qualitymap.dao;

import java.util.List;
import java.util.Map;

import com.qualitymap.vo.OverviewServicequality;

public interface OverviewServicequalityDao {

	/**
	 * 获取上下月的平均时延
	 * 
	 * @param yearMonth
	 * @param groupid
	 * @return
	 * @return String
	 */
	List<Map<String, Object>> getAvgDelay(String yearMonth, String lastMonth, String groupid);

	/**
	 * 获取上下月的时延达标率
	 * 
	 * @param yearMonth
	 * @param groupid
	 * @return
	 * @return String
	 */
	List<Map<String, Object>> getDelayStandardRate(String yearMonth, String lastMonth, String groupid);

	/**
	 * 获取上下月平均页面元素打开成功率
	 * 
	 * @param yearMonth
	 * @param groupid
	 * @return
	 * @return String
	 */
	List<Map<String, Object>> getAvgPageSuccessRate(String yearMonth, String lastMonth, String groupid);

	/**
	 * 获取上下月页面元素打开达标率
	 * 
	 * @param yearMonth
	 * @param groupid
	 * @return
	 * @return String
	 */
	List<Map<String, Object>> getPageStandardRate(String yearMonth, String lastMonth, String groupid);

	void savequality(OverviewServicequality serquality);

	void deletequality(OverviewServicequality serquality);

	List<OverviewServicequality> findquality();

	void updatequality(OverviewServicequality serquality);
	
	/**
	 * 获取上下月的90%用户页面时延
	 * 
	 * @param yearMonth
	 * @param groupid
	 * @return
	 * @return String
	 */
	List<Map<String, Object>> getTop90PageDelay(String yearMonth, String lastMonth, String groupid);
	
	/**
	 * 获取上下月90%用户页面元素打开成功率
	 * 
	 * @param yearMonth
	 * @param groupid
	 * @return
	 * @return String
	 */
	List<Map<String, Object>> getTop90PageSuccessRate(String yearMonth, String lastMonth, String groupid);
	
	/**
	 * 获取上下月平均ping时延(MS)
	 * 
	 * @param yearMonth
	 * @param groupid
	 * @return
	 * @return String
	 */
	List<Map<String, Object>> getAvgPingDelay(String yearMonth, String lastMonth, String groupid);
	
	/**
	 * 获取上下月90%用户ping时延
	 * 
	 * @param yearMonth
	 * @param groupid
	 * @return
	 * @return String
	 */
	List<Map<String, Object>> getTop90PingDelay(String yearMonth, String lastMonth, String groupid);
	
	/**
	 * 获取上下月ping丢包率
	 * 
	 * @param yearMonth
	 * @param groupid
	 * @return
	 * @return String
	 */
	List<Map<String, Object>> getPingLossRate(String yearMonth, String lastMonth, String groupid);
	
	/**
	 * 获取上下月90%用户ping丢包率
	 * 
	 * @param yearMonth
	 * @param groupid
	 * @return
	 * @return String
	 */
	List<Map<String, Object>> getTop90PingLossRate(String yearMonth, String lastMonth, String groupid);
}
