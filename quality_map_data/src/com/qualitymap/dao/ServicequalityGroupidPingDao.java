package com.qualitymap.dao;

import java.util.List;
import java.util.Map;

import com.qualitymap.vo.ServicequalityGroupidPing;

/**
 * 
 * @author kongxiangchun
 *
 */
public interface ServicequalityGroupidPingDao {

	/**
	 * 获取本期（当月）最佳的结果数据
	 * @param month
	 * @param groupid
	 * @return
	 * @return String
	 */
	List<Map<String, Object>>  getBestResult(String month,String broadType);

	
	/**
	 * 获取本省的结果数据在全国的排名
	 * @param month
	 * @param groupid
	 * @return
	 * @return String
	 */
	String getDelayResultOrder(String month,String groupid,String broadType);
	
	String getLossRateResultOrder(String month,String groupid,String broadType);
	/**
	 * 获取本省的结果数据
	 * @param month
	 * @param groupid
	 * @return
	 * @return String
	 */
	List<Map<String, Object>> getProvinceResult(String month,String groupid,String broadType);
	
	/**
	 * 获取平均时延趋势图
	 * @param groupid
	 * @return
	 * @return List<Map<String,Object>>
	 */
	List<Map<String, Object>> getAvgDelayData(String groupid);

	/**
	 * 获取平均时延的排名
	 * @param thismonth
	 * @param premonth
	 * @param groupid
	 * @return
	 * @return List<Map<String,Object>>
	 */
	List<Map<String, Object>> getAvgDelayOrder(String thismonth,String premonth,String groupid);
	
	
	/**
	 * 获取宽带品质监测报告详情
	 * @param groupid
	 * @param month
	 * @param broadband_type
	 * @return
	 */
	List<Map<String, Object>> getPingReportItem(String groupid,String month,String broadband_type);
	
	/**
	 * 获取质量分析数据上下月数据的增减情况
	 * @param groupid
	 * @param month
	 * @param broadband_type
	 * @return
	 */
	List<Map<String, Object>> getPingServiceQualityCompare(String groupid,String month,String permonth,String broadband_type);
	
	
	/**
	 * 获取城市list
	 * @param month
	 * @param groupid
	 * @return
	 * @return List<Map<String,Object>>
	 */
	List<Map<String, Object>> getCityList(String month,String groupid);
	
	/**
	 * 根据groupid获取有效样本
	 * @param month
	 * @param groupid
	 * @return
	 * @return String
	 */
	String getValidSampleNum(String month,String groupid,String broadType);
	/**
	 * 获取上下月的平均时延
	 */
	public List<Map<String, Object>> getAvgDelay(String yearMonth, String lastMonth, String groupid);
	/**
	 * 获取上下月的时延达标率
	 */
	public List<Map<String, Object>> getDelayStandardRate(String yearMonth, String lastMonth, String groupid);
	
	/**
	 * 获取上下月平均ping时延(MS)
	 */
	public List<Map<String, Object>> getAvgPingDelay(String yearMonth, String lastMonth, String groupid);
	
	/**
	 * 获取上下月90%用户ping时延
	 */
	public List<Map<String, Object>> getTop90PingDelay(String yearMonth, String lastMonth, String groupid);
	
	/**
	 * 获取上下月ping丢包率
	 */
	public List<Map<String, Object>> getPingLossRate(String yearMonth, String lastMonth, String groupid);
	
	
	/**
	 * 获取上下月90%用户ping丢包率
	 */
	public List<Map<String, Object>> getTop90PingLossRate(String yearMonth, String lastMonth, String groupid); 
	
	
	
}
