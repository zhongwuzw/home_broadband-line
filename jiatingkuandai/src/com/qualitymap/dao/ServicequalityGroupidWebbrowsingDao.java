package com.qualitymap.dao;

import java.util.List;
import java.util.Map;

/**
 * 
 * @author kongxiangchun
 *
 */
public interface ServicequalityGroupidWebbrowsingDao {

	
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
	
	String getSuccessRateResultOrder(String month,String groupid,String broadType);
	/**
	 * 获取本省的结果数据
	 * @param month
	 * @param groupid
	 * @return
	 * @return String
	 */
	List<Map<String, Object>> getProvinceResult(String month,String groupid,String broadType);
	
	/**
	 * 获取质量分析数据上下月数据的增减情况
	 * @param groupid
	 * @param month
	 * @param broadband_type
	 * @return
	 */
	List<Map<String, Object>> getWebbrowsingServiceQualityCompare(String groupid,String month,String permonth,String broadband_type);
	
	
	
	/**
	 * 获取上下月的平均时延
	 */
	public List<Map<String, Object>> getAvgDelay(String yearMonth, String lastMonth, String groupid);
	
	/**
	 * 获取页面浏览访问成功率趋势数据
	 * @param groupid
	 * @return
	 * @return List<Map<String,Object>>
	 */
	List<Map<String, Object>> getPageBrowseSuccessData(String groupid,String probetype);
	
	/**
	 * 获取页面浏览访问成功率的排名
	 * @param thismonth
	 * @param premonth
	 * @param groupid
	 * @return
	 * @return List<Map<String,Object>>
	 */
	List<Map<String, Object>> getPageBrowseSuccessOrder(String thismonth,String premonth,String groupid,String probetype);
	
	/**
	 * 获取页面元素打开成功率趋势数据
	 * @param groupid
	 * @return
	 * @return List<Map<String,Object>>
	 */
	List<Map<String, Object>> getPageSuccessData(String groupid,String probetype);
	
	/**
	 * 获取页面元素打开成功率的排名
	 * @param thismonth
	 * @param premonth
	 * @param groupid
	 * @return
	 * @return List<Map<String,Object>>
	 */
	List<Map<String, Object>> getPageSuccessOrder(String thismonth,String premonth,String groupid,String probetype);
	/**
	 * 获取页面显示时延趋势数据
	 * @param groupid
	 * @return
	 * @return List<Map<String,Object>>
	 */
	List<Map<String, Object>> getPageDelayData(String groupid,String probetype);
	
	/**
	 * 获取页面显示时延的排名
	 * @param thismonth
	 * @param premonth
	 * @param groupid
	 * @return
	 * @return List<Map<String,Object>>
	 */
	List<Map<String, Object>> getPageDelayOrder(String thismonth,String premonth,String groupid,String probetype);
	
	/**
	 * 获取上下月平均页面元素打开成功率
	 */
	public List<Map<String, Object>> getAvgPageSuccessRate(String yearMonth, String lastMonth, String groupid);
	
	/**
	 * 获取上下月页面元素打开达标率
	 */
	public List<Map<String, Object>> getPageStandardRate(String yearMonth, String lastMonth, String groupid);
	
	
	/**
	 * 获取上下月90%用户页面时延
	 */
	public List<Map<String, Object>> getTop90PageDelay(String yearMonth, String lastMonth, String groupid);
	
	
	/**
	 * 获取上下月90%用户页面元素打开成功率
	 */
	public List<Map<String, Object>> getTop90PageSuccessRate(String yearMonth, String lastMonth, String groupid);
	
	
}
