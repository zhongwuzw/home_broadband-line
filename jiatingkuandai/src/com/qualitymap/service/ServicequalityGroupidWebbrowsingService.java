package com.qualitymap.service;

/**
 * 
 * @author kongxiangchun
 *
 */
public interface ServicequalityGroupidWebbrowsingService {
	
	/**
	 * 获取本月指标情况
	 * @param groupid
	 * @param month
	 * @param broadband_type
	 * @return
	 */
	
	String getWebbrowsingKPIbyGroupid(String groupid,String month,String broadband_type);
	
	/**
	 * 获取质量分析数据上下月数据的增减情况
	 * @param groupid
	 * @param month
	 * @param broadband_type
	 * @return
	 */
	String getWebbrowsingServiceQualityCompare(String groupid,String month,String broadband_type);
	
	/**
	 * 获取上下月的平均时延
	 * @param yearMonth
	 * @param groupid
	 * @return
	 * @return String
	 */
	String getAvgDelay(String yearMonth,String groupid);
	
	/**
	 * 获取上下月的页面浏览成功率
	 * @param yearMonth
	 * @param groupid
	 * @return
	 * @return String
	 */
	String getPageBrowseSuccess(String yearMonth,String groupid);
	
	/**
	 * 获取页面浏览访问成功趋势
	 * @param groupid
	 * @return
	 * @return String
	 */
	String getPageBrowseSuccessData(String groupid,String probetype);
	
	/**
	 * 获取页面浏览访问成功率排名
	 * @param month
	 * @param groupid
	 * @return
	 * @return String
	 */
	String getPageBrowseSuccessOrder(String month,String groupid,String probetype);

	/**
	 * 获取页面打开成功趋势
	 * @param groupid
	 * @return
	 * @return String
	 */
	String getPageSuccessData(String groupid,String probetype);
	
	/**
	 * 获取页面元素打开成功率排名
	 * @param month
	 * @param groupid
	 * @return
	 * @return String
	 */
	String getPageSuccessOrder(String month,String groupid,String probetype);
	/**
	 * 获取页面显示时延趋势
	 * @param groupid
	 * @return
	 * @return String
	 */
	String getPageDelayData(String groupid,String probetype);
	
	/**
	 * 获取页面显示时延排名
	 * @param month
	 * @param groupid
	 * @return
	 * @return String
	 */
	String getPageDelayOrder(String month,String groupid,String probetype);
	/**
	 * 获取上下月平均页面元素打开成功率
	 * @param yearMonth
	 * @param groupid
	 * @return
	 * @return String
	 */
	String getAvgPageSuccessRate(String yearMonth,String groupid);

	/**
	 * 获取上下月页面元素打开达标率
	 * @param yearMonth
	 * @param groupid
	 * @return
	 * @return String
	 */
	String getPageStandardRate(String yearMonth,String groupid);
	
	/**
	 * 获取上下月的90%用户页面时延
	 * @param yearMonth
	 * @param groupid
	 * @return
	 * @return String
	 */
	String getTop90PageDelay(String yearMonth,String groupid);
	
	/**
	 * 获取上下月90%用户页面元素打开成功率
	 * @param yearMonth
	 * @param groupid
	 * @return
	 * @return String
	 */
	String getTop90PageSuccessRate(String yearMonth,String groupid);


}
