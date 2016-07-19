package com.qualitymap.service;


/**
 * 
 * @author kongxiangchun
 *
 */
public interface ServicequalityGroupidHttpDownloadService {
	
	/**
	 * 获取上下月的下载成功率
	 * @param yearMonth
	 * @param groupid
	 * @return
	 * @return String
	 */
	String getHttpDownloadSuccessRate(String yearMonth,String groupid);
	
	/**
	 * 获取上下月的下载速率
	 * @param yearMonth
	 * @param groupid
	 * @return
	 * @return String
	 */
	String getHttpDownloadRate(String yearMonth,String groupid);
	

	/**
	 * 获取下载速率趋势数据
	 * @param groupid
	 * @return
	 * @return String
	 */
	String getDownloadRateData(String groupid,String probetype);
	
	/**
	 * 获取下载成功率趋势数据
	 * @param groupid
	 * @return
	 * @return String
	 */
	String getDownloadSuccessRateData(String groupid,String probetype);
	
	
	/**
	 * 获取下载成功率的排名
	 * @param thismonth
	 * @param premonth
	 * @param groupid
	 * @return
	 * @return String
	 */
	String getDownloadSuccessRateOrder(String month,String groupid,String probetype);
	
	/**
	 * 获取下载速率的排名
	 * @param thismonth
	 * @param premonth
	 * @param groupid
	 * @return
	 * @return String
	 */
	String getDownloadRateOrder(String month,String groupid,String probetype);

}
