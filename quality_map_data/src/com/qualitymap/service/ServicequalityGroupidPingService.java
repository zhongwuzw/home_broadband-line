package com.qualitymap.service;



/**
 * 测试报告ping 
 * @author：zqh
 * @date：2016.5.4
 */
public interface ServicequalityGroupidPingService {

	
	/**
	 * 获取本月指标情况
	 * @param groupid
	 * @param month
	 * @param broadband_type
	 * @return
	 */
	
	String getPingKPIbyGroupid(String groupid,String month,String broadband_type);
	
	/**
	 * 获取质量分析数据上下月数据的增减情况
	 * @param groupid
	 * @param month
	 * @param broadband_type
	 * @return
	 */
	String getPingServiceQualityCompare(String groupid,String month,String broadband_type);

	
	/**
	 * 获取平均时延趋势
	 * @param groupid
	 * @return
	 * @return String
	 */
	String getAvgDelayDatas(String groupid);
	
	/**
	 * 获取平均时延排名
	 * @param groupid
	 * @return
	 * @return String
	 */
	String getAvgdelayOrder(String month,String groupid);
	
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
	 * @param yearMonth
	 * @param groupid
	 * @return
	 * @return String
	 */
	String getAvgDelay(String yearMonth,String groupid);
	
	/**
	 * 获取上下月的时延达标率
	 * @param yearMonth
	 * @param groupid
	 * @return
	 * @return String
	 */
	String getDelayStandardRate(String yearMonth,String groupid);
	/**
	 * 获取上下月平均ping时延(MS)
	 * @param yearMonth
	 * @param groupid
	 * @return
	 * @return String
	 */
	String getAvgPingDelay(String yearMonth,String groupid);
	/**
	 * 获取上下月90%用户ping时延
	 * @param yearMonth
	 * @param groupid
	 * @return
	 * @return String
	 */
	String getTop90PingDelay(String yearMonth,String groupid);
	/**
	 * 获取上下月ping丢包率
	 * @param yearMonth
	 * @param groupid
	 * @return
	 * @return String
	 */
	String getPingLossRate(String yearMonth,String groupid);
	/**
	 * 获取上下月90%用户ping丢包率
	 * @param yearMonth
	 * @param groupid
	 * @return
	 * @return String
	 */
	String getTop90PingLossRate(String yearMonth,String groupid);
	
}
