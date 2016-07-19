package com.qualitymap.service;

/**
 * 省级纬度数据分析
 * @author：kxc
 * @date：Apr 12, 2016
 */
public interface OverviewProvinceService {

	/**
	 * 获取签约带宽占比
	 * @param month
	 * @param groupid
	 * @return
	 * @return String
	 */
	String getBroadbandData(String month,String groupid);
	
	/**
	 * 获取各省用户占比
	 * @param month
	 * @param groupid
	 * @return
	 * @return String
	 */
	String getProvinceUserPercent(String month,String groupid );
	
	String findByMonth(String month,String group);
	String findProvice(String groupid,String month);
	
	/**
	 * 获取宽带类型累计和本月用户数
	 * @param month
	 * @param groupid
	 * @return
	 * @return String
	 */
	String getBroadbandTypeData(String month,String groupid );
	/**
	 * 根据groupid，month获取样本总数
	 * @param month
	 * @param groupid
	 * @return
	 * @return String
	 */
	String getTotalSampleNum(String month,String groupid,String broadType);
	/**
	 * 根据groupid获取用户(报告中的用户字段)
	 * @param month
	 * @param groupid
	 * @return
	 * @return String
	 */
	String getUsernumByGroupId(String month,String groupid,String broadType);
	/**
	 * 根据groupid获取终端(报告中的终端字段)
	 * @param month
	 * @param groupid
	 * @return
	 * @return String
	 */
	String getTerminalnumByGroupId(String month,String groupid,String broadType);
	
	/**
	 * 获取页面测试报告结果详细信息
	 * @param month
	 * @param groupid
	 * @param broadType
	 * @return
	 */
	String getReportItem(String month,String groupid,String broadType);
	/**
	 * 获取页面测试报告结果以探针类型维度的详细信息
	 * @param month
	 * @param groupid
	 * @param broadType
	 * @return
	 */
	String getProbetypeReportItem(String month,String groupid,String broadType);
	/**
	 * 根据groupid获取有效样本
	 * @param month
	 * @param groupid
	 * @return
	 * @return String
	 */
	String getValidSampleNum(String month,String groupid,String broadType);
}
