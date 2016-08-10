package com.qualitymap.dao;

import java.util.List;
import java.util.Map;

import com.qualitymap.vo.OverviewProvince;

/**
 * 省级纬度数据分析
 * @author：kxc
 * @date：Apr 12, 2016
 */
public interface OverviewProvinceDao {
	
	
	/**
	 * 获取签约带宽占比
	 * @param month
	 * @param groupid
	 * @return
	 * @return String
	 */
	List<Map<String, Object>> getBroadbandData(String month,String groupid);
	
	/**
	 * 获取各省用户占比
	 * @param month
	 * @param groupid
	 * @return
	 * @return List<Map<String,Object>>
	 */
	List<Map<String, Object>> getProvinceUserPercent(String month,String groupid );

	void saveProvice(OverviewProvince viewPro);

	void deletProvice(OverviewProvince viewPro);

	List<OverviewProvince> findProvice(String groupid,String month);

	void updateProvice(OverviewProvince viewPro);
	
	List<OverviewProvince> findByMonth(String month,String group);

	/**
	 * 获取宽带类型累计和本月用户数
	 * @param month
	 * @param groupid
	 * @return
	 * @return List<Map<String,Object>>
	 */
	List<Map<String, Object>> getBroadbandTypeData(String month,String groupid);
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
	 * 下面6个接口为查询测试报告结果需要
	 * @param groupid
	 * @param month
	 * @param broadband_type
	 * @return
	 */
	List<Map<String, Object>> getwebdelaydata(String groupid,String month,String broadband_type);
	List<Map<String, Object>> getvideodelaydata(String groupid,String month,String broadband_type);
	List<Map<String, Object>> getwebratedata(String groupid,String month,String broadband_type);
	List<Map<String, Object>> getvideoproportiondata(String groupid,String month,String broadband_type);
	List<Map<String, Object>> getwebninetydelaydata(String groupid,String month,String broadband_type);
	List<Map<String, Object>> getvideocachedata(String groupid,String month,String broadband_type);
	List<Map<String, Object>> gethttpuploaddata(String groupid,String month,String broadband_type);
	List<Map<String, Object>> gethttpdownloaddata(String groupid,String month,String broadband_type);
	
//----------------------------------------------------------------------------------------------------------	
	/**
	 * 下面6个接口为查询测试报告中获取探针类型维度数据需要
	 * @param groupid
	 * @param month
	 * @param broadband_type
	 * @return
	 */
	List<Map<String, Object>> getdelaywebdata(String groupid,String month,String broadband_type);
	List<Map<String, Object>> getdelayvideodata(String groupid,String month,String broadband_type);
	List<Map<String, Object>> getratewebdata(String groupid,String month,String broadband_type);
	List<Map<String, Object>> getproportionvideodata(String groupid,String month,String broadband_type);
	List<Map<String, Object>> getdelayninetywebdata(String groupid,String month,String broadband_type);
	List<Map<String, Object>> getcachevideodata(String groupid,String month,String broadband_type);
	List<Map<String, Object>> getuploadhttpdata(String groupid,String month,String broadband_type);
	List<Map<String, Object>> getdownloadhttpdata(String groupid,String month,String broadband_type);
	
	/**
	 * 根据groupid获取有效样本
	 * @param month
	 * @param groupid
	 * @return
	 * @return String
	 */
	String getValidSampleNum(String month,String groupid,String broadType);
	
	
	
	/**
	 * 获取质量分析 即 上下月中数据的增减情况
	 * @param thismonth
	 * @param premonth
	 * @param groupid
	 * @return
	 * @return List<Map<String,Object>>
	 */
	List<Map<String, Object>> getServiceQualityCompare(String probetype ,String fieldName, String tableName ,String dataq,String datatype, String thismonth,String premonth,String groupid,String broadband_type);
	List<Map<String, Object>> getHttpdownloadServiceQualityCompare(String thismonth,String premonth,String groupid,String broadband_type);
	List<Map<String, Object>> getVideoServiceQualityCompare(String thismonth,String premonth,String groupid,String broadband_type);
	List<Map<String, Object>> getWebbrowsingServiceQualityCompare(String thismonth,String premonth,String groupid,String broadband_type);
	/*List<Map<String, Object>> getHttpServiceQualityCompare(String thismonth,String premonth,String groupid,String broadband_type);
	List<Map<String, Object>> getHttpServiceQualityCompare(String thismonth,String premonth,String groupid,String broadband_type);*/
	
	
	
	/**
	 * 获取本期（当月）最佳的结果数据
	 * @param month
	 * @param groupid
	 * @return
	 * @return String
	 */
	List<Map<String, Object>>  getBestResult(String probetype ,String fieldName, String tableName ,String dataq,String datatype, String month,String broadType);

	
	/**
	 * 获取本省的结果数据在全国的排名
	 * @param month
	 * @param groupid
	 * @return
	 * @return String
	 */
	String getResultOrder(String probetype ,String fieldName, String tableName ,String dataq,String datatype,String month,String groupid,String broadType);
	
	/**
	 * 获取本省的结果数据
	 * @param month
	 * @param groupid
	 * @return
	 * @return String
	 */
	List<Map<String, Object>> getProvinceResult(String probetype ,String fieldName, String tableName ,String dataq,String datatype,String month,String groupid,String broadType);
	
	/**
	 * 根据groupid个数判断返回的是全国还是省份名称
	 * @param groupid
	 * @return
	 */
	List<Map<String, Object>> getProvinceName(String groupid);
}
