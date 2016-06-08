package com.qualitymap.dao;

import java.util.List;
import java.util.Map;

import com.qualitymap.service.OverviewServicequalityProvinceService;
import com.qualitymap.vo.OverviewServicequalityProvince;

/**
 * 业务质量数据统计
 * @author：kxc
 * @date：Apr 11, 2016
 */
public interface OverviewServicequalityProvinceDao {


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
	 * 获取页面元素打开成功率趋势数据
	 * @param groupid
	 * @return
	 * @return List<Map<String,Object>>
	 */
	List<Map<String, Object>> getPageSuccessData(String groupid);
	
	/**
	 * 获取页面元素打开成功率的排名
	 * @param thismonth
	 * @param premonth
	 * @param groupid
	 * @return
	 * @return List<Map<String,Object>>
	 */
	List<Map<String, Object>> getPageSuccessOrder(String thismonth,String premonth,String groupid);

	void saveQualityProvince(OverviewServicequalityProvince serqualityprovince);

	void deleteqQualityProvinc(OverviewServicequalityProvince serqualityprovince);

	List<OverviewServicequalityProvince> findQualityProvinc();

	void updateQualityProvinc(OverviewServicequalityProvince serqualityprovince);



}
