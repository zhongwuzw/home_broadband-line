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

	List<OverviewProvince> findProvice(String groupid);

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
}
