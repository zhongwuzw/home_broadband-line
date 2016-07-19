package com.qualitymap.dao;

import java.util.List;
import java.util.Map;

import com.qualitymap.vo.OverviewKpiApi;

/**
 * 
 * @author：kxc
 * @date：Apr 8, 2016
 */
public interface OverviewKpiApiDao {
	void saveType(OverviewKpiApi kpi);

	void deletType(OverviewKpiApi kpi);

	List<OverviewKpiApi> find();

	void updateType(OverviewKpiApi kpi);
	
	/**
	 * 获取累计注册用户数及增长百分比
	 * @param month
	 * @param groupid
	 * @return
	 * @return String
	 */
	List<Map<String, Object>> getRegusernameNum(String premonth,String thismonth, String userName);
	/**
	 * 获取累计使用用户数及增长百分比
	 * @param month
	 * @param groupid
	 * @return
	 * @return String
	 */
	List<Map<String, Object>> getCustomersNum(String premonth,String thismonth, String userName);
}
