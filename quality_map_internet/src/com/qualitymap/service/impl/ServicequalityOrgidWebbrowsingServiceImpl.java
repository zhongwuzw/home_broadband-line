package com.qualitymap.service.impl;

import java.text.DecimalFormat;
import java.text.Format;
import java.util.List;
import java.util.Map;

import javax.annotation.Resource;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import com.qualitymap.dao.ServicequalityOrgidWebbrowsingDao;
import com.qualitymap.service.ServicequalityOrgidWebbrowsingService;

public class ServicequalityOrgidWebbrowsingServiceImpl implements ServicequalityOrgidWebbrowsingService{

	@Resource
	ServicequalityOrgidWebbrowsingDao orgidWebbrowsingDao;
	static Format fm = new DecimalFormat("#.##");
	/**
	 * 获取宽带品质监测报告详情
	 */
	@Override
	public String getPingReportItem(String groupid, String month, String broadband_type) {
		JSONObject dataJSON = new JSONObject();
		JSONArray dataarray = new JSONArray();
		if (!"".equals(groupid)) {

			List<Map<String, Object>> queryList = orgidWebbrowsingDao.getPingReportItem(groupid, month, broadband_type);
			dataarray = JSONArray.fromObject(queryList);
		}
		dataJSON.put("data", dataarray);
		return dataJSON.toString();
	}
	/**
	 * 获取平台list
	 */
	@Override
	public String getCityList(String month, String groupid,String broadType) {
		JSONObject dataJSON = new JSONObject();
		JSONArray dataarray = new JSONArray();
		if (!"".equals(groupid)) {
			List<Map<String, Object>> queryList = orgidWebbrowsingDao.getCityList(month, groupid,broadType);

			dataarray = JSONArray.fromObject(queryList);
		}
		dataJSON.put("data", dataarray);
		return dataJSON.toString();
	}
	
	/**
	 * 根据groupid获取有效样本
	 */
	@Override
	public String getValidSampleNum(String month, String groupid,String broadType) {

		JSONObject dataJSON = new JSONObject();

		if (!"".equals(groupid)) {
			String ping_test_times = orgidWebbrowsingDao.getValidSampleNum(month, groupid,broadType);
			dataJSON.put("page_test_times", ping_test_times);
		} else {
			dataJSON.put("page_test_times", "");
		}
		return dataJSON.toString();
	}
	
	/**
	 * 根据groupid获取账户数(报告中的地区字段)
	 */
	@Override
	public String getOrgnumByGroupId(String month, String groupid,String broadType) {

		JSONObject dataJSON = new JSONObject();

		if (!"".equals(groupid)) {
			String org_num = orgidWebbrowsingDao.getOrgnumByGroupId(month, groupid,broadType);
			dataJSON.put("org_num", org_num);
		} else {
			dataJSON.put("org_num", "");
		}
		return dataJSON.toString();
	}
	
}
