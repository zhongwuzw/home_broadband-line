package com.qualitymap.service.impl;

import java.text.DecimalFormat;
import java.text.Format;
import java.util.List;
import java.util.Map;

import javax.annotation.Resource;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import com.qualitymap.dao.ServicequalityOrgidPingDao;
import com.qualitymap.service.ServicequalityOrgidPingService;

public class ServicequalityOrgidPingServiceImpl implements ServicequalityOrgidPingService{

	@Resource
	ServicequalityOrgidPingDao servicequalityOrgidPingDao;
	static Format fm = new DecimalFormat("#.##");

	/**
	 * 获取宽带品质监测报告详情
	 */
	@Override
	public String getPingReportItem(String groupid, String month, String broadband_type) {
		JSONObject dataJSON = new JSONObject();
		JSONArray dataarray = new JSONArray();
		if (!"".equals(groupid)) {

			List<Map<String, Object>> queryList = servicequalityOrgidPingDao.getPingReportItem(groupid, month, broadband_type);
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
			List<Map<String, Object>> queryList = servicequalityOrgidPingDao.getCityList(month, groupid,broadType);

			dataarray = JSONArray.fromObject(queryList);
		}
		dataJSON.put("data", dataarray);
		return dataJSON.toString();
	}
	
	
	
	/**
	 * 根据groupid获取账户数(报告中的地区字段)
	 */
	@Override
	public String getOrgnumByGroupId(String month, String groupid,String broadType) {

		JSONObject dataJSON = new JSONObject();

		if (!"".equals(groupid)) {
			String org_num = servicequalityOrgidPingDao.getOrgnumByGroupId(month, groupid,broadType);
			dataJSON.put("org_num", org_num);
		} else {
			dataJSON.put("org_num", "");
		}
		return dataJSON.toString();
	}


}
