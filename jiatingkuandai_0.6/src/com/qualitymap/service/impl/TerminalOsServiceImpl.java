package com.qualitymap.service.impl;

import java.util.List;
import java.util.Map;

import javax.annotation.Resource;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import com.qualitymap.dao.TerminalOsDao;
import com.qualitymap.service.TerminalOsService;

/**
 * 终端分析
 * 
 * @author：kxc
 * @date：Apr 8, 2016
 */
public class TerminalOsServiceImpl implements TerminalOsService {

	@Resource
	private TerminalOsDao osDao;

	/**
	 * 获取特定月份平台测试次数占比
	 */
	@Override
	public String getPlatformPercent(String month, String groupid) {
		JSONObject dataJSON = new JSONObject();
		JSONArray dataarray = new JSONArray();
		if (!"".equals(groupid)) {

			List<Map<String, Object>> queryList = osDao.getPlatformPercent(month, groupid);
			dataarray = JSONArray.fromObject(queryList);
		}
		dataJSON.put("data", dataarray);
		return dataJSON.toString();
	}

	/**
	 * 按平台类型获取
	 */
	@Override
	public String getPlatData(String month, String groupid) {
		// TODO Auto-generated method stub
		JSONObject dataJSON = new JSONObject();
		JSONArray dataarray = new JSONArray();
		if (!"".equals(groupid)) {

			List<Map<String, Object>> queryList = osDao.getPlatData(month, groupid);

			dataarray = JSONArray.fromObject(queryList);
		}
		dataJSON.put("data", dataarray);
		return dataJSON.toString();
	}

	/**
	 * 获取平台分布数据
	 */
	@Override
	public String getPlatformDistribution(String month, String groupid) {
		JSONObject dataJSON = new JSONObject();
		JSONArray dataarray = new JSONArray();
		if (!"".equals(groupid)) {
			List<Map<String, Object>> queryList = osDao.getPlatformDistribution(month, groupid);

			dataarray = JSONArray.fromObject(queryList);
		}
		dataJSON.put("data", dataarray);
		return dataJSON.toString();
	}

	/**
	 * huoq获取平台数据详细信息
	 */
	@Override
	public String getPlatformDetailData(String month,String groupid) {
		JSONObject dataJSON = new JSONObject();
		JSONArray dataarray = new JSONArray();
		if (!"".equals(groupid)) {
			List<Map<String, Object>> queryList = osDao.getPlatformDetailData(month,groupid);

			dataarray = JSONArray.fromObject(queryList);
		}
		dataJSON.put("data", dataarray);
		return dataJSON.toString();
	}

	/**
	 * 获取平台list
	 */
	@Override
	public String getPlatformList(String month, String groupid) {
		JSONObject dataJSON = new JSONObject();
		JSONArray dataarray = new JSONArray();
		if (!"".equals(groupid)) {
			List<Map<String, Object>> queryList = osDao.getPlatformList(month, groupid);

			dataarray = JSONArray.fromObject(queryList);
		}
		dataJSON.put("data", dataarray);
		return dataJSON.toString();
	}
	
}
