package com.qualitymap.service.impl;

import java.text.DecimalFormat;
import java.text.Format;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.annotation.Resource;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import com.qualitymap.dao.ServicequalityGroupidVideoDao;
import com.qualitymap.service.ServicequalityGroupidVideoService;
import com.qualitymap.utils.UtilDate;

public class ServicequalityGroupidVideoServiceImpl implements ServicequalityGroupidVideoService {

	@Resource
	ServicequalityGroupidVideoDao groupidVideoDao;
	static Format fm = new DecimalFormat("#.##");

	/**
	 * 获取上下月的平均时延
	 */
	@Override
	public String getVideoDelay(String yearMonth, String groupid) {
		String lastMonth = UtilDate.getPreviousMonth(yearMonth);

		JSONObject avgJson = new JSONObject();

		List<Map<String, Object>> avgDelayList = new ArrayList<Map<String, Object>>();

		if (!"".equals(groupid)) {
			avgDelayList = groupidVideoDao.getVideoDelay(yearMonth, lastMonth, groupid);
		}
		avgJson.put("data", avgDelayList);

		return avgJson.toString();
	}

	/**
	 * 视频卡顿次数上下月数据
	 * 
	 */
	@Override
	public String getCacheCount(String yearMonth, String groupid) {
		String lastMonth = UtilDate.getPreviousMonth(yearMonth);

		JSONObject avgJson = new JSONObject();
		List<Map<String, Object>> avgDelayList = new ArrayList<Map<String, Object>>();
		if (!"".equals(groupid)) {
			avgDelayList = groupidVideoDao.getCacheCount(yearMonth, lastMonth, groupid);
		}
		avgJson.put("data", avgDelayList);

		return avgJson.toString();
	}

	/**
	 * 获取视频播放成功率趋势数据
	 */
	@Override
	public String getVideoPlaySuccessData(String groupid, String probetype) {
		// TODO Auto-generated method stub
		JSONObject dataJSON = new JSONObject();
		JSONArray avgArray = new JSONArray();
		JSONArray monthArray = new JSONArray();
		if (!"".equals(groupid)) {

			List<Map<String, Object>> queryList = groupidVideoDao.getVideoPlaySuccessData(groupid, probetype);

			for (Iterator iterator = queryList.iterator(); iterator.hasNext();) {
				Map<String, Object> map = (Map<String, Object>) iterator.next();
				double page_success_rate = Double.parseDouble(map.get("success_rate").toString());
				String month = map.get("month").toString();
				avgArray.add(fm.format(page_success_rate));
				monthArray.add(month);
			}
		}

		dataJSON.put("lable", monthArray);
		dataJSON.put("data", avgArray);

		return dataJSON.toString();
	}

	/**
	 * 获取视频播放成功率的排名
	 */
	@Override
	public String getVideoPlaySuccessOrder(String thismonth, String groupid, String probetype) {

		JSONObject dataJSON = new JSONObject();

		JSONArray dataArray = new JSONArray();

		String prmonth = UtilDate.getPreviousMonth(thismonth);
		if (!"".equals(groupid)) {
			List<Map<String, Object>> monList = groupidVideoDao.getVideoPlaySuccessOrder(thismonth, prmonth, groupid, probetype);

			List<Map<String, Object>> mapList = new ArrayList<Map<String, Object>>();

			String percent = "";

			for (Iterator iterator = monList.iterator(); iterator.hasNext();) {
				Map<String, Object> map = (Map<String, Object>) iterator.next();
				Map datamap = new HashMap();

				String province = (String) map.get("groupname");
				String pagesuccess = "";
				if (map.get("success_rate") != null) {
					pagesuccess = map.get("success_rate").toString();
				}else{
					pagesuccess = "0";
				}

				String pre_pagesuccess = "";

				if (map.get("pre_success_rate") != null) {
					pre_pagesuccess = map.get("pre_success_rate").toString();
				}

				if (pre_pagesuccess != null && !pre_pagesuccess.isEmpty()&&!"999999".equals(pagesuccess)) {
					double difValue = Double.valueOf(pagesuccess) - Double.valueOf(pre_pagesuccess);
					percent = fm.format(difValue / Double.valueOf(pre_pagesuccess) * 100);
				} else {
					percent = "N/A";
				}
				
				String thisdata = "";
				if(!"999999".equals(pagesuccess)){
					 thisdata= fm.format(Double.valueOf(pagesuccess));
				}else{
					thisdata = "N/A";
				}

				datamap.put("province", province);
				datamap.put("thisdata", thisdata);
				if (pre_pagesuccess != null && !pre_pagesuccess.isEmpty()) {
					datamap.put("predata", fm.format(Double.valueOf(pre_pagesuccess)));
				} else {
					datamap.put("predata", "N/A");
				}
				datamap.put("percent", percent);

				mapList.add(datamap);
			}

			dataArray = JSONArray.fromObject(mapList);
		}

		dataJSON.put("data", dataArray);

		return dataJSON.toString();
	}
	
	/**
	 * 获取视频加载时长趋势数据
	 */
	@Override
	public String getVideoDelayData(String groupid, String probetype) {
		// TODO Auto-generated method stub
		JSONObject dataJSON = new JSONObject();
		JSONArray avgArray = new JSONArray();
		JSONArray monthArray = new JSONArray();
		if (!"".equals(groupid)) {

			List<Map<String, Object>> queryList = groupidVideoDao.getVideoDelayData(groupid, probetype);

			for (Iterator iterator = queryList.iterator(); iterator.hasNext();) {
				Map<String, Object> map = (Map<String, Object>) iterator.next();
				double page_success_rate = Double.parseDouble(map.get("avg_video_delay").toString());
				String month = map.get("month").toString();
				avgArray.add(fm.format(page_success_rate));
				monthArray.add(month);
			}
		}

		dataJSON.put("lable", monthArray);
		dataJSON.put("data", avgArray);

		return dataJSON.toString();
	}

	/**
	 * 获取视频加载时长的排名
	 */
	@Override
	public String getVideoDelayOrder(String thismonth, String groupid, String probetype) {

		JSONObject dataJSON = new JSONObject();

		JSONArray dataArray = new JSONArray();

		String prmonth = UtilDate.getPreviousMonth(thismonth);
		if (!"".equals(groupid)) {
			List<Map<String, Object>> monList = groupidVideoDao.getVideoDelayOrder(thismonth, prmonth, groupid, probetype);

			List<Map<String, Object>> mapList = new ArrayList<Map<String, Object>>();

			String percent = "";

			for (Iterator iterator = monList.iterator(); iterator.hasNext();) {
				Map<String, Object> map = (Map<String, Object>) iterator.next();
				Map datamap = new HashMap();

				String province = (String) map.get("groupname");
				String pagesuccess = "";
				if (map.get("avg_video_delay") != null) {
					pagesuccess = map.get("avg_video_delay").toString();
				}else{
					pagesuccess = "0";
				}

				String pre_pagesuccess = "";

				if (map.get("pre_avg_video_delay") != null) {
					pre_pagesuccess = map.get("pre_avg_video_delay").toString();
				}

				if (pre_pagesuccess != null && !pre_pagesuccess.isEmpty()&&!"999999".equals(pagesuccess)) {
					double difValue = Double.valueOf(pagesuccess) - Double.valueOf(pre_pagesuccess);
					percent = fm.format(difValue / Double.valueOf(pre_pagesuccess) * 100);
				} else {
					percent = "N/A";
				}
				
				String thisdata = "";
				if(!"999999".equals(pagesuccess)){
					 thisdata= fm.format(Double.valueOf(pagesuccess));
				}else{
					thisdata = "N/A";
				}

				datamap.put("province", province);
				datamap.put("thisdata", thisdata);
				if (pre_pagesuccess != null && !pre_pagesuccess.isEmpty()) {
					datamap.put("predata", fm.format(Double.valueOf(pre_pagesuccess)));
				} else {
					datamap.put("predata", "N/A");
				}
				datamap.put("percent", percent);

				mapList.add(datamap);
			}

			dataArray = JSONArray.fromObject(mapList);
		}

		dataJSON.put("data", dataArray);

		return dataJSON.toString();
	}
	/**
	 * 获取视频卡顿次数数据
	 */
	@Override
	public String getVideoCacheCountData(String groupid, String probetype) {
		// TODO Auto-generated method stub
		JSONObject dataJSON = new JSONObject();
		JSONArray avgArray = new JSONArray();
		JSONArray monthArray = new JSONArray();
		if (!"".equals(groupid)) {

			List<Map<String, Object>> queryList = groupidVideoDao.getVideoCacheCountData(groupid, probetype);

			for (Iterator iterator = queryList.iterator(); iterator.hasNext();) {
				Map<String, Object> map = (Map<String, Object>) iterator.next();
				double page_success_rate = Double.parseDouble(map.get("video_cache_count").toString());
				String month = map.get("month").toString();
				avgArray.add(fm.format(page_success_rate));
				monthArray.add(month);
			}
		}

		dataJSON.put("lable", monthArray);
		dataJSON.put("data", avgArray);

		return dataJSON.toString();
	}
	/**
	 * 获取视频卡顿次数的排名
	 */
	@Override
	public String getVideoCacheCountOrder(String thismonth, String groupid, String probetype) {

		JSONObject dataJSON = new JSONObject();

		JSONArray dataArray = new JSONArray();

		String prmonth = UtilDate.getPreviousMonth(thismonth);
		if (!"".equals(groupid)) {
			List<Map<String, Object>> monList = groupidVideoDao.getVideoCacheCountOrder(thismonth, prmonth, groupid, probetype);

			List<Map<String, Object>> mapList = new ArrayList<Map<String, Object>>();

			String percent = "";

			for (Iterator iterator = monList.iterator(); iterator.hasNext();) {
				Map<String, Object> map = (Map<String, Object>) iterator.next();
				Map datamap = new HashMap();

				String province = (String) map.get("groupname");
				String pagesuccess = "";
				if (map.get("video_cache_count") != null) {
					pagesuccess = map.get("video_cache_count").toString();
				}else{
					pagesuccess = "0";
				}

				String pre_pagesuccess = "";

				if (map.get("pre_video_cache_count") != null) {
					pre_pagesuccess = map.get("pre_video_cache_count").toString();
				}

				if (pre_pagesuccess != null && !pre_pagesuccess.isEmpty()) {
					double difValue = Double.valueOf(pagesuccess) - Double.valueOf(pre_pagesuccess);
					percent = fm.format(difValue / Double.valueOf(pre_pagesuccess) * 100);
				} else {
					percent = "N/A";
				}

				String thisdata = "";
				if(!"0".equals(pagesuccess)){
					 thisdata= fm.format(Double.valueOf(pagesuccess));
				}else{
					thisdata = "N/A";
				}

				datamap.put("province", province);
				datamap.put("thisdata", thisdata);
				if (pre_pagesuccess != null && !pre_pagesuccess.isEmpty()) {
					datamap.put("predata", fm.format(Double.valueOf(pre_pagesuccess)));
				} else {
					datamap.put("predata", "N/A");
				}
				datamap.put("percent", percent);

				mapList.add(datamap);
			}

			dataArray = JSONArray.fromObject(mapList);
		}

		dataJSON.put("data", dataArray);

		return dataJSON.toString();
	}

}
