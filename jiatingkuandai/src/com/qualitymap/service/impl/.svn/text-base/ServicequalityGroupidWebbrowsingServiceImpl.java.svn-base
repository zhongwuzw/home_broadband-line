package com.qualitymap.service.impl;

import java.text.DecimalFormat;
import java.text.Format;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.annotation.Resource;

import org.apache.struts2.components.Else;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import com.qualitymap.dao.ServicequalityGroupidWebbrowsingDao;
import com.qualitymap.service.ServicequalityGroupidWebbrowsingService;
import com.qualitymap.utils.UtilDate;

public class ServicequalityGroupidWebbrowsingServiceImpl implements ServicequalityGroupidWebbrowsingService {

	@Resource
	ServicequalityGroupidWebbrowsingDao groupidWebbrowsingDao;
	static Format fm = new DecimalFormat("#.##");

	/**
	 * 质量分析上下月的数据增减值
	 */
	@Override
	public String getWebbrowsingServiceQualityCompare(String groupid, String month, String broadband_type) {
		String lastMonth = UtilDate.getPreviousMonth(month);

		JSONObject avgJson = new JSONObject();
		String avg_delay = "";
		String success_rate = "";

		if (!"".equals(groupid)) {
			List<Map<String, Object>> avgDelayList = groupidWebbrowsingDao.getWebbrowsingServiceQualityCompare(groupid, month, lastMonth, broadband_type);

			/*
			 * for (Iterator iterator = avgDelayList.iterator();
			 * iterator.hasNext();) { Map<String, Object> map = (Map<String,
			 * Object>) iterator.next();
			 * 
			 * String page_avg_delay = (String) map.get("page_avg_delay");
			 * String page_success_rate =(String) map.get("page_success_rate");
			 * 
			 * if(page_avg_delay.contains("-")){ avg_delay =
			 * "降低"+page_avg_delay; }else{ avg_delay = "提高"+page_avg_delay; }
			 * 
			 * if(page_success_rate.contains("-")){ success_rate =
			 * "降低"+page_success_rate; }else{ success_rate =
			 * "提高"+page_success_rate; } }
			 */

			if (avgDelayList.size() > 0) {
				Map<String, Object> map = avgDelayList.get(0);
				String page_avg_delay =  map.get("page_avg_delay").toString();
				String page_success_rate = map.get("page_success_rate").toString();

				if (page_avg_delay.contains("-")) {
					avg_delay = "减少" + Math.abs(Double.parseDouble(page_avg_delay));
				} else if(!page_avg_delay.contains("N/A")) {
					avg_delay = "增加" + page_avg_delay;
				}else{
					avg_delay = page_avg_delay;
				}

				if (page_success_rate.contains("-")) {
					success_rate = "减少" + Math.abs(Double.parseDouble(page_success_rate));
				} else if(!page_success_rate.contains("N/A")){
					success_rate = "增加" + page_success_rate;
				}else {
					success_rate = page_success_rate;
				}
			}
		}
		avgJson.put("page_avg_delay", avg_delay);
		avgJson.put("page_success_rate", success_rate);
		return avgJson.toString();
	}

	@Override
	public String getAvgDelay(String yearMonth, String groupid) {
		String lastMonth = UtilDate.getPreviousMonth(yearMonth);

		JSONObject avgJson = new JSONObject();

		List<Map<String, Object>> avgDelayList = new ArrayList<Map<String,Object>>();

		if (!"".equals(groupid)) {
			 avgDelayList = groupidWebbrowsingDao.getAvgDelay(yearMonth, lastMonth, groupid);
		}	
		avgJson.put("data",avgDelayList);

		return avgJson.toString();
	}

	/**
	 * 获取本月上月的网页浏览成功率
	 */
	@Override
	public String getPageBrowseSuccess(String yearMonth, String groupid) {
		String lastMonth = UtilDate.getPreviousMonth(yearMonth);

		JSONObject avgJson = new JSONObject();

		List<Map<String, Object>> avgDelayList = new ArrayList<Map<String,Object>>();

		if (!"".equals(groupid)) {
			 avgDelayList = groupidWebbrowsingDao.getPageBrowseSuccess(yearMonth, lastMonth, groupid);
		}	
		avgJson.put("data",avgDelayList);

		return avgJson.toString();
	}

	/**
	 * 获取页面浏览访问趋势
	 */
	@Override
	public String getPageBrowseSuccessData(String groupid,String probetype) {
		// TODO Auto-generated method stub
		JSONObject dataJSON = new JSONObject();
		JSONArray avgArray = new JSONArray();
		JSONArray monthArray = new JSONArray();
		if (!"".equals(groupid)) {

			List<Map<String, Object>> queryList = groupidWebbrowsingDao.getPageBrowseSuccessData(groupid,probetype);

			for (Iterator iterator = queryList.iterator(); iterator.hasNext();) {
				Map<String, Object> map = (Map<String, Object>) iterator.next();
				double page_success_rate = Double.parseDouble(map.get("success_rate").toString())*100;
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
	 * 获取页面浏览访问排名
	 */
	@Override
	public String getPageBrowseSuccessOrder(String month, String groupid,String probetype) {

		JSONObject dataJSON = new JSONObject();

		JSONArray dataArray = new JSONArray();

		String prmonth = UtilDate.getPreviousMonth(month);
		if (!"".equals(groupid)) {
			List<Map<String, Object>> monList = groupidWebbrowsingDao.getPageBrowseSuccessOrder(month, prmonth, groupid, probetype);

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

				if (pre_pagesuccess != null && !pre_pagesuccess.isEmpty()) {
					double difValue = Double.valueOf(pagesuccess) - Double.valueOf(pre_pagesuccess);
					percent = fm.format(difValue / Double.valueOf(pre_pagesuccess) * 100);
				} else {
					percent = "N/A";
				}

				String thisdata = "";
				if(!"0".equals(pagesuccess)){
					 thisdata= fm.format(Double.valueOf(pagesuccess)*100);
				}else{
					thisdata = "N/A";
				}

				datamap.put("province", province);
				datamap.put("thisdata", thisdata);
				
				if (pre_pagesuccess != null && !pre_pagesuccess.isEmpty()) {
					datamap.put("predata", fm.format(Double.valueOf(pre_pagesuccess)*100));
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
	 * 获取90%页面时延(ms)上下月数据
	 */
	@Override
	public String getPageAvgNinetyDelay(String yearMonth, String groupid) {
		String lastMonth = UtilDate.getPreviousMonth(yearMonth);

		JSONObject avgJson = new JSONObject();

		List<Map<String, Object>> avgDelayList = new ArrayList<Map<String,Object>>();

		if (!"".equals(groupid)) {
			 avgDelayList = groupidWebbrowsingDao.getPageAvgNinetyDelay(yearMonth, lastMonth, groupid);
		}	
		avgJson.put("data",avgDelayList);

		return avgJson.toString();
	}

	/**
	 * 获取页面显示时延趋势
	 */
	@Override
	public String getPageSuccessData(String groupid,String probetype) {
		// TODO Auto-generated method stub
		JSONObject dataJSON = new JSONObject();
		JSONArray avgArray = new JSONArray();
		JSONArray monthArray = new JSONArray();
		if (!"".equals(groupid)) {

			List<Map<String, Object>> queryList = groupidWebbrowsingDao.getPageSuccessData(groupid,probetype);

			for (Iterator iterator = queryList.iterator(); iterator.hasNext();) {
				Map<String, Object> map = (Map<String, Object>) iterator.next();
				double page_success_rate = Double.parseDouble(map.get("page_success_rate").toString());
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
	 * 获取页面显示时延排名
	 */
	@Override
	public String getPageSuccessOrder(String month, String groupid,String probetype) {

		JSONObject dataJSON = new JSONObject();

		JSONArray dataArray = new JSONArray();

		String prmonth = UtilDate.getPreviousMonth(month);
		if (!"".equals(groupid)) {
			List<Map<String, Object>> monList = groupidWebbrowsingDao.getPageSuccessOrder(month, prmonth, groupid, probetype);

			List<Map<String, Object>> mapList = new ArrayList<Map<String, Object>>();

			String percent = "";

			for (Iterator iterator = monList.iterator(); iterator.hasNext();) {
				Map<String, Object> map = (Map<String, Object>) iterator.next();
				Map datamap = new HashMap();

				String province = (String) map.get("groupname");
				String pagesuccess = "";
				if (map.get("page_success_rate") != null) {
					pagesuccess = map.get("page_success_rate").toString();
				}else{
					pagesuccess = "0";
				}

				String pre_pagesuccess = "";

				if (map.get("pre_page_success_rate") != null) {
					pre_pagesuccess = map.get("pre_page_success_rate").toString();
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

	@Override
	public String getAvgPageSuccessRate(String yearMonth, String groupid) {
		String lastMonth = UtilDate.getPreviousMonth(yearMonth);

		JSONObject avgJson = new JSONObject();
		double monthAvg = 0;
		double preAvg = 0;
		if (!"".equals(groupid)) {

			List<Map<String, Object>> avgDelayList = groupidWebbrowsingDao.getAvgPageSuccessRate(yearMonth, lastMonth, groupid);

			for (Iterator iterator = avgDelayList.iterator(); iterator.hasNext();) {
				Map<String, Object> map = (Map<String, Object>) iterator.next();

				if (yearMonth.equals(map.get("month"))) {
					monthAvg = Double.parseDouble(map.get("avg_page_success_rate").toString());
				} else {
					preAvg = Double.parseDouble(map.get("avg_page_success_rate").toString());
				}
			}
		}
		avgJson.put("thismonth", fm.format(monthAvg));
		avgJson.put("lastmonth", fm.format(preAvg));

		return avgJson.toString();
	}

	@Override
	public String getPageStandardRate(String yearMonth, String groupid) {
		String lastMonth = UtilDate.getPreviousMonth(yearMonth);

		JSONObject avgJson = new JSONObject();
		List<Map<String, Object>> avgDelayList = new ArrayList<Map<String,Object>>();
		if (!"".equals(groupid)) {
			avgDelayList = groupidWebbrowsingDao.getPageStandardRate(yearMonth, lastMonth, groupid);
		}
		avgJson.put("data", avgDelayList);

		return avgJson.toString();
	}

	/**
	 * 90%用户页面时延
	 */
	@Override
	public String getTop90PageDelay(String yearMonth, String groupid) {
		String lastMonth = UtilDate.getPreviousMonth(yearMonth);

		JSONObject avgJson = new JSONObject();
		double monthAvg = 0;
		double preAvg = 0;
		if (!"".equals(groupid)) {
			List<Map<String, Object>> avgDelayList = groupidWebbrowsingDao.getTop90PageDelay(yearMonth, lastMonth, groupid);

			for (Iterator iterator = avgDelayList.iterator(); iterator.hasNext();) {
				Map<String, Object> map = (Map<String, Object>) iterator.next();

				if (yearMonth.equals(map.get("month"))) {
					monthAvg = Double.parseDouble(map.get("top90_page_delay").toString());
				} else {
					preAvg = Double.parseDouble(map.get("top90_page_delay").toString());
				}
			}
		}
		avgJson.put("thismonth", fm.format(monthAvg));
		avgJson.put("lastmonth", fm.format(preAvg));

		return avgJson.toString();
	}

	/**
	 * 90%用户页面元素打开成功率
	 */
	@Override
	public String getTop90PageSuccessRate(String yearMonth, String groupid) {
		String lastMonth = UtilDate.getPreviousMonth(yearMonth);

		JSONObject avgJson = new JSONObject();
		double monthAvg = 0;
		double preAvg = 0;

		if (!"".equals(groupid)) {

			List<Map<String, Object>> avgDelayList = groupidWebbrowsingDao.getTop90PageSuccessRate(yearMonth, lastMonth, groupid);

			for (Iterator iterator = avgDelayList.iterator(); iterator.hasNext();) {
				Map<String, Object> map = (Map<String, Object>) iterator.next();

				if (yearMonth.equals(map.get("month"))) {
					monthAvg = Double.parseDouble(map.get("top90_page_success_rate").toString());
				} else {
					preAvg = Double.parseDouble(map.get("top90_page_success_rate").toString());
				}
			}
		}
		avgJson.put("thismonth", fm.format(monthAvg));
		avgJson.put("lastmonth", fm.format(preAvg));

		return avgJson.toString();
	}

	/**
	 * 获取本月指标情况
	 */
	@Override
	public String getWebbrowsingKPIbyGroupid(String groupid, String month, String broadband_type) {

		JSONObject avgJson = new JSONObject();
		Map<String, Object> provinceMap = new HashMap<String, Object>();
		Map<String, Object> bestResultMap = new HashMap<String, Object>();

		if (!"".equals(groupid)) {

			List<Map<String, Object>> provinceResList = groupidWebbrowsingDao.getProvinceResult(month, groupid, broadband_type);
			if (provinceResList.size() > 0) {
				provinceMap = provinceResList.get(0);

			}
			List<Map<String, Object>> bestResList = groupidWebbrowsingDao.getBestResult(month, broadband_type);
			if (bestResList.size() > 0) {
				bestResultMap = bestResList.get(0);
			}
			provinceMap.putAll(bestResultMap);
			String delay_order = groupidWebbrowsingDao.getDelayResultOrder(month, groupid, broadband_type);
			String rate_order = groupidWebbrowsingDao.getSuccessRateResultOrder(month, groupid, broadband_type);

			provinceMap.put("delay_order", delay_order);

			provinceMap.put("rate_order", rate_order);
		}
		avgJson.put("data", provinceMap);

		return avgJson.toString();
	}
	/**
	 * 获取页面显示时延趋势数据
	 */
	@Override
	public String getPageDelayData(String groupid, String probetype) {
		// TODO Auto-generated method stub
		JSONObject dataJSON = new JSONObject();
		JSONArray avgArray = new JSONArray();
		JSONArray monthArray = new JSONArray();
		if (!"".equals(groupid)) {

			List<Map<String, Object>> queryList = groupidWebbrowsingDao.getPageDelayData(groupid, probetype);

			for (Iterator iterator = queryList.iterator(); iterator.hasNext();) {
				Map<String, Object> map = (Map<String, Object>) iterator.next();
				double page_avg_delay = Double.parseDouble(map.get("page_avg_delay").toString());
				String month = map.get("month").toString();
				avgArray.add(fm.format(page_avg_delay));
				monthArray.add(month);
			}
		}

		dataJSON.put("lable", monthArray);
		dataJSON.put("data", avgArray);

		return dataJSON.toString();
	}
	
	
	/**
	 * 获取页面显示时延的排名
	 */
	@Override
	public String getPageDelayOrder(String month, String groupid, String probetype) {

		JSONObject dataJSON = new JSONObject();

		JSONArray dataArray = new JSONArray();

		String prmonth = UtilDate.getPreviousMonth(month);
		if (!"".equals(groupid)) {
			List<Map<String, Object>> monList = groupidWebbrowsingDao.getPageDelayOrder(month, prmonth, groupid, probetype);

			List<Map<String, Object>> mapList = new ArrayList<Map<String, Object>>();

			String percent = "";

			for (Iterator iterator = monList.iterator(); iterator.hasNext();) {
				Map<String, Object> map = (Map<String, Object>) iterator.next();
				Map datamap = new HashMap();

				String province = (String) map.get("groupname");
				String pagesuccess = "";
				if (map.get("page_avg_delay") != null) {
					pagesuccess = map.get("page_avg_delay").toString();
				}else {
					pagesuccess = "0";
				}

				String pre_pagesuccess = "";

				if (map.get("pre_page_avg_delay") != null) {
					pre_pagesuccess = map.get("pre_page_avg_delay").toString();
				}
				if (pre_pagesuccess != null && !pre_pagesuccess.isEmpty()&&!"999999".equals(pagesuccess)) {
					double difValue = Double.valueOf(pagesuccess) - Double.valueOf(pre_pagesuccess);
					percent = fm.format(difValue / Double.valueOf(pre_pagesuccess) * 100);
				} else {
					percent = "N/A";
				}
				String page = "";
				if(!"999999".equals(pagesuccess)){
					 page= fm.format(Double.valueOf(pagesuccess));
				}else{
					page = "N/A";
				}
				datamap.put("province", province);
				datamap.put("thisdata",page );
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
	 * 全国家庭宽带网页浏览平均90%页面访问时延趋势
	 * 2016.6.16 zqh
	 */
	@Override
	public String getPageNinetyDelayData(String groupid, String probetype) {
		// TODO Auto-generated method stub
		JSONObject dataJSON = new JSONObject();
		JSONArray avgArray = new JSONArray();
		JSONArray monthArray = new JSONArray();
		if (!"".equals(groupid)) {

			List<Map<String, Object>> queryList = groupidWebbrowsingDao.getPageNinetyDelayData(groupid, probetype);

			for (Iterator iterator = queryList.iterator(); iterator.hasNext();) {
				Map<String, Object> map = (Map<String, Object>) iterator.next();
				double page_avg_delay = Double.parseDouble(map.get("page_avg_ninetydelay").toString());
				String month = map.get("month").toString();
				avgArray.add(fm.format(page_avg_delay));
				monthArray.add(month);
			}
		}

		dataJSON.put("lable", monthArray);
		dataJSON.put("data", avgArray);

		return dataJSON.toString();
	}
	
	
	/**
	 * 全国家庭宽带网页浏览平均90%页面访问时延排名
	 * 2016.6.16 zqh
	 */
	@Override
	public String getPageNinetyDelayOrder(String month, String groupid, String probetype) {

		JSONObject dataJSON = new JSONObject();

		JSONArray dataArray = new JSONArray();

		String prmonth = UtilDate.getPreviousMonth(month);
		if (!"".equals(groupid)) {
			List<Map<String, Object>> monList = groupidWebbrowsingDao.getPageNinetyDelayOrder(month, prmonth, groupid, probetype);

			List<Map<String, Object>> mapList = new ArrayList<Map<String, Object>>();

			String percent = "";

			for (Iterator iterator = monList.iterator(); iterator.hasNext();) {
				Map<String, Object> map = (Map<String, Object>) iterator.next();
				Map datamap = new HashMap();

				String province = (String) map.get("groupname");
				String pagesuccess = "";
				if (map.get("page_avg_ninetydelay") != null) {
					pagesuccess = map.get("page_avg_ninetydelay").toString();
				}else {
					pagesuccess = "0";
				}

				String pre_pagesuccess = "";

				if (map.get("pre_page_avg_ninetydelay") != null) {
					pre_pagesuccess = map.get("pre_page_avg_ninetydelay").toString();
				}
				if (pre_pagesuccess != null && !pre_pagesuccess.isEmpty()&&!"999999".equals(pagesuccess) && !("0").equals(pre_pagesuccess)) {
					double difValue = Double.valueOf(pagesuccess) - Double.valueOf(pre_pagesuccess);
					percent = fm.format(difValue / Double.valueOf(pre_pagesuccess) * 100);
				} else {
					percent = "N/A";
				}
				String page = "";
				if(!"999999".equals(pagesuccess)){
					 page= fm.format(Double.valueOf(pagesuccess));
				}else{
					page = "N/A";
				}
				datamap.put("province", province);
				datamap.put("thisdata",page );
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
