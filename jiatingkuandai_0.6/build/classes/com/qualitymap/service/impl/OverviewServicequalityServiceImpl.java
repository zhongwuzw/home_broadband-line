package com.qualitymap.service.impl;

import java.text.DecimalFormat;
import java.text.Format;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.annotation.Resource;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import com.qualitymap.dao.OverviewServicequalityDao;
import com.qualitymap.service.OverviewServicequalityService;
import com.qualitymap.utils.UtilDate;

/**
 * Description: OTS统计平台业务质量
 * 
 * @author zqh 2016-4-7: PM 04:15:11
 */
public class OverviewServicequalityServiceImpl implements OverviewServicequalityService {

	@Resource
	OverviewServicequalityDao overviewServicequalityDao;
	static Format fm = new DecimalFormat("#.##");

	@Override
	public String getAvgDelay(String yearMonth, String groupid) {
		String lastMonth = UtilDate.getPreviousMonth(yearMonth);

		JSONObject avgJson = new JSONObject();

		double monthAvgDelay = 0;
		double preAvgDelay = 0;

		if (!"".equals(groupid)) {
			List<Map<String, Object>> avgDelayList = overviewServicequalityDao.getAvgDelay(yearMonth, lastMonth, groupid);

			for (Iterator iterator = avgDelayList.iterator(); iterator.hasNext();) {
				Map<String, Object> map = (Map<String, Object>) iterator.next();

				if (yearMonth.equals(map.get("month"))) {
					monthAvgDelay = Double.parseDouble(map.get("avg_delay").toString());
				} else {
					preAvgDelay = Double.parseDouble(map.get("avg_delay").toString());
				}
			}
		}
		avgJson.put("thismonth", fm.format(monthAvgDelay));
		avgJson.put("lastmonth", fm.format(preAvgDelay));

		return avgJson.toString();
	}

	@Override
	public String getDelayStandardRate(String yearMonth, String groupid) {
		String lastMonth = UtilDate.getPreviousMonth(yearMonth);

		JSONObject avgJson = new JSONObject();
		double monthAvg = 0;
		double preAvg = 0;
		if (!"".equals(groupid)) {
			List<Map<String, Object>> avgDelayList = overviewServicequalityDao.getDelayStandardRate(yearMonth, lastMonth, groupid);

			for (Iterator iterator = avgDelayList.iterator(); iterator.hasNext();) {
				Map<String, Object> map = (Map<String, Object>) iterator.next();

				if (yearMonth.equals(map.get("month"))) {
					monthAvg = Double.parseDouble(map.get("delay_standard_rate").toString());
				} else {
					preAvg = Double.parseDouble(map.get("delay_standard_rate").toString());
				}
			}
		}
		avgJson.put("thismonth", fm.format(monthAvg));
		avgJson.put("lastmonth", fm.format(preAvg));

		return avgJson.toString();
	}

	@Override
	public String getAvgPageSuccessRate(String yearMonth, String groupid) {
		String lastMonth = UtilDate.getPreviousMonth(yearMonth);

		JSONObject avgJson = new JSONObject();
		double monthAvg = 0;
		double preAvg = 0;
		if (!"".equals(groupid)) {

			List<Map<String, Object>> avgDelayList = overviewServicequalityDao.getAvgPageSuccessRate(yearMonth, lastMonth, groupid);

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
		double monthAvg = 0;
		double preAvg = 0;

		if (!"".equals(groupid)) {

			List<Map<String, Object>> avgDelayList = overviewServicequalityDao.getPageStandardRate(yearMonth, lastMonth, groupid);

			for (Iterator iterator = avgDelayList.iterator(); iterator.hasNext();) {
				Map<String, Object> map = (Map<String, Object>) iterator.next();

				if (yearMonth.equals(map.get("month"))) {
					monthAvg = Double.parseDouble(map.get("page_standard_rate").toString());
				} else {
					preAvg = Double.parseDouble(map.get("page_standard_rate").toString());
				}
			}
		}
		avgJson.put("thismonth", fm.format(monthAvg));
		avgJson.put("lastmonth", fm.format(preAvg));

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
			List<Map<String, Object>> avgDelayList = overviewServicequalityDao.getTop90PageDelay(yearMonth, lastMonth, groupid);

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

			List<Map<String, Object>> avgDelayList = overviewServicequalityDao.getTop90PageSuccessRate(yearMonth, lastMonth, groupid);

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
	 * 平均ping时延(MS)
	 */
	@Override
	public String getAvgPingDelay(String yearMonth, String groupid) {
		String lastMonth = UtilDate.getPreviousMonth(yearMonth);

		JSONObject avgJson = new JSONObject();
		double monthAvg = 0;
		double preAvg = 0;

		if (!"".equals(groupid)) {

			List<Map<String, Object>> avgDelayList = overviewServicequalityDao.getAvgPingDelay(yearMonth, lastMonth, groupid);

			for (Iterator iterator = avgDelayList.iterator(); iterator.hasNext();) {
				Map<String, Object> map = (Map<String, Object>) iterator.next();

				if (yearMonth.equals(map.get("month"))) {
					monthAvg = Double.parseDouble(map.get("avg_ping_delay").toString());
				} else {
					preAvg = Double.parseDouble(map.get("avg_ping_delay").toString());
				}
			}
		}
		avgJson.put("thismonth", fm.format(monthAvg));
		avgJson.put("lastmonth", fm.format(preAvg));

		return avgJson.toString();
	}
	/**
	 * 90%用户ping时延
	 */
	@Override
	public String getTop90PingDelay(String yearMonth, String groupid) {
		String lastMonth = UtilDate.getPreviousMonth(yearMonth);

		JSONObject avgJson = new JSONObject();
		double monthAvg = 0;
		double preAvg = 0;

		if (!"".equals(groupid)) {

			List<Map<String, Object>> avgDelayList = overviewServicequalityDao.getTop90PingDelay(yearMonth, lastMonth, groupid);

			for (Iterator iterator = avgDelayList.iterator(); iterator.hasNext();) {
				Map<String, Object> map = (Map<String, Object>) iterator.next();

				if (yearMonth.equals(map.get("month"))) {
					monthAvg = Double.parseDouble(map.get("top90_ping_delay").toString());
				} else {
					preAvg = Double.parseDouble(map.get("top90_ping_delay").toString());
				}
			}
		}
		avgJson.put("thismonth", fm.format(monthAvg));
		avgJson.put("lastmonth", fm.format(preAvg));

		return avgJson.toString();
	}
	/**
	 * ping丢包率
	 */
	@Override
	public String getPingLossRate(String yearMonth, String groupid) {
		String lastMonth = UtilDate.getPreviousMonth(yearMonth);

		JSONObject avgJson = new JSONObject();
		double monthAvg = 0;
		double preAvg = 0;

		if (!"".equals(groupid)) {

			List<Map<String, Object>> avgDelayList = overviewServicequalityDao.getPingLossRate(yearMonth, lastMonth, groupid);

			for (Iterator iterator = avgDelayList.iterator(); iterator.hasNext();) {
				Map<String, Object> map = (Map<String, Object>) iterator.next();

				if (yearMonth.equals(map.get("month"))) {
					monthAvg = Double.parseDouble(map.get("ping_loss_rate").toString());
				} else {
					preAvg = Double.parseDouble(map.get("ping_loss_rate").toString());
				}
			}
		}
		avgJson.put("thismonth", fm.format(monthAvg));
		avgJson.put("lastmonth", fm.format(preAvg));

		return avgJson.toString();
	}
	/**
	 * 90%用户ping丢包率
	 */
	@Override
	public String getTop90PingLossRate(String yearMonth, String groupid) {
		String lastMonth = UtilDate.getPreviousMonth(yearMonth);

		JSONObject avgJson = new JSONObject();
		double monthAvg = 0;
		double preAvg = 0;

		if (!"".equals(groupid)) {

			List<Map<String, Object>> avgDelayList = overviewServicequalityDao.getTop90PingLossRate(yearMonth, lastMonth, groupid);

			for (Iterator iterator = avgDelayList.iterator(); iterator.hasNext();) {
				Map<String, Object> map = (Map<String, Object>) iterator.next();

				if (yearMonth.equals(map.get("month"))) {
					monthAvg = Double.parseDouble(map.get("top90_ping_loss_rate").toString());
				} else {
					preAvg = Double.parseDouble(map.get("top90_ping_loss_rate").toString());
				}
			}
		}
		avgJson.put("thismonth", fm.format(monthAvg));
		avgJson.put("lastmonth", fm.format(preAvg));

		return avgJson.toString();
	}
}