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

import com.qualitymap.dao.ServicequalityGroupidPingDao;
import com.qualitymap.service.ServicequalityGroupidPingService;
import com.qualitymap.utils.UtilDate;

public class ServicequalityGroupidPingServiceImpl implements ServicequalityGroupidPingService {

	@Resource
	ServicequalityGroupidPingDao groupidPingDao;
	static Format fm = new DecimalFormat("#.##");

	/**
	 * 获取平均时延趋势
	 */
	@Override
	public String getAvgDelayDatas(String groupid) {
		// TODO Auto-generated method stub
		JSONObject dataJSON = new JSONObject();

		JSONArray avgArray = new JSONArray();
		JSONArray monthArray = new JSONArray();

		if (!"".equals(groupid)) {

			List<Map<String, Object>> queryList = groupidPingDao.getAvgDelayData(groupid);
			if (queryList.size() > 0) {

				for (Iterator iterator = queryList.iterator(); iterator.hasNext();) {
					Map<String, Object> map = (Map<String, Object>) iterator.next();
					double avg_delay = Double.parseDouble(map.get("avg_delay").toString());
					String month = map.get("month").toString();
					avgArray.add(fm.format(avg_delay));
					monthArray.add(month);
				}
			}
		}

		dataJSON.put("lable", monthArray);
		dataJSON.put("data", avgArray);
		return dataJSON.toString();
	}

	/**
	 * 获取平均时延排名
	 */
	@Override
	public String getAvgdelayOrder(String month, String groupid) {

		JSONObject dataJSON = new JSONObject();

		JSONArray dataArray = new JSONArray();

		String prmonth = UtilDate.getPreviousMonth(month);

		if (!"".equals(groupid)) {
			List<Map<String, Object>> monList = groupidPingDao.getAvgDelayOrder(month, prmonth, groupid);

			List<Map<String, Object>> mapList = new ArrayList<Map<String, Object>>();

			double previousNum = 0;
			double toNum = 0;
			String percent = "";

			for (Iterator iterator = monList.iterator(); iterator.hasNext();) {
				Map<String, Object> map = (Map<String, Object>) iterator.next();
				Map datamap = new HashMap();

				String province = (String) map.get("groupname");
				
				String preavg= "";
				
				if(map.get("preavg")!=null){
					 preavg = map.get("preavg").toString();
				}
				
				String avgdelay = "";
				
				if(map.get("avg_delay")!=null){
					 avgdelay = map.get("avg_delay").toString();
				}
				
				/*if (!"".equals(avgdelay)) {
					toNum = Double.valueOf(avgdelay);
				}
				if (!"".equals(preavg)) {
					previousNum = Double.valueOf(preavg);
				}
*/
				if (preavg !=null &&!preavg.isEmpty()) {
					double difValue = Double.valueOf(avgdelay) - Double.valueOf(preavg);
					percent = fm.format(difValue / Double.valueOf(preavg) * 100);
				} else {
					percent = "N/A";
				}
				datamap.put("province", province);
				datamap.put("thisdata", fm.format(Double.valueOf(avgdelay)));
				if(preavg !=null &&!preavg.isEmpty()){
					datamap.put("predata", fm.format(Double.valueOf(preavg)));
				}else{
					datamap.put("predata","N/A");
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
	 * 根据groupid获取有效样本
	 */
	@Override
	public String getValidSampleNum(String month, String groupid, String broadType) {

		JSONObject dataJSON = new JSONObject();

		if (!"".equals(groupid)) {
			String ping_test_times = groupidPingDao.getValidSampleNum(month, groupid, broadType);
			dataJSON.put("ping_test_times", ping_test_times);
		} else {
			dataJSON.put("ping_test_times", "");
		}
		return dataJSON.toString();
	}

	@Override
	public String getAvgDelay(String yearMonth, String groupid) {
		String lastMonth = UtilDate.getPreviousMonth(yearMonth);

		JSONObject avgJson = new JSONObject();

		double monthAvgDelay = 0;
		double preAvgDelay = 0;

		if (!"".equals(groupid)) {
			List<Map<String, Object>> avgDelayList = groupidPingDao.getAvgDelay(yearMonth, lastMonth, groupid);

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

			List<Map<String, Object>> avgDelayList = groupidPingDao.getAvgPingDelay(yearMonth, lastMonth, groupid);

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

	@Override
	public String getDelayStandardRate(String yearMonth, String groupid) {
		String lastMonth = UtilDate.getPreviousMonth(yearMonth);

		JSONObject avgJson = new JSONObject();
		double monthAvg = 0;
		double preAvg = 0;
		if (!"".equals(groupid)) {
			List<Map<String, Object>> avgDelayList = groupidPingDao.getDelayStandardRate(yearMonth, lastMonth, groupid);

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

			List<Map<String, Object>> avgDelayList = groupidPingDao.getPingLossRate(yearMonth, lastMonth, groupid);

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
	 * 90%用户ping时延
	 */
	@Override
	public String getTop90PingDelay(String yearMonth, String groupid) {
		String lastMonth = UtilDate.getPreviousMonth(yearMonth);

		JSONObject avgJson = new JSONObject();
		double monthAvg = 0;
		double preAvg = 0;

		if (!"".equals(groupid)) {

			List<Map<String, Object>> avgDelayList = groupidPingDao.getTop90PingDelay(yearMonth, lastMonth, groupid);

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
	 * 90%用户ping丢包率
	 */
	@Override
	public String getTop90PingLossRate(String yearMonth, String groupid) {
		String lastMonth = UtilDate.getPreviousMonth(yearMonth);

		JSONObject avgJson = new JSONObject();
		double monthAvg = 0;
		double preAvg = 0;

		if (!"".equals(groupid)) {

			List<Map<String, Object>> avgDelayList = groupidPingDao.getTop90PingLossRate(yearMonth, lastMonth, groupid);

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

	/**
	 * 质量分析上下月的数据增减值
	 */
	@Override
	public String getPingServiceQualityCompare(String groupid, String month, String broadband_type) {
		String lastMonth = UtilDate.getPreviousMonth(month);

		JSONObject avgJson = new JSONObject();
		String avg_delay = "";
		String loss_rate = "";

		if (!"".equals(groupid)) {
			List<Map<String, Object>> avgDelayList = groupidPingDao.getPingServiceQualityCompare(groupid, month, lastMonth, broadband_type);

			/*
			 * for (Iterator iterator = avgDelayList.iterator();
			 * iterator.hasNext();) { Map<String, Object> map = (Map<String,
			 * Object>) iterator.next();
			 * 
			 * 
			 * }
			 */

			if (avgDelayList.size() > 0) {
				Map<String, Object> map = avgDelayList.get(0);
				String ping_avg_delay = map.get("ping_avg_delay").toString();
				String ping_loss_rate = map.get("ping_loss_rate").toString();

				if (ping_avg_delay.contains("-")) {
					avg_delay = "减少" + Math.abs(Double.parseDouble(ping_avg_delay));
				} else if (ping_avg_delay.contains("N/A")) {
					avg_delay = ping_avg_delay;
				} else {
					avg_delay = "增加" + ping_avg_delay;
				}

				if (ping_loss_rate.contains("-")) {
					loss_rate = "减少" + Math.abs(Double.parseDouble(ping_loss_rate));
				} else if (ping_loss_rate.contains("N/A")) {
					loss_rate = ping_loss_rate;
				} else {
					loss_rate = "增加" + ping_loss_rate;
				}
			}
		}
		avgJson.put("ping_avg_delay", avg_delay);
		avgJson.put("ping_loss_rate", loss_rate);
		return avgJson.toString();
	}

	/**
	 * 获取本月指标情况
	 */
	@Override
	public String getPingKPIbyGroupid(String groupid, String month, String broadband_type) {

		JSONObject avgJson = new JSONObject();
		Map<String, Object> provinceMap = new HashMap<String, Object>();
		Map<String, Object> bestResultMap = new HashMap<String, Object>();

		if (!"".equals(groupid)) {

			List<Map<String, Object>> provinceResList = groupidPingDao.getProvinceResult(month, groupid, broadband_type);
			if (provinceResList.size() > 0) {
				provinceMap = provinceResList.get(0);
			}

			List<Map<String, Object>> bestResList = groupidPingDao.getBestResult(month, broadband_type);
			if (bestResList.size() > 0) {
				bestResultMap = bestResList.get(0);
			}
			provinceMap.putAll(bestResultMap);
			String avg_delay_order = groupidPingDao.getDelayResultOrder(month, groupid, broadband_type);
			String loss_rate_order = groupidPingDao.getLossRateResultOrder(month, groupid, broadband_type);

			provinceMap.put("delay_order", avg_delay_order);
			provinceMap.put("rate_order", loss_rate_order);

		}
		avgJson.put("data", provinceMap);

		return avgJson.toString();
	}

}
