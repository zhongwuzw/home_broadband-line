package com.qualitymap.service.impl;

import java.text.DecimalFormat;
import java.text.Format;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.annotation.Resource;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import com.qualitymap.dao.OverviewKpiDao;
import com.qualitymap.service.OverviewKpiService;
import com.qualitymap.utils.UtilDate;

/**
 * 概览数据统计分析
 * 
 * @author：kxc
 * @date：Apr 12, 2016
 */
public class OverviewKpiServiceImpl implements OverviewKpiService {

	@Resource
	private OverviewKpiDao kpiDao;
	static Format fm = new DecimalFormat("#.##");

	/**
	 * 累计用户数
	 */
	@Override
	public String getAccumulativnum(String month, String groupid) {

		JSONObject dataJSON = new JSONObject();
		if (!"".equals(groupid)) {
			String strAccumulativnum = kpiDao.getAccumulativnum(month, groupid);
			dataJSON.put("accumulativ_num", strAccumulativnum);
		} else {
			dataJSON.put("accumulativ_num", "");
		}
		return dataJSON.toString();
	}

	/**
	 * 本月参测省份数
	 */
	@Override
	public String getProvincenum(String month, String groupid) {

		JSONObject dataJSON = new JSONObject();
		if (!"".equals(groupid)) {
			String test_province_num = kpiDao.getProvincenum(month, groupid);
			dataJSON.put("test_province_num", test_province_num);
		} else {
			dataJSON.put("test_province_num", "");
		}
		return dataJSON.toString();
	}

	/**
	 * 本月新增用户数
	 */
	@Override
	public String getOrgnum(String month, String groupid) {

		JSONObject dataJSON = new JSONObject();

		if (!"".equals(groupid)) {
			String org_num = kpiDao.getOrgnum(month, groupid);
			dataJSON.put("org_num", org_num);
		} else {
			dataJSON.put("org_num", "");
		}
		return dataJSON.toString();
	}
	

	/**
	 * 渠道数
	 */
	@Override
	public String getChannelnum(String month, String groupid) {

		JSONObject dataJSON = new JSONObject();
		if (!"".equals(groupid)) {
			String distribution_channel_num = kpiDao.getChannelnum(month, groupid);
			dataJSON.put("distribution_channel_num", distribution_channel_num);
		} else {
			dataJSON.put("distribution_channel_num", "");
		}
		return dataJSON.toString();
	}

	/**
	 * 终端总数及增长比
	 */
	@Override
	public String getTerminalNum(String month, String groupid) {

		JSONObject dataJSON = new JSONObject();

		if (!"".equals(groupid)) {
			String prmonth = UtilDate.getPreviousMonth(month);
			List<Map<String, Object>> terList = kpiDao.getTerminalNum(prmonth, month, groupid);

			double previousNum = 0;
			double toNum = 0;

			for (Iterator iterator = terList.iterator(); iterator.hasNext();) {
				Map<String, Object> map = (Map<String, Object>) iterator.next();

				if (month.equals(map.get("month"))) {
					toNum = Double.valueOf(map.get("terminal_num").toString());
				} else if (prmonth.equals(map.get("month"))) {
					previousNum = Double.valueOf(map.get("terminal_num").toString());
				}
			}
			String percent = "";
			if (previousNum != 0) {
				double difValue = toNum - previousNum;
				percent = fm.format(difValue / previousNum * 100) + "%";
			} else {
				percent = "N/A";
			}

			dataJSON.put("percent", percent);
			dataJSON.put("terminal_num", toNum);
		} else {
			dataJSON.put("percent", "");
			dataJSON.put("terminal_num", "");
		}

		return dataJSON.toString();
	}

	/**
	 * 累计注册用户数及增长比
	 */
	@Override
	public String getRegusernameNum(String month, String groupid) {

		JSONObject dataJSON = new JSONObject();

		if (!"".equals(groupid)) {
			String prmonth = UtilDate.getPreviousMonth(month);
			List<Map<String, Object>> terList = kpiDao.getRegusernameNum(prmonth, month, groupid);
			double previousNum = 0;
			double toNum = 0;

			for (Iterator iterator = terList.iterator(); iterator.hasNext();) {
				Map<String, Object> map = (Map<String, Object>) iterator.next();

				if (month.equals(map.get("month"))) {
					toNum = Double.valueOf(map.get("regusername_num").toString());
				} else if (prmonth.equals(map.get("month"))) {
					previousNum = Double.valueOf(map.get("regusername_num").toString());
				}
			}

			String percent = "";
			if (previousNum != 0) {
				double difValue = toNum - previousNum;
				percent = fm.format(difValue / previousNum * 100) + "%";
			} else {
				percent = "N/A";
			}

			dataJSON.put("percent", percent);
			dataJSON.put("regusername_num", toNum);
		} else {
			dataJSON.put("percent", "");
			dataJSON.put("regusername_num", "");
		}

		return dataJSON.toString();
	}

	/**
	 * 累计用户数及增长比
	 */
	@Override
	public String getCustomersNum(String month, String groupid) {

		JSONObject dataJSON = new JSONObject();

		if (!"".equals(groupid)) {
			String prmonth = UtilDate.getPreviousMonth(month);
			// month = month+","+prmonth;
			List<Map<String, Object>> terList = kpiDao.getCustomersNum(prmonth, month, groupid);

			double previousNum = 0;
			double toNum = 0;

			for (Iterator iterator = terList.iterator(); iterator.hasNext();) {
				Map<String, Object> map = (Map<String, Object>) iterator.next();

				if (month.equals(map.get("month"))) {
					toNum = Double.valueOf(map.get("customers_num").toString());
				} else if (prmonth.equals(map.get("month"))) {
					previousNum = Double.valueOf(map.get("customers_num").toString());
				}
			}

			String percent = "";
			if (previousNum != 0) {
				double difValue = toNum - previousNum;
				percent = fm.format(difValue / previousNum * 100) + "%";
			} else {
				percent = "N/A";
			}

			dataJSON.put("percent", percent);
			dataJSON.put("customers_num", toNum);
		} else {
			dataJSON.put("percent", "");
			dataJSON.put("customers_num", "");
		}

		return dataJSON.toString();
	}

	/**
	 * 启动次数及增长比
	 */
	@Override
	public String getNewlyIncreaseNum(String month, String groupid) {

		JSONObject dataJSON = new JSONObject();

		if (!"".equals(groupid)) {
			//上月
			String prmonth = UtilDate.getPreviousMonth(month);
			//上上月
			String premonth = UtilDate.getPreviousMonth(prmonth);
			// month = month+","+prmonth;
			List<Map<String, Object>> terList = kpiDao.getNewlyIncreaseNum(premonth, prmonth, groupid);

			double previousNum = 0;
			double toNum = 0;

			for (Iterator iterator = terList.iterator(); iterator.hasNext();) {
				Map<String, Object> map = (Map<String, Object>) iterator.next();

				if (month.equals(map.get("month"))) {
					toNum = Double.valueOf(map.get("newly_increase_num").toString());
				} else if (prmonth.equals(map.get("month"))) {
					previousNum = Double.valueOf(map.get("newly_increase_num").toString());
				}
			}

			String percent = "";
			if (previousNum != 0) {
				double difValue = toNum - previousNum;
				percent = fm.format(difValue / previousNum * 100) + "%";
			} else {
				percent = "N/A";
			}

			dataJSON.put("percent", percent);
			dataJSON.put("newly_increase_num", previousNum);
		} else {
			dataJSON.put("percent", "");
			dataJSON.put("newly_increase_num", "");

		}

		return dataJSON.toString();
	}

	/**
	 * 用户数趋势分析
	 */
	@Override
	public String getUserTendencyData(String groupid) {

		JSONObject dataJSON = new JSONObject();

		if (!"".equals(groupid)) {
			List<Map<String, Object>> terList = kpiDao.getUserTendencyData(groupid);

			JSONArray userNumarray = new JSONArray();
			JSONArray accArray = new JSONArray();
			JSONArray monthArray = new JSONArray();

			if (terList.size() > 0) {
				for (Iterator iterator = terList.iterator(); iterator.hasNext();) {
					Map<String, Object> map = (Map<String, Object>) iterator.next();
					userNumarray.add(map.get("new_user_num"));
					accArray.add(map.get("accumulativ_num"));
					monthArray.add(map.get("month"));
				}
			} else {
				dataJSON.put("data", new JSONArray());
			}
			dataJSON.put("lables", monthArray);
			dataJSON.put("new_user_num", userNumarray);
			dataJSON.put("accumulativ_num", accArray);
		} else {
			dataJSON.put("data", new JSONArray());
		}

		return dataJSON.toString();
	}

	/**
	 * 测试次数
	 */
	@Override
	public String getThismonthTesttimes(String month, String groupid) {

		JSONObject dataJSON = new JSONObject();

		if (!"".equals(groupid)) {
			//上月
			String prmonth = UtilDate.getPreviousMonth(month);
			List<Map<String, Object>> terList = kpiDao.getThismonthTesttimes(prmonth, month, groupid);

			double previousNum = 0;
			double toNum = 0;

			for (Iterator iterator = terList.iterator(); iterator.hasNext();) {
				Map<String, Object> map = (Map<String, Object>) iterator.next();

				if (month.equals(map.get("month"))) {
					toNum = Double.valueOf(map.get("newly_increase_num").toString());
				} else if (prmonth.equals(map.get("month"))) {
					previousNum = Double.valueOf(map.get("newly_increase_num").toString());
				}
			}

			String percent = "";
			if (previousNum != 0) {
				double difValue = toNum - previousNum;
				percent = fm.format(difValue / previousNum * 100) + "%";
			} else {
				percent = "N/A";
			}

			dataJSON.put("percent", percent); 
			dataJSON.put("newly_increase_num", previousNum);
			dataJSON.put("thismonthnum", toNum);
		} else {
			dataJSON.put("percent", "");
			dataJSON.put("newly_increase_num", "");
			dataJSON.put("thismonthnum", "");
		}

		return dataJSON.toString();
	}
	
	
}