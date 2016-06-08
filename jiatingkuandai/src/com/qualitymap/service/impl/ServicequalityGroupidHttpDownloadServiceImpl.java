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

import com.qualitymap.dao.ServicequalityGroupidHttpDownloadDao;
import com.qualitymap.dao.ServicequalityGroupidVideoDao;
import com.qualitymap.service.ServicequalityGroupidHttpDownloadService;
import com.qualitymap.service.ServicequalityGroupidVideoService;
import com.qualitymap.utils.UtilDate;

public class ServicequalityGroupidHttpDownloadServiceImpl implements ServicequalityGroupidHttpDownloadService {

	@Resource
	ServicequalityGroupidHttpDownloadDao groupidHttpDownDao;
	static Format fm = new DecimalFormat("#.##");

	/**
	 * 获取上下月的打开时延
	 */
	@Override
	public String getHttpDownloadRate(String yearMonth, String groupid) {
		String lastMonth = UtilDate.getPreviousMonth(yearMonth);

		JSONObject avgJson = new JSONObject();

		List<Map<String, Object>> avgDelayList = new ArrayList<Map<String,Object>>();

		if (!"".equals(groupid)) {
			 avgDelayList = groupidHttpDownDao.getHttpDownloadRate(yearMonth, lastMonth, groupid);
		}	
		avgJson.put("data",avgDelayList);

		return avgJson.toString();
	}

	/**
	 * 获取下载速率趋势数据
	 */
	@Override
	public String getDownloadRateData(String groupid, String probetype) {
		// TODO Auto-generated method stub
		JSONObject dataJSON = new JSONObject();
		JSONArray avgArray = new JSONArray();
		JSONArray monthArray = new JSONArray();
		if (!"".equals(groupid)) {

			List<Map<String, Object>> queryList = groupidHttpDownDao.getDownloadRateData(groupid, probetype);

			for (Iterator iterator = queryList.iterator(); iterator.hasNext();) {
				Map<String, Object> map = (Map<String, Object>) iterator.next();
				double avg_download_rate = Double.parseDouble(map.get("avg_download_rate").toString());
				String month = map.get("month").toString();
				avgArray.add(avg_download_rate);
				monthArray.add(month);
			}
		}

		dataJSON.put("lable", monthArray);
		dataJSON.put("data", avgArray);

		return dataJSON.toString();
	}

	/**
	 * 获取下载速率的排名
	 */
	@Override
	public String getDownloadRateOrder(String month, String groupid, String probetype) {

		JSONObject dataJSON = new JSONObject();

		JSONArray dataArray = new JSONArray();

		String prmonth = UtilDate.getPreviousMonth(month);
		if (!"".equals(groupid)) {
			List<Map<String, Object>> monList = groupidHttpDownDao.getDownloadRateOrder(month, prmonth, groupid, probetype);

			List<Map<String, Object>> mapList = new ArrayList<Map<String, Object>>();

			String percent = "";

			for (Iterator iterator = monList.iterator(); iterator.hasNext();) {
				Map<String, Object> map = (Map<String, Object>) iterator.next();
				Map datamap = new HashMap();

				String province = (String) map.get("groupname");
				String pagesuccess = "";
				if (map.get("avg_download_rate") != null) {
					pagesuccess = map.get("avg_download_rate").toString();
				}else{
					pagesuccess = "0";
				}

				String pre_pagesuccess = "";

				if (map.get("pre_avg_download_rate") != null) {
					pre_pagesuccess = map.get("pre_avg_download_rate").toString();
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
