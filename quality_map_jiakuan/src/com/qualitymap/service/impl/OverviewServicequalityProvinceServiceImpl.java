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

import com.qualitymap.dao.OverviewServicequalityProvinceDao;
import com.qualitymap.service.OverviewServicequalityProvinceService;
import com.qualitymap.utils.UtilDate;
import com.qualitymap.vo.OverviewServicequalityProvince;

/**
 * 业务质量数据统计
 * 
 * @author：kxc
 * @date：Apr 11, 2016
 */
public class OverviewServicequalityProvinceServiceImpl implements OverviewServicequalityProvinceService {

	@Resource
	private OverviewServicequalityProvinceDao servicequalityDao;
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

			List<Map<String, Object>> queryList = servicequalityDao.getAvgDelayData(groupid);
			if(queryList.size()>0){
				
				for (Iterator iterator = queryList.iterator(); iterator.hasNext();) {
					Map<String, Object> map = (Map<String, Object>) iterator.next();
					double avg_delay =  Double.parseDouble(map.get("avg_delay").toString());
					String month =  map.get("month").toString();
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
			List<Map<String, Object>> monList = servicequalityDao.getAvgDelayOrder(month, prmonth, groupid);

			List<Map<String, Object>> mapList = new ArrayList<Map<String, Object>>();

			double previousNum = 0;
			double toNum = 0;
			String percent = "";

			for (Iterator iterator = monList.iterator(); iterator.hasNext();) {
				Map<String, Object> map = (Map<String, Object>) iterator.next();
				Map datamap = new HashMap();

				String province = (String) map.get("province");
				String preavg =  map.get("preavg").toString();
				String avgdelay = map.get("avg_delay").toString();
				if(!"".equals(avgdelay)){
					toNum = Double.valueOf( avgdelay);
				}
				if(!"".equals(preavg)){
					previousNum = Double.valueOf(preavg);
				}

				if (previousNum != 0) {
					double difValue = toNum - previousNum;
					percent = fm.format(difValue / previousNum * 100);
				} else {
					percent = "N/A";
				}

				datamap.put("province", province);
				datamap.put("thisdata",fm.format(toNum));
				datamap.put("predata", fm.format(previousNum));
				datamap.put("percent", percent);

				mapList.add(datamap);
			}

			dataArray = JSONArray.fromObject(mapList);
		}

		dataJSON.put("data", dataArray);

		return dataJSON.toString();
	}

	/**
	 * 获取页面打开成功趋势
	 */
	@Override
	public String getPageSuccessData(String groupid) {
		// TODO Auto-generated method stub
		JSONObject dataJSON = new JSONObject();
		JSONArray avgArray = new JSONArray();
		JSONArray monthArray = new JSONArray();
		if (!"".equals(groupid)) {

			List<Map<String, Object>> queryList = servicequalityDao.getPageSuccessData(groupid);

			for (Iterator iterator = queryList.iterator(); iterator.hasNext();) {
				Map<String, Object> map = (Map<String, Object>) iterator.next();
				double page_success_rate =  Double.parseDouble(map.get("page_success_rate").toString());
				String month =  map.get("month").toString();
				avgArray.add(fm.format(page_success_rate));
				monthArray.add(month);
			}
		}

		dataJSON.put("lable", monthArray);
		dataJSON.put("data", avgArray);
		
		return dataJSON.toString();
	}

	/**
	 * 获取页面元素打开成功率排名
	 */
	@Override
	public String getPageSuccessOrder(String month, String groupid) {

		JSONObject dataJSON = new JSONObject();

		JSONArray dataArray = new JSONArray();

		String prmonth = UtilDate.getPreviousMonth(month);
		if (!"".equals(groupid)) {
			List<Map<String, Object>> monList = servicequalityDao.getPageSuccessOrder(month, prmonth, groupid);

			List<Map<String, Object>> mapList = new ArrayList<Map<String, Object>>();

			double previousNum = 0;
			double toNum = 0;
			String percent = "";

			for (Iterator iterator = monList.iterator(); iterator.hasNext();) {
				Map<String, Object> map = (Map<String, Object>) iterator.next();
				Map datamap = new HashMap();

				String province = (String) map.get("province");
				
				String pagesuccess =  map.get("page_success_rate").toString();
				String pre_pagesuccess = map.get("pre_page_success_rate").toString() ;
				
				if(!"".equals(pagesuccess)){
					toNum = Double.valueOf(pagesuccess);
				}
				
				if(!"".equals(pre_pagesuccess)){
					previousNum = Double.valueOf(pre_pagesuccess);
				}

				if (previousNum != 0) {
					double difValue = toNum - previousNum;
					percent = fm.format(difValue / previousNum * 100);
				} else {
					percent = "N/A";
				}

				datamap.put("province", province);
				datamap.put("thisdata", fm.format(toNum));
				datamap.put("predata", fm.format(previousNum));
				datamap.put("percent", percent);

				mapList.add(datamap);
			}

			dataArray = JSONArray.fromObject(mapList);
		}

		dataJSON.put("data", dataArray);

		return dataJSON.toString();
	}

}
