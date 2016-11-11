package com.qualitymap.service.impl;

import java.text.DecimalFormat;
import java.text.Format;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.annotation.Resource;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import com.qualitymap.dao.ServicequalityOrgidWebbrowsingDao;
import com.qualitymap.dao.ServicequalityperiodDao;
import com.qualitymap.service.ServicequalityOrgidWebbrowsingService;
import com.qualitymap.service.ServicequalityperiodService;

public class ServicequalityperiodServiceImpl implements ServicequalityperiodService{

	@Resource
	ServicequalityperiodDao servicequalityperiodDao;
	static Format fm = new DecimalFormat("#.##");
	/**
	 * 分地域指标统计
	 */
	@Override
	public String getperiodData(String groupid, String month, String broadband_type) {

		LinkedHashMap<String, LinkedHashMap<String, LinkedHashMap<String, List<LinkedHashMap<String, String>>>>> datamap = new LinkedHashMap<String, LinkedHashMap<String, LinkedHashMap<String, List<LinkedHashMap<String, String>>>>>();

		List<Map<String, Object>> getPeriodrateHttpdownload = servicequalityperiodDao.getPeriodrateHttpdownload(groupid, month, broadband_type);
		List<Map<String, Object>> getPeriodNinetyDelayWebbrowsing = servicequalityperiodDao.getPeriodNinetyDelayWebbrowsing(groupid, month, broadband_type);
		List<Map<String, Object>> getPeriodAvgDelayWebbrowsing = servicequalityperiodDao.getPeriodAvgDelayWebbrowsing(groupid, month, broadband_type);
		List<Map<String, Object>> getPeriodVideoDelayVideo = servicequalityperiodDao.getPeriodVideoDelayVideo(groupid, month, broadband_type);
		List<Map<String, Object>> getPeriodcacheCountVideo = servicequalityperiodDao.getPeriodcacheCountVideo(groupid, month, broadband_type);
		List<Map<String, Object>> getPeriodBufferproportionVideo = servicequalityperiodDao.getPeriodBufferproportionVideo(groupid, month, broadband_type);
	
		List<Map<String, Object>> datalist = new ArrayList<Map<String, Object>>();

		
		
		datalist.addAll(getPeriodrateHttpdownload);
		
		datalist.addAll(getPeriodNinetyDelayWebbrowsing);
		datalist.addAll(getPeriodAvgDelayWebbrowsing);
		
		datalist.addAll(getPeriodVideoDelayVideo);
		datalist.addAll(getPeriodcacheCountVideo);
		datalist.addAll(getPeriodBufferproportionVideo);
		
		
		
		
		
		for (Iterator iterator = datalist.iterator(); iterator.hasNext();) {
				Map<String, Object> map = (Map<String, Object>) iterator.next();

			if (datamap.containsKey(map.get("period"))) {
				LinkedHashMap<String, LinkedHashMap<String, List<LinkedHashMap<String, String>>>> kpimap = datamap.get(map.get("period"));

				if (kpimap.containsKey(map.get("kpitypename"))) {

					LinkedHashMap<String, List<LinkedHashMap<String, String>>> filemap = kpimap.get(map.get("kpitypename"));

					if (filemap.containsKey(map.get("fieldname"))) {
						List<LinkedHashMap<String, String>> perlist = filemap.get(map.get("fieldname"));
						LinkedHashMap<String, String> percentmap = new LinkedHashMap<String, String>();
						percentmap.put("percent75", map.get("percent75") + "");
						percentmap.put("percent85", map.get("percent85") + "");
						percentmap.put("percent95", map.get("percent95") + "");
						percentmap.put("avgvalue", map.get("avgvalue") + "");
						percentmap.put("usertype", map.get("usertype") + "");
						percentmap.put("successratevalue", map.get("successratevalue") + "");
						perlist.add(percentmap);
						filemap.put(map.get("fieldname") + "", perlist);
						kpimap.put(map.get("kpitypename") + "", filemap);
						datamap.put(map.get("period") + "", kpimap);
						
					} else {
						//Map<String, List<Map<String, String>>> fieldmap = new HashMap<String, List<Map<String, String>>>();
						List<LinkedHashMap<String, String>> maplist_new = new ArrayList<LinkedHashMap<String, String>>();
						LinkedHashMap<String, String> percentmap = new LinkedHashMap<String, String>();
						percentmap.put("percent75", map.get("percent75") + "");
						percentmap.put("percent85", map.get("percent85") + "");
						percentmap.put("percent95", map.get("percent95") + "");
						percentmap.put("avgvalue", map.get("avgvalue") + "");
						percentmap.put("usertype", map.get("usertype") + "");
						percentmap.put("successratevalue", map.get("successratevalue") + "");

						maplist_new.add(percentmap);
						filemap.put(map.get("fieldname") + "", maplist_new);
						kpimap.put(map.get("kpitypename") + "", filemap);
						datamap.put(map.get("period") + "", kpimap);
						
					}

				} else {
					LinkedHashMap<String, List<LinkedHashMap<String, String>>> fieldmap = new LinkedHashMap<String, List<LinkedHashMap<String, String>>>();
					List<LinkedHashMap<String, String>> maplist_new = new ArrayList<LinkedHashMap<String, String>>();
					LinkedHashMap<String, String> percentmap = new LinkedHashMap<String, String>();
					percentmap.put("percent75", map.get("percent75") + "");
					percentmap.put("percent85", map.get("percent85") + "");
					percentmap.put("percent95", map.get("percent95") + "");
					percentmap.put("avgvalue", map.get("avgvalue") + "");
					percentmap.put("usertype", map.get("usertype") + "");
					percentmap.put("successratevalue", map.get("successratevalue") + "");
					maplist_new.add(percentmap);
					fieldmap.put(map.get("fieldname") + "", maplist_new);
					kpimap.put(map.get("kpitypename") + "", fieldmap);
					datamap.put(map.get("period") + "", kpimap);
				}

			} else {

				//Map<String, Map<String, Map<String, List<Map<String, String>>>>> kpimap_new = new HashMap<String, Map<String, Map<String, List<Map<String, String>>>>>();
				LinkedHashMap<String, LinkedHashMap<String, List<LinkedHashMap<String, String>>>> citymap = new LinkedHashMap<String, LinkedHashMap<String, List<LinkedHashMap<String, String>>>>();
				LinkedHashMap<String, List<LinkedHashMap<String, String>>> fieldmap = new LinkedHashMap<String, List<LinkedHashMap<String, String>>>();
				List<LinkedHashMap<String, String>> maplist_new = new ArrayList<LinkedHashMap<String, String>>();
				LinkedHashMap<String, String> percentmap = new LinkedHashMap<String, String>();
				percentmap.put("percent75", map.get("percent75") + "");
				percentmap.put("percent85", map.get("percent85") + "");
				percentmap.put("percent95", map.get("percent95") + "");
				percentmap.put("avgvalue", map.get("avgvalue") + "");
				percentmap.put("usertype", map.get("usertype") + "");
				percentmap.put("successratevalue", map.get("successratevalue") + "");
				maplist_new.add(percentmap);
				fieldmap.put(map.get("fieldname") + "", maplist_new);
				citymap.put(map.get("kpitypename") + "", fieldmap);
				datamap.put(map.get("period") + "", citymap);
				
			}
		}

		

		
			 JSONArray dataarray = new JSONArray();
			JSONObject cityjson = new JSONObject();
	
			JSONObject jsonObject = new JSONObject();
	
			JSONObject kpiobject = new JSONObject();
			JSONObject fieldobject = new JSONObject(); 
		  for (Entry<String, LinkedHashMap<String, LinkedHashMap<String, List<LinkedHashMap<String, String>>>>> entry : datamap.entrySet()) {
			cityjson.put("period", entry.getKey());

			LinkedHashMap<String, LinkedHashMap<String, List<LinkedHashMap<String, String>>>> kpimap = entry.getValue();

			JSONArray cityarray = new JSONArray();
			for (Entry<String, LinkedHashMap<String, List<LinkedHashMap<String, String>>>> kpi : kpimap.entrySet()) {

				kpiobject.put("kpitypename", kpi.getKey());

				LinkedHashMap<String, List<LinkedHashMap<String, String>>> filemap = kpi.getValue();

				JSONArray fieldarray = new JSONArray();
				for (Entry<String, List<LinkedHashMap<String, String>>> files : filemap.entrySet()) {
					fieldobject.put("fieldname", files.getKey());

					fieldobject.put("filedata", files.getValue());
					fieldarray.add(fieldobject);
				}

				kpiobject.put("kpidata", fieldarray);
				cityarray.add(kpiobject);
			}

			cityjson.put("perioddata", cityarray);
			dataarray.add(cityjson);
		}

		jsonObject.put("data", dataarray);
		System.out.println(jsonObject.toString());
		return jsonObject.toString();
	}

	
}
