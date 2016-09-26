package com.qualitymap.service.impl;

import java.text.DecimalFormat;
import java.text.Format;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.annotation.Resource;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import com.qualitymap.dao.OverviewServicequalityDao;
import com.qualitymap.service.OverviewServicequalityService;


/**
 * Description: OTS统计平台业务质量
 * 
 * @author zqh 2016-4-7: PM 04:15:11
 */
public class OverviewServicequalityServiceImpl implements OverviewServicequalityService {
	@Resource
	OverviewServicequalityDao overviewServicequalityDao;
	static Format fm = new DecimalFormat("#.##");

	/**
	 * 获取用户指标统计
	 */
	@Override
	public String getUserIndicator(String groupid, String month, String broadband_type) {

		JSONObject datajson = new JSONObject();
		JSONObject androidjson = new JSONObject();
		JSONObject iosjson = new JSONObject();
		JSONObject pcjson = new JSONObject();
		
		String group = "";

		if(groupid.contains(",")){
			group = "and groupid in(" + groupid + ")";
		}else{
			group = " and groupid ='" + groupid + "' ";
		}
		
		// 下载用户指标统计
		List<Map<String, Object>> getHttpDownloadUserIndicator = overviewServicequalityDao.getHttpDownloadUserIndicator(groupid, month, broadband_type,group);
		//List<Map<String, Object>> getHttpDownloadSuccessUserIndicator = overviewServicequalityDao.getHttpDownloadSuccessUserIndicator(groupid, month, broadband_type,group);
		// 视频用户指标
		List<Map<String, Object>> getVideoProportionUserIndicator = overviewServicequalityDao.getVideoProportionUserIndicator(groupid, month, broadband_type,group);
		List<Map<String, Object>> getVideoDelayUserIndicator = overviewServicequalityDao.getVideoDelayUserIndicator(groupid, month, broadband_type,group);
		List<Map<String, Object>> getVideoCache_countUserIndicator = overviewServicequalityDao.getVideoCache_countUserIndicator(groupid, month, broadband_type,group);
		//List<Map<String, Object>> getVideoPaly_successUserIndicator = overviewServicequalityDao.getVideoPaly_successUserIndicator(groupid, month, broadband_type,group);
		// 网页浏览用户指标统计
		List<Map<String, Object>> getWebbrowsingAvgdelayUserIndicator = overviewServicequalityDao.getWebbrowsingAvgdelayUserIndicator(groupid, month, broadband_type,group);
		List<Map<String, Object>> getWebbrowsingninetydelayUserIndicator = overviewServicequalityDao.getWebbrowsingninetydelayUserIndicator(groupid, month, broadband_type,group);
		//List<Map<String, Object>> getWebbrowsingsuccessUserIndicator = overviewServicequalityDao.getWebbrowsingsuccessUserIndicator(groupid, month, broadband_type,group);
		//List<Map<String, Object>> getWebbrowsingVisitSuccessRate = overviewServicequalityDao.getWebbrowsingVisitSuccessRate(groupid, month, broadband_type);

		List<Map<String, Object>> datalist = new ArrayList<Map<String, Object>>();

		datalist.addAll(getHttpDownloadUserIndicator);
		//datalist.addAll(getHttpDownloadSuccessUserIndicator);
		
		
		datalist.addAll(getWebbrowsingninetydelayUserIndicator);
		datalist.addAll(getWebbrowsingAvgdelayUserIndicator);
		//datalist.addAll(getWebbrowsingsuccessUserIndicator);
		//datalist.addAll(getWebbrowsingVisitSuccessRate);

		datalist.addAll(getVideoProportionUserIndicator);
		datalist.addAll(getVideoCache_countUserIndicator);
		datalist.addAll(getVideoDelayUserIndicator);
		//datalist.addAll(getVideoPaly_successUserIndicator);
		
		JSONArray androidArray = new JSONArray();
		JSONArray iosArray = new JSONArray();
		JSONArray pcArray = new JSONArray();
		JSONArray dataArray = new JSONArray();

		List list = new ArrayList();
		for (Iterator iterator = datalist.iterator(); iterator.hasNext();) {
			Map<String, Object> map = (Map<String, Object>) iterator.next();

			if ("PC".equals(map.get("probetype"))) {
				pcArray.add(map);
			} else if ("Android".equals(map.get("probetype"))) {
				androidArray.add(map);
			} else if ("iOS".equals(map.get("probetype"))) {
				iosArray.add(map);
			}
		}
		if (androidArray.size() > 0) {
			androidjson.put("type", "Android");
			androidjson.put("data", androidArray);
			dataArray.add(androidjson);
		}

		if (iosArray.size() > 0) {
			iosjson.put("type", "iOS");
			iosjson.put("data", iosArray);
			dataArray.add(iosjson);
		}
		if (pcArray.size() > 0) {

			pcjson.put("type", "PC");
			pcjson.put("data", pcArray);
			dataArray.add(pcjson);
		}

		

		datajson.put("data", dataArray);
System.out.println(datajson.toString());
		return datajson.toString();

	}

	/**
	 * 获取地域级别用户指标统计
	 */
	@Override
	public String getTerritoryData(String groupid, String month, String broadband_type) {

		JSONObject datajson = new JSONObject();
		JSONArray dataaArray = new JSONArray();
		JSONArray urlArray = new JSONArray();

		Map<String, Map<String, String>> urldata = new HashMap<String, Map<String, String>>();

		List<Map<String, Object>> getTerritoryHttpDownload = overviewServicequalityDao.getTerritoryHttpDownload(groupid, month, broadband_type);
		List<Map<String, Object>> getTerritoryVideo = overviewServicequalityDao.getTerritoryVideo(groupid, month, broadband_type);
		List<Map<String, Object>> getTerritoryWebbrowsing = overviewServicequalityDao.getTerritoryWebbrowsing(groupid, month, broadband_type);

		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
		list.addAll(getTerritoryHttpDownload);
		list.addAll(getTerritoryVideo);
		list.addAll(getTerritoryWebbrowsing);

		Map<String, Map<String, String>> cityMap = new HashMap<String, Map<String, String>>();

		for (Iterator iterator = list.iterator(); iterator.hasNext();) {
			Map<String, Object> map = (Map<String, Object>) iterator.next();

			double url_num = 0;
			String surl_num = map.get("url_num") + "";
			if (surl_num != null && !surl_num.isEmpty() && !surl_num.equals("null")) {
				url_num = Double.parseDouble(surl_num);
			}

			if (urldata.containsKey(map.get("testtype"))) {

				Map<String, String> dmap = urldata.get(map.get("testtype"));
				double nurl_num = Double.parseDouble(dmap.get("url_num")) + url_num;
				dmap.put("url_num", (int) nurl_num + "");

			} else {

				Map<String, String> newdmap = new HashMap<String, String>();

				newdmap.put("testtype", (String) map.get("testtype"));
				newdmap.put("url_num", (int) url_num + "");
				urldata.put(map.get("testtype") + "", newdmap);
			}

			String snum = map.get("samplenum") + "";
			String vnum = map.get("validsample") + "";
			double samplenum = 0;
			double validsample = 0;
			if (snum != null && !snum.isEmpty() && !snum.equals("null")) {
				samplenum = Double.parseDouble(snum);
			}
			if (vnum != null && !vnum.isEmpty() && !vnum.equals("null")) {
				validsample = Double.parseDouble(vnum);
			}

			if (cityMap.containsKey(map.get("cityname"))) {

				Map<String, String> data = cityMap.get(map.get("cityname"));

				double citysamplenum = Double.parseDouble(data.get("samplenum") + "") + samplenum;
				double cityvalidsample = Double.parseDouble(data.get("validsample") + "") + validsample;

				if (map.containsKey("internetdownload")) {
					double internet = (Double) map.get("internetdownload");
					data.put("internetdownload", (int) internet + "");
				} else if (map.containsKey("webbrowse")) {
					double webbrowse = (Double) map.get("webbrowse");
					data.put("webbrowse", (int) webbrowse + "");
				} else if (map.containsKey("h5video")) {
					double h5video = (Double) map.get("h5video");
					data.put("h5video", (int) h5video + "");
				}
				data.put("samplenum", (int) citysamplenum + "");
				data.put("validsample", (int) cityvalidsample + "");
				data.put("cityname", map.get("cityname") + "");

				cityMap.put((String) map.get("cityname"), data);

			} else {

				Map<String, String> newdata = new HashMap<String, String>();
				if (map.containsKey("internetdownload")) {
					double internet = (Double) map.get("internetdownload");
					newdata.put("internetdownload", (int) internet + "");
					newdata.put("webbrowse", "0");
					newdata.put("h5video", "0");
				} else if (map.containsKey("webbrowse")) {
					double webbrowse = (Double) map.get("webbrowse");
					newdata.put("webbrowse", (int) webbrowse + "");
					newdata.put("internetdownload", "0");
					newdata.put("h5video", "0");
				} else if (map.containsKey("h5video")) {
					double h5video = (Double) map.get("h5video");
					newdata.put("h5video", (int) h5video + "");
					newdata.put("internetdownload", "0");
					newdata.put("webbrowse", "0");
				}
				newdata.put("samplenum", (int) samplenum + "");
				newdata.put("validsample", (int) validsample + "");
				newdata.put("cityname", map.get("cityname") + "");
				cityMap.put(map.get("cityname") + "", newdata);
			}

		}

		for (Entry<String, Map<String, String>> entry : cityMap.entrySet()) {

			dataaArray.add(entry.getValue());
		}
		for (Entry<String, Map<String, String>> entry : urldata.entrySet()) {

			urlArray.add(entry.getValue());
		}

		datajson.put("data", dataaArray);
		datajson.put("urldata", urlArray);
		System.out.println(datajson.toString());
		return datajson.toString();
	}

	/**
	 * 分地域指标统计
	 */
	@Override
	public String getCityData(String groupid, String month, String broadband_type) {

		LinkedHashMap<String, LinkedHashMap<String, LinkedHashMap<String, List<LinkedHashMap<String, String>>>>> datamap = new LinkedHashMap<String, LinkedHashMap<String, LinkedHashMap<String, List<LinkedHashMap<String, String>>>>>();

		List<Map<String, Object>> getCityrateHttpdownload = overviewServicequalityDao.getCityrateHttpdownload(groupid, month, broadband_type);
		//List<Map<String, Object>> getCitysuccessHttpdownload = overviewServicequalityDao.getCitysuccessHttpdownload(groupid, month, broadband_type);
		List<Map<String, Object>> getCityVideoDelayVideo = overviewServicequalityDao.getCityVideoDelayVideo(groupid, month, broadband_type);
		List<Map<String, Object>> getCitycacheCountVideo = overviewServicequalityDao.getCitycacheCountVideo(groupid, month, broadband_type);
		//List<Map<String, Object>> getCityPlaySuccessVideo = overviewServicequalityDao.getCityPlaySuccessVideo(groupid, month, broadband_type);
		List<Map<String, Object>> getCityBufferproportionVideo = overviewServicequalityDao.getCityBufferproportionVideo(groupid, month, broadband_type);
		List<Map<String, Object>> getCityNinetyDelayWebbrowsing = overviewServicequalityDao.getCityNinetyDelayWebbrowsing(groupid, month, broadband_type);
		List<Map<String, Object>> getCityAvgDelayWebbrowsing = overviewServicequalityDao.getCityAvgDelayWebbrowsing(groupid, month, broadband_type);
		//List<Map<String, Object>> getCityPageSuccessRateWebbrowsing = overviewServicequalityDao.getCityPageSuccessRateWebbrowsing(groupid, month, broadband_type);
		//List<Map<String, Object>> getCitySuccessWebbrowsing = overviewServicequalityDao.getCitySuccessWebbrowsing(groupid, month, broadband_type);
	
		List<Map<String, Object>> datalist = new ArrayList<Map<String, Object>>();

		
		
		datalist.addAll(getCityrateHttpdownload);
		//datalist.addAll(getCitysuccessHttpdownload);
		
		datalist.addAll(getCityNinetyDelayWebbrowsing);
		datalist.addAll(getCityAvgDelayWebbrowsing);
		//datalist.addAll(getCityPageSuccessRateWebbrowsing);
		//datalist.addAll(getCitySuccessWebbrowsing);
		
		datalist.addAll(getCityVideoDelayVideo);
		datalist.addAll(getCitycacheCountVideo);
		//datalist.addAll(getCityPlaySuccessVideo);
		datalist.addAll(getCityBufferproportionVideo);
		
		
		
		
		
		for (Iterator iterator = datalist.iterator(); iterator.hasNext();) {
				Map<String, Object> map = (Map<String, Object>) iterator.next();

			if (datamap.containsKey(map.get("cityname"))) {
				LinkedHashMap<String, LinkedHashMap<String, List<LinkedHashMap<String, String>>>> kpimap = datamap.get(map.get("cityname"));

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
						datamap.put(map.get("cityname") + "", kpimap);
						
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
						datamap.put(map.get("cityname") + "", kpimap);
						
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
					datamap.put(map.get("cityname") + "", kpimap);
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
				datamap.put(map.get("cityname") + "", citymap);
				
			}
		}

		

		
			 JSONArray dataarray = new JSONArray();
			JSONObject cityjson = new JSONObject();
	
			JSONObject jsonObject = new JSONObject();
	
			JSONObject kpiobject = new JSONObject();
			JSONObject fieldobject = new JSONObject(); 
		  for (Entry<String, LinkedHashMap<String, LinkedHashMap<String, List<LinkedHashMap<String, String>>>>> entry : datamap.entrySet()) {
			cityjson.put("cityname", entry.getKey());

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

			cityjson.put("citydata", cityarray);
			dataarray.add(cityjson);
		}

		jsonObject.put("data", dataarray);
		System.out.println(jsonObject.toString());
		return jsonObject.toString();
	}

	/**
	 * 按运营商进行的指标统计
	 */
	@Override
	public String getOperatorData(String groupid, String month, String broadband_type) {

		
		JSONArray dataaArray = new JSONArray();
		JSONObject datajson = new JSONObject();
		List<Map<String, Object>> getSpecialCasebufferVideo = overviewServicequalityDao.getSpecialCasebufferVideo(groupid, month, broadband_type);
		List<Map<String, Object>> getSpecialCaseHttpDownloadRate = overviewServicequalityDao.getSpecialCaseHttpDownloadRate(groupid, month, broadband_type);
		List<Map<String, Object>> getSpecialCaseCacheCountVideo = overviewServicequalityDao.getSpecialCaseCacheCountVideo(groupid, month, broadband_type);
		List<Map<String, Object>> getSpecialCaseFirstDelayVideo = overviewServicequalityDao.getSpecialCaseFirstDelayVideo(groupid, month, broadband_type);
		List<Map<String, Object>> getSpecialCaseNinetyDelayWebbrowsing = overviewServicequalityDao.getSpecialCaseNinetyDelayWebbrowsing(groupid, month, broadband_type);
		List<Map<String, Object>> getSpecialCaseAvgDelayWebbrowsing = overviewServicequalityDao.getSpecialCaseAvgDelayWebbrowsing(groupid, month, broadband_type);
		List<List<Map<String, Object>>> datalist = new ArrayList<List<Map<String, Object>>>();
		
		datalist.add(getSpecialCaseHttpDownloadRate);
		datalist.add(getSpecialCaseNinetyDelayWebbrowsing);
		datalist.add(getSpecialCaseAvgDelayWebbrowsing);
		datalist.add(getSpecialCasebufferVideo);
		datalist.add(getSpecialCaseCacheCountVideo);
		datalist.add(getSpecialCaseFirstDelayVideo);
		
		for (Iterator iterator = datalist.iterator(); iterator.hasNext();) {
			List<Map<String, Object>> list = (List<Map<String, Object>>) iterator.next();
			JSONObject kpiarray = dealListData(list);
			if(kpiarray.size()>0){
				
				dataaArray.add(kpiarray);
			}
		}
		datajson.put("data", dataaArray);
		System.out.println(datajson.toString());
		return datajson.toString();
	}

	

	/**
	 * 处理竞品对标数据
	 * 
	 * @param dataList
	 * @return
	 */
	public static JSONObject dealListData(List<Map<String, Object>> dataList) {

		LinkedHashMap<String, LinkedHashMap<String, List<LinkedHashMap<String, String>>>> datamap = new LinkedHashMap<String, LinkedHashMap<String, List<LinkedHashMap<String, String>>>>();

		for (Iterator iterator = dataList.iterator(); iterator.hasNext();) {
			Map<String, Object> map = (Map<String, Object>) iterator.next();

			if (datamap.containsKey(map.get("kpiname"))) {
				LinkedHashMap<String, List<LinkedHashMap<String, String>>> oldkpimap = datamap.get(map.get("kpiname"));

				if (oldkpimap.containsKey(map.get("probetype"))) {
					List<LinkedHashMap<String, String>> oldprolist = oldkpimap.get(map.get("probetype"));
					LinkedHashMap<String, String> operatormap = new LinkedHashMap<String, String>();
					operatormap.put("percent75", map.get("percent75") + "");
					operatormap.put("percent85", map.get("percent85") + "");
					operatormap.put("percent95", map.get("percent95") + "");
					operatormap.put("operator", map.get("operator") + "");
					operatormap.put("avgvalue", map.get("avgvalue") + "");
					operatormap.put("successratevalue", map.get("successratevalue") + "");
					oldprolist.add(operatormap);
					oldkpimap.put(map.get("probetype") + "", oldprolist);
					datamap.put(map.get("kpiname") + "", oldkpimap);

				} else {
					List<LinkedHashMap<String, String>> newprobetypeList = new ArrayList<LinkedHashMap<String, String>>();
					LinkedHashMap<String, String> operatormap = new LinkedHashMap<String, String>();

					operatormap.put("percent75", map.get("percent75") + "");
					operatormap.put("percent85", map.get("percent85") + "");
					operatormap.put("percent95", map.get("percent95") + "");
					operatormap.put("operator", map.get("operator") + "");
					operatormap.put("avgvalue", map.get("avgvalue") + "");
					operatormap.put("successratevalue", map.get("successratevalue") + "");
					newprobetypeList.add(operatormap);
					oldkpimap.put(map.get("probetype") + "", newprobetypeList);
					datamap.put(map.get("kpiname") + "", oldkpimap);
				}

			} else {
				LinkedHashMap<String, List<LinkedHashMap<String, String>>> newkpimap = new LinkedHashMap<String, List<LinkedHashMap<String, String>>>();
				List<LinkedHashMap<String, String>> newprobetypeList = new ArrayList<LinkedHashMap<String, String>>();
				LinkedHashMap<String, String> operatormap = new LinkedHashMap<String, String>();

				operatormap.put("percent75", map.get("percent75") + "");
				operatormap.put("percent85", map.get("percent85") + "");
				operatormap.put("percent95", map.get("percent95") + "");
				operatormap.put("operator", map.get("operator") + "");
				operatormap.put("avgvalue", map.get("avgvalue") + "");
				operatormap.put("successratevalue", map.get("successratevalue") + "");
				newprobetypeList.add(operatormap);
				newkpimap.put(map.get("probetype") + "", newprobetypeList);
				datamap.put(map.get("kpiname") + "", newkpimap);
			}
		}

		
		JSONObject datajson = new JSONObject();
		JSONObject probetypejson = new JSONObject();

		for (Entry<String, LinkedHashMap<String, List<LinkedHashMap<String, String>>>> entry : datamap.entrySet()) {
			datajson.put("kpitypename", entry.getKey());

			 LinkedHashMap<String, List<LinkedHashMap<String, String>>> kpimap = entry.getValue();

			 JSONArray probetypearray = new JSONArray();
			for (Entry<String,  List<LinkedHashMap<String, String>>> kpi : kpimap.entrySet()) {
				probetypejson.put("probename", kpi.getKey());
				probetypejson.put("probedata", kpi.getValue());
				probetypearray.add(probetypejson);
			}
			datajson.put("kpidata", probetypearray);
			//cityarray.add(datajson);
		}

		//jsonObject.put("data", cityarray);
		System.out.println(datajson.toString());
		return datajson;
	}

	
}
