package com.qualitymap.service.impl;

import java.io.IOException;
import java.io.InputStream;
import java.text.DecimalFormat;
import java.text.Format;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

import javax.annotation.Resource;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import com.qualitymap.dao.OverviewProvinceDao;
import com.qualitymap.service.OverviewProvinceService;
import com.qualitymap.utils.UtilDate;
import com.qualitymap.vo.OverviewProvince;

/**
 * 省级纬度数据分析
 * 
 * @author：kxc
 * @date：Apr 12, 2016
 */
public class OverviewProvinceServiceImpl implements OverviewProvinceService {

	@Resource
	private OverviewProvinceDao provinceDao;
	private static Pattern pattern = Pattern.compile("[,]");
	private static Pattern pattern1 = Pattern.compile("[;]");

	@Override
	public String findByMonth(String month, String group) {
		// TODO Auto-generated method stub
		JSONObject dataJSON = new JSONObject();
		JSONArray dataarray = new JSONArray();
		if (!"".equals(group)) {

			List<OverviewProvince> provinceList = provinceDao.findByMonth(month, group);

			dataarray = JSONArray.fromObject(provinceList);
			dataJSON.put("data", dataarray);
		} else {
			dataJSON.put("data", dataarray);
		}

		return dataJSON.toString();
	}

	/**
	 * 查询所有的数据
	 */
	@Override
	public String findProvice(String groupid, String month) {

		JSONObject datajson = new JSONObject();
		JSONArray dataarray = new JSONArray();

		if (!"".equals(groupid)) {

			List<OverviewProvince> provinceList = provinceDao.findProvice(groupid, month);

			dataarray = JSONArray.fromObject(provinceList);
		}
		datajson.put("data", dataarray);

		return datajson.toString();
	}

	/**
	 * 获取签约带宽占比
	 * 
	 * @param month
	 * @param groupid
	 * @return
	 * @return String
	 */
	@Override
	public String getBroadbandData(String month, String groupid) {

		JSONObject dataJSON = new JSONObject();
		JSONArray dataarray = new JSONArray();
		if (!"".equals(groupid)) {

			List<Map<String, Object>> provinceList = provinceDao.getBroadbandData(month, groupid);

			dataarray = JSONArray.fromObject(provinceList);
			dataJSON.put("data", dataarray);
		} else {
			dataJSON.put("data", dataarray);
		}

		return dataJSON.toString();
	}

	/**
	 * 获取各省用户占比
	 */
	@Override
	public String getProvinceUserPercent(String month, String groupid) {

		JSONObject dataJSON = new JSONObject();
		JSONArray newuserNumarray = new JSONArray();
		JSONArray provincearray = new JSONArray();
		if (!"".equals(groupid)) {

			List<Map<String, Object>> provinceList = provinceDao.getProvinceUserPercent(month, groupid);

			for (Iterator iterator = provinceList.iterator(); iterator.hasNext();) {
				Map<String, Object> map = (Map<String, Object>) iterator.next();

				String userNum = map.get("new_user_num").toString();
				String province = map.get("province").toString();
				newuserNumarray.add(userNum);
				provincearray.add(province);
			}
		}

		dataJSON.put("lables", provincearray);
		dataJSON.put("data", newuserNumarray);
		return dataJSON.toString();
	}

	/**
	 * 获取宽带类型累计和本月用户数
	 */
	@Override
	public String getBroadbandTypeData(String month, String groupid) {
		JSONObject dataJSON = new JSONObject();
		JSONArray dataarray = new JSONArray();
		if (!"".equals(groupid)) {
			List<Map<String, Object>> queryList = provinceDao.getBroadbandTypeData(month, groupid);
			for (Iterator iterator = queryList.iterator(); iterator.hasNext();) {
				Map<String, Object> map = (Map<String, Object>) iterator.next();
				if((map.get("thisMonth")+"").equals("null")){
					
				}
				
			}
			dataarray = JSONArray.fromObject(queryList);
		}
		dataJSON.put("data", dataarray);
		System.out.println(dataJSON.toString());
		return dataJSON.toString();
	}

	/**
	 * 根据groupid，month获取样本总数
	 */
	@Override
	public String getTotalSampleNum(String month, String groupid, String broadType) {

		JSONObject dataJSON = new JSONObject();

		/*if (!"".equals(groupid)) {
			String new_sample_num = provinceDao.getTotalSampleNum(month, groupid, broadType);
			dataJSON.put("new_sample_num", new_sample_num);
		} else {
			dataJSON.put("new_sample_num", "");
		}*/
		return dataJSON.toString();
	}

	/**
	 * 根据groupid获取用户(报告中的用户字段)
	 */
	@Override
	public String getUsernumByGroupId(String month, String groupid, String broadType) {

		JSONObject dataJSON = new JSONObject();

		if (!"".equals(groupid)) {
			String new_user_num = provinceDao.getUsernumByGroupId(month, groupid, broadType);
			dataJSON.put("new_user_num", new_user_num);
		} else {
			dataJSON.put("new_user_num", "");
		}
		return dataJSON.toString();
	}

	/**
	 * 根据groupid获取终端(报告中的终端字段)
	 */
	@Override
	public String getTerminalnumByGroupId(String month, String groupid, String broadType) {

		JSONObject dataJSON = new JSONObject();

		if (!"".equals(groupid)) {
			String new_terminal_num = provinceDao.getTerminalnumByGroupId(month, groupid, broadType);
			dataJSON.put("new_terminal_num", new_terminal_num);
		} else {
			dataJSON.put("new_terminal_num", "");
		}
		return dataJSON.toString();
	}

	/**
	 * 获取测试报告结果详细数据
	 */
	@Override
	public String getReportItem(String month, String groupid, String broadType) {

		JSONObject dataJSON = new JSONObject();
		Format fm = new DecimalFormat("#");
		JSONArray terminalarray = new JSONArray();
		List<Map<String, Object>> dataList = new ArrayList<Map<String, Object>>();

		Map dataMap = new HashMap();
		Map numMap = new HashMap();
		Map<String, String> cityMap = new HashMap<String, String>();

		Pattern pattern = Pattern.compile("[,]");
		Pattern pattern1 = Pattern.compile("[$]");
		Pattern pattern2 = Pattern.compile("[|]");
		if (!"".equals(groupid)) {

			// 查询获取各个测试用例的数据
			List<Map<String, Object>> webdelayList = provinceDao.getwebdelaydata(groupid, month, broadType);
			List<Map<String, Object>> videodelayList = provinceDao.getvideodelaydata(groupid, month, broadType);
			List<Map<String, Object>> webrateList = provinceDao.getwebratedata(groupid, month, broadType);
			List<Map<String, Object>> videocacheList = provinceDao.getvideocachedata(groupid, month, broadType);
			List<Map<String, Object>> httpuploadList = provinceDao.gethttpuploaddata(groupid, month, broadType);
			List<Map<String, Object>> httpdownloadList = provinceDao.gethttpdownloaddata(groupid, month, broadType);
			List<Map<String, Object>> videoproportionList = provinceDao.getvideoproportiondata(groupid, month, broadType);
			List<Map<String, Object>> webninetydelayList = provinceDao.getwebninetydelaydata(groupid, month, broadType);
			// 把各个测试用例的数据放入一个集合中
			dataList.addAll(webrateList);
			dataList.addAll(webdelayList);
			dataList.addAll(videocacheList);
			dataList.addAll(videodelayList);
			dataList.addAll(httpuploadList);
			dataList.addAll(httpdownloadList);
			dataList.addAll(videoproportionList);
			dataList.addAll(webninetydelayList);

			String citys = "";
			int city_num = 0;
			// 遍历该总集合
			for (Iterator iterator = dataList.iterator(); iterator.hasNext();) {
				Map<String, Object> map = (Map<String, Object>) iterator.next();
				// 获取参数值
				String typename = map.get("typename").toString();
				String orgname = map.get("orgname").toString();
				String value = map.get("value").toString();
				String type = map.get("type").toString();
				double new_sample_num = Double.parseDouble(map.get("new_sample_num").toString());
				double case_num = Double.parseDouble(map.get("case_num").toString());

				// 重新计算样本数及用例数，并以城市名为key存放map中
				if (numMap.containsKey(orgname)) {
					String nums = (String) numMap.get(orgname);
					String d[] = pattern.split(nums);
					double pre_sample = Double.parseDouble(d[0]);
					double pre_case = Double.parseDouble(d[1]);
					double sample = pre_sample + new_sample_num;
					double case_ = pre_case + case_num;
					numMap.put(orgname, sample + "," + case_);
				} else {
					numMap.put(orgname, new_sample_num + "," + case_num);
				}
				// 把相应的测试用例的数据放入不同的map中，并把该map以城市为key存放入map中
				if (dataMap.containsKey(orgname)) {

					Map<String, String> mapdats = new HashMap<String, String>();

					String s = (String) dataMap.get(orgname);

					dataMap.put(orgname, s + "$" + typename + "|" + type + "," + value);
				} else {
					dataMap.put(orgname, typename + "|" + type + "," + value);
					citys += orgname + ",";
					city_num++;
				}
			}

			if (citys.indexOf(",") > 0) {
				cityMap.put("citys", citys.substring(0, citys.lastIndexOf(",")));
				cityMap.put("city_num", city_num + "");
			}

			// 对总集合map进行遍历取值
			Iterator<Map.Entry<String, String>> it = dataMap.entrySet().iterator();
			JSONObject fajson = new JSONObject();
			while (it.hasNext()) {
				JSONObject childjson = new JSONObject();
				Map.Entry<String, String> entry = it.next();
				// 最外层为城市维度
				childjson.put("city", entry.getKey());
				String numvalue = (String) numMap.get(entry.getKey());
				String numvs[] = pattern.split(numvalue);
				// System.out.println(fm.format("10.2"));
				childjson.put("new_sample_num", (int) (Double.parseDouble(numvs[0])));

				String ca_num = "";
				if ((int) (Double.parseDouble(numvs[1])) == 0) {
					ca_num = "N/A";
				} else {
					ca_num = (int) (Double.parseDouble(numvs[1])) + "";
				}
				childjson.put("case_num", ca_num);

				String ss = entry.getValue();
				JSONObject sunjson = new JSONObject();
				String children[] = pattern1.split(ss);
				JSONArray dagaarry = new JSONArray();

				// List list = new ArrayList();
				List uplist = new ArrayList();
				List downlist = new ArrayList();
				List vdelaylist = new ArrayList();
				List vcanchlist = new ArrayList();
				List wdelaylist = new ArrayList();
				List wratelist = new ArrayList();
				List wninetydelaylist = new ArrayList();
				List vproportionlist = new ArrayList();

				Map sq = new HashMap();
				for (int i = 0; i < children.length; i++) {
					String string = children[i];
					String[] grandsons = pattern2.split(string);
					String typena = grandsons[0];
					// sunjson.put("typename", grandsons[0]);
					Map dmap = new HashMap();

					String greatgrandsons[] = pattern.split(grandsons[1]);
					dmap.put("probetype", greatgrandsons[0]);
					dmap.put("value", greatgrandsons[1]);
					if ("上传速率".equals(typena)) {
						uplist.add(dmap);
						sq.put(typena, uplist);
					} else if ("下载速率".equals(typena)) {
						downlist.add(dmap);
						sq.put(typena, downlist);
					} else if ("页面元素打开成功率".equals(typena)) {
						wratelist.add(dmap);
						sq.put(typena, wratelist);
					} else if ("页面显示时延".equals(typena)) {
						wdelaylist.add(dmap);
						sq.put(typena, wdelaylist);
					} else if ("视频卡顿次数".equals(typena)) {
						vcanchlist.add(dmap);
						sq.put(typena, vcanchlist);
					} else if ("视频加载时长".equals(typena)) {
						vdelaylist.add(dmap);
						sq.put(typena, vdelaylist);
					} else if ("90%页面时延".equals(typena)) {
						wninetydelaylist.add(dmap);
						sq.put(typena, wninetydelaylist);
					} else if ("视频缓冲占比".equals(typena)) {
						vproportionlist.add(dmap);
						sq.put(typena, vproportionlist);
					}

				}
				Iterator<Map.Entry<String, List>> s = sq.entrySet().iterator();
				while (s.hasNext()) {
					Map.Entry<String, List> entrys = s.next();
					sunjson.put("typename", entrys.getKey());
					sunjson.put("data", entrys.getValue());
					dagaarry.add(sunjson);
				}

				childjson.put("item", dagaarry);
				terminalarray.add(childjson);
			}
		}
		dataJSON.put("citydata", cityMap);
		dataJSON.put("values", terminalarray);
		System.out.println(dataJSON.toString());
		return dataJSON.toString();
	}

	/**
	 * 获取页面测试报告结果以探针类型维度的详细信息（该方法获取数据的方式与一方法相同）
	 * 
	 * @param month
	 * @param groupid
	 * @param broadType
	 * @return
	 */
	@Override
	public String getProbetypeReportItem(String month, String groupid, String broadType) {

		JSONObject dataJSON = new JSONObject();

		Pattern pattern = Pattern.compile("[,]");
		Pattern pattern1 = Pattern.compile("[$]");
		Pattern pattern2 = Pattern.compile("[|]");

		JSONArray terminalarray = new JSONArray();
		List<Map<String, Object>> dataList = new ArrayList<Map<String, Object>>();
		Map<String, Map> mapdat = new HashMap<String, Map>();

		Map dataMap = new HashMap();
		Map numMap = new HashMap();
		Format fm = new DecimalFormat("#");
		Map<String, String> uploadMap = new HashMap<String, String>();

		if (!"".equals(groupid)) {

			List<Map<String, Object>> webdelayList = provinceDao.getdelaywebdata(groupid, month, broadType);
			List<Map<String, Object>> videodelayList = provinceDao.getdelayvideodata(groupid, month, broadType);
			List<Map<String, Object>> webrateList = provinceDao.getratewebdata(groupid, month, broadType);
			List<Map<String, Object>> videocacheList = provinceDao.getcachevideodata(groupid, month, broadType);
			List<Map<String, Object>> webninetydelayList = provinceDao.getdelayninetywebdata(groupid, month, broadType);
			List<Map<String, Object>> videoproportionList = provinceDao.getproportionvideodata(groupid, month, broadType);
			List<Map<String, Object>> httpuploadList = provinceDao.getuploadhttpdata(groupid, month, broadType);
			List<Map<String, Object>> httpdownloadList = provinceDao.getdownloadhttpdata(groupid, month, broadType);

			dataList.addAll(webrateList);
			dataList.addAll(webdelayList);
			dataList.addAll(videocacheList);
			dataList.addAll(videodelayList);
			dataList.addAll(httpuploadList);
			dataList.addAll(httpdownloadList);
			dataList.addAll(webninetydelayList);
			dataList.addAll(videoproportionList);

			for (Iterator iterator = dataList.iterator(); iterator.hasNext();) {
				Map<String, Object> map = (Map<String, Object>) iterator.next();
				Map vdmap = new HashMap();
				String typename = map.get("typename").toString();
				String value = map.get("value").toString();
				String type = map.get("type").toString();

				double new_sample_num = Double.parseDouble(map.get("new_sample_num").toString());
				double case_num = Double.parseDouble(map.get("case_num").toString());

				// 重新计算样本数及用例数，并以城市名为key存放map中
				if (numMap.containsKey(type)) {
					String nums = (String) numMap.get(type);
					String d[] = pattern.split(nums);
					double pre_sample = Double.parseDouble(d[0]);
					double pre_case = Double.parseDouble(d[1]);
					double sample = pre_sample + new_sample_num;
					double case_ = pre_case + case_num;
					numMap.put(type, sample + "," + case_);
				} else {
					numMap.put(type, new_sample_num + "," + case_num);
				}

				/*
				 * if (dataMap.containsKey(type)) { uploadMap.put(typename,
				 * value); } else { uploadMap.put(typename, value);
				 * dataMap.put(type, uploadMap); }
				 */

				if (dataMap.containsKey(type)) {

					String s = (String) dataMap.get(type);
					dataMap.put(type, s + "$" + typename + "," + value);
				} else {
					dataMap.put(type, typename + "," + value);
				}
			}

			Iterator<Map.Entry<String, String>> it = dataMap.entrySet().iterator();
			JSONObject fajson = new JSONObject();
			while (it.hasNext()) {
				JSONObject childjson = new JSONObject();
				Map.Entry<String, String> entry = it.next();
				childjson.put("probetype", entry.getKey());

				String numvalue = (String) numMap.get(entry.getKey());
				String numvs[] = pattern.split(numvalue);
				childjson.put("new_sample_num", (int) (Double.parseDouble(numvs[0])));

				String ca_num = "";
				if ((int) (Double.parseDouble(numvs[1])) == 0) {
					ca_num = "N/A";
				} else {
					ca_num = (int) (Double.parseDouble(numvs[1])) + "";
				}
				childjson.put("case_num", ca_num);

				// Map valuemap = entry.getValue();
				String ss = entry.getValue();
				JSONObject sunjson = new JSONObject();
				String children[] = pattern1.split(ss);
				JSONArray dagaarry = new JSONArray();
				for (int i = 0; i < children.length; i++) {
					String string = children[i];
					String[] grandsons = pattern.split(string);
					sunjson.put("typename", grandsons[0]);
					sunjson.put("value", grandsons[1]);
					List list = new ArrayList();
					String greatgrandsons[] = pattern.split(grandsons[1]);

					Map dmap = new HashMap();

					// dmap.put("typename", greatgrandsons[0]);
					/*
					 * dmap.put("value", greatgrandsons[1]); list.add(dmap);
					 */

					// sunjson.put("data", list);
					dagaarry.add(sunjson);
				}

				childjson.put("item", dagaarry);
				terminalarray.add(childjson);

				/*
				 * Iterator<Map.Entry<String, Map>> values =
				 * valuemap.entrySet().iterator(); JSONArray dagaarry = new
				 * JSONArray(); while (values.hasNext()) { JSONObject sunjson =
				 * new JSONObject(); Map vdmap = new HashMap();
				 * Map.Entry<String, Map> valuedata = values.next();
				 * 
				 * sunjson.put("typename", valuedata.getKey());
				 * sunjson.put("value", valuedata.getValue());
				 * dagaarry.add(sunjson); } childjson.put("item", dagaarry);
				 * terminalarray.add(childjson);
				 */
			}
		}
		dataJSON.put("values", terminalarray);
		System.out.println(dataJSON.toString());
		return dataJSON.toString();
	}

	/**
	 * 根据groupid获取有效样本
	 */
	@Override
	public String getValidSampleNum(String month, String groupid, String broadType) {

		JSONObject dataJSON = new JSONObject();

		if (!"".equals(groupid)) {
			String ping_test_times = provinceDao.getValidSampleNum(month, groupid, broadType);
			dataJSON.put("ping_test_times", ping_test_times);
		} else {
			dataJSON.put("ping_test_times", "");
		}
		return dataJSON.toString();
	}

	/**
	 * 获取质量分析 即 上下月中数据的增减情况
	 */
	/*
	 * @Override public String getServiceQualityCompare(String month, String
	 * groupid, String broadband_type) { String lastMonth =
	 * UtilDate.getPreviousMonth(month);
	 * 
	 * JSONObject avgJson = new JSONObject(); JSONObject dataJson = new
	 * JSONObject(); JSONArray dataarray = new JSONArray();
	 * 
	 * if (!"".equals(groupid)) { List<Map<String, Object>> httpdownloadList =
	 * provinceDao.getHttpdownloadServiceQualityCompare(month, lastMonth,
	 * groupid, broadband_type); List<Map<String, Object>> vidoList =
	 * provinceDao.getVideoServiceQualityCompare(month, lastMonth, groupid,
	 * broadband_type); List<Map<String, Object>> webList =
	 * provinceDao.getWebbrowsingServiceQualityCompare(month, lastMonth,
	 * groupid, broadband_type);
	 * 
	 * if (httpdownloadList.size() > 0) {
	 * 
	 * for (Iterator iterator = httpdownloadList.iterator();
	 * iterator.hasNext();) { Map<String, Object> map = (Map<String, Object>)
	 * iterator.next();
	 * 
	 * avgJson.put("probetype", map.get("probetype")); avgJson.put("testtype",
	 * "下载速率"); String defferentials = map.get("differentials") + ""; if
	 * (defferentials.isEmpty() || defferentials == null ||
	 * defferentials.equals("null")) { avgJson.put("data", "N/A"); } else { if
	 * (defferentials.contains("-")) { avgJson.put("data", "减少" +
	 * Math.abs(Double.parseDouble(defferentials))); } else {
	 * avgJson.put("data", "增加" + defferentials); } } dataarray.add(avgJson);
	 * 
	 * }
	 * 
	 * } if (vidoList.size() > 0) {
	 * 
	 * for (Iterator iterator = vidoList.iterator(); iterator.hasNext();) {
	 * Map<String, Object> map = (Map<String, Object>) iterator.next();
	 * avgJson.put("probetype", map.get("probetype")); avgJson.put("testtype",
	 * "视频缓冲时长占比"); String defferentials = map.get("differentials") + ""; if
	 * (defferentials.isEmpty() || defferentials == null ||
	 * defferentials.equals("null")) { avgJson.put("data", "N/A"); } else { if
	 * (defferentials.contains("-")) { avgJson.put("data", "减少" +
	 * Math.abs(Double.parseDouble(defferentials))); } else {
	 * avgJson.put("data", "增加" + defferentials); } } dataarray.add(avgJson); }
	 * 
	 * } if (webList.size() > 0) {
	 * 
	 * for (Iterator iterator = webList.iterator(); iterator.hasNext();) {
	 * Map<String, Object> map = (Map<String, Object>) iterator.next();
	 * 
	 * avgJson.put("probetype", map.get("probetype")); avgJson.put("testtype",
	 * "90%页面时延"); String defferentials = map.get("differentials") + ""; if
	 * (defferentials.isEmpty() || defferentials == null ||
	 * defferentials.equals("null")) { avgJson.put("data", "N/A"); } else { if
	 * (defferentials.contains("-")) { avgJson.put("data", "减少" +
	 * Math.abs(Double.parseDouble(defferentials))); } else {
	 * avgJson.put("data", "增加" + defferentials); } } dataarray.add(avgJson);
	 * 
	 * }
	 * 
	 * } } dataJson.put("data", dataarray); return dataJson.toString(); }
	 */

	@Override
	public String getServiceQualityCompare(String month, String groupid, String broadband_type) {
		String lastMonth = UtilDate.getPreviousMonth(month);

		JSONObject avgJson = new JSONObject();
		JSONObject dataJson = new JSONObject();
		JSONArray dataarray = new JSONArray();

		DecimalFormat df = null;

		Map<String, List<Map<String, String>>> typemap = new HashMap<String, List<Map<String, String>>>();

		Properties properties = new Properties();
		InputStream in = OverviewProvinceServiceImpl.class.getClassLoader().getResourceAsStream("config/common.properties");

		for (int i = 0; i < 100; i++) {
			String string = "";
			try {
				properties.load(in);
				string = properties.getProperty("paramter" + (i + 1));

			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			if (string != null && !string.isEmpty()) {
				String data[] = pattern.split(string);

				String probetype = data[0];
				String fieldName = data[1];
				String tableName = data[2];
				String dataq = data[3];
				String datatype = data[4];

				String bestfielddata = "";

				if (fieldName.contains("delay")) {
					df = new DecimalFormat("#");
				} else if (fieldName.contains("proportion") || fieldName.contains("download_rate")) {
					df = new DecimalFormat("#.00");
				}

				String datas = "";
				if (!"".equals(groupid)) {
					List<Map<String, Object>> dataList = provinceDao.getServiceQualityCompare(probetype, fieldName, tableName, dataq, datatype, month, lastMonth, groupid, broadband_type);

					if (dataList.size() > 0) {
						Map<String, Object> map = dataList.get(0);
						String avg_data = map.get("avg_data") + "";
						String pre_avg_data = map.get("pre_avg_data") + "";

						if (avg_data == null || avg_data.isEmpty() || avg_data.equals("null")) {
							datas = "(本月无值)";// ,上月值为"+pre_avg_data+"
						} else if (pre_avg_data == null || pre_avg_data.isEmpty() || pre_avg_data.equals("null")) {
							datas = "(上月无值)";// ,本月值为"+avg_data+"
						} else {
							datas = (Double.parseDouble(avg_data) - Double.parseDouble(pre_avg_data)) + "";

							String fielddata = "";

							if (fieldName.contains("delay")) {
								fielddata = df.format(Math.abs(Double.parseDouble(datas))) + "ms";
							} else if (fieldName.contains("proportion")) {
								if (Double.parseDouble(datas) == 0) {
									fielddata = "0";
								} else {
									fielddata = (df.format(Math.abs(Double.parseDouble(datas)) * 100)) + "%";
								}
							} else if (fieldName.contains("download_rate")) {
								if (Double.parseDouble(datas) == 0) {
									fielddata = "0 Mbps";
								} else {
									fielddata = (df.format(Math.abs(Double.parseDouble(datas)))) + "Mbps";
								}
							} else {
								fielddata = Math.abs(Double.parseDouble(datas)) + "";
							}

							if (datas.contains("-")) {
								datas = "减少" + fielddata;
							} else {
								datas = "增加" + fielddata;
							}
						}

						if (typemap.containsKey(probetype)) {

							List<Map<String, String>> maplist = typemap.get(probetype);
							Map amap = new HashMap();
							amap.put("data", datas);
							amap.put("datatype", datatype);
							maplist.add(amap);
							typemap.put(probetype, maplist);
						} else {
							List<Map<String, String>> maplist = new ArrayList<Map<String, String>>();
							Map datamap = new HashMap();
							datamap.put("data", datas);
							datamap.put("datatype", datatype);
							maplist.add(datamap);
							typemap.put(probetype, maplist);
						}

					} else {
						datas = "(两个月无值)";
					}
				}
			} else {
				break;
			}
		}
		JSONObject json = new JSONObject();

		for (Map.Entry<String, List<Map<String, String>>> type : typemap.entrySet()) {

			dataJson.put("typename", type.getKey());
			dataJson.put("data", type.getValue());
			dataarray.add(dataJson);
		}

		json.put("data", dataarray);
		System.out.println(json.toString());
		return json.toString();
	}

	/**
	 * 
	 * 获取各类型指标情况
	 * 
	 */
	@Override
	public String getKPIbyGroupid(String month, String groupid, String broadband_type) {

		JSONArray dataArray = new JSONArray();

		JSONObject dataJson = new JSONObject();

		Properties properties = new Properties();
		InputStream in = OverviewProvinceServiceImpl.class.getClassLoader().getResourceAsStream("config/common.properties");

		for (int i = 0; i < 100; i++) {
			String string = "";
			try {
				properties.load(in);
				string = properties.getProperty("paramter" + (i + 1));

			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			if (string != null && !string.isEmpty()) {
				String data[] = pattern.split(string);

				String probetype = data[0];
				String fieldName = data[1];
				String tableName = data[2];
				String dataq = data[3];
				String datatype = data[4];
				String field85 = data[5];

				String usertype = "";
				Map provinceMap = new HashMap();
				Map bestResultMap = new HashMap();

				if (probetype.equals("Android")) {
					usertype = "安卓";
				} else if (probetype.equals("iOS")) {
					usertype = "苹果";
				} else if (probetype.equals("PC")) {
					usertype = "桌面";
				}

				String ss = "";
				if (dataq.contains(">=")) {
					ss = "max";
				} else {
					ss = "min";
				}

				String fielddata = "";
				String bestfielddata = "";

				if (field85.contains("delay")) {
					fielddata = "CONCAT(round(" + field85 + ",0) ,'ms') ";
					bestfielddata = "CONCAT(round(" + ss + "(abs(" + field85 + ")), 0),'ms')";
				} else if (field85.contains("proportion")) {
					fielddata = "CONCAT(round(" + field85 + ",2)*100 ,'%') ";
					bestfielddata = "CONCAT(round(" + ss + "(abs(" + field85 + ")), 2)*100,'%')";
				} else if (field85.contains("download_rate")) {
					fielddata = "CONCAT(round(" + field85 + ",2) ,'Mbps') ";
					bestfielddata = "CONCAT(round(" + ss + "(abs(" + field85 + ")), 2),'Mbps')";
				} else {
					fielddata = field85;
					bestfielddata = "" + ss + "(abs(" + field85 + "))";
				}

				if (!"".equals(groupid)) {

					List<Map<String, Object>> provinceResList = provinceDao.getProvinceResult(probetype, fielddata, tableName, dataq, datatype, month, groupid, broadband_type);
					if (provinceResList.size() > 0) {
						provinceMap = provinceResList.get(0);
					}

					List<Map<String, Object>> bestResList = provinceDao.getBestResult(probetype, bestfielddata, tableName, dataq, datatype, month, broadband_type);
					if (bestResList.size() > 0) {
						bestResultMap = bestResList.get(0);
					}
					if (provinceResList.size() == 0) {
						provinceMap.put("avg_order", "N/A");
						provinceMap.put("datatype", datatype);
						provinceMap.put("usertype", usertype);
						provinceMap.put("avg_data", "N/A");
						// provinceMap.put("best_data", "N/A");
					} else {

						// provinceMap.putAll(bestResultMap);
						String avg_order = provinceDao.getResultOrder(probetype, field85, tableName, dataq, datatype, month, groupid, broadband_type);
						provinceMap.put("avg_order", avg_order);
						provinceMap.put("datatype", datatype);
						provinceMap.put("usertype", usertype);
					}

					if (bestResList.size() > 0) {
						if (bestResList.get(0).get("best_data") == null || bestResList.get(0).get("best_data").equals("null")) {

							provinceMap.put("best_data", "N/A");
						} else {

							provinceMap.putAll(bestResultMap);
						}
					} else {
						provinceMap.put("best_data", "N/A");
					}
					dataArray.add(provinceMap);

				}
			} else {
				break;
			}

		}
		dataJson.put("data", dataArray);
		System.out.println(dataJson.toString());

		return dataJson.toString();
	}

	/**
	 * 根据groupid的个数获取判断是全国还是返回各省名称
	 */
	@Override
	public String getProvinceName(String groupid) {
		JSONObject dataJson = new JSONObject();
		String groupname = "";
		List<Map<String, Object>> groupnameList = provinceDao.getProvinceName(groupid);
		if (groupnameList.size() > 1) {
			groupname = "全国";
		} else {
			groupname = (String) groupnameList.get(0).get("groupname");
		}

		dataJson.put("groupname", groupname);

		return dataJson.toString();
	}
}
