package com.opencassandra.jiakuan.servicequality.servicequalitybase;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.opencassandra.descfile.ConfParser;
import com.opencassandra.jiakuan.base.BaseDaoImp;
import com.opencassandra.service.utils.UtilDate;

public class ServicequalityVideoBaseDao extends BaseDaoImp {

	public static List<Map> groupList = new ArrayList<Map>();

	/**
	 * 根据探针类型对数据进行分类处理
	 * 
	 * @param dataSourceTable
	 * @param dataSourcebasename
	 * @param tarTableName
	 * @param month
	 * @param targetBaseName
	 * @param probetype
	 * @return
	 */
	public List<Map> getDataByprobetype(String dataSourceTable, String dataSourcebasename, String tarTableName, String month, String targetBaseName, String probetype, String authBaseName,String whereParameter, String groupParameter) {
		List<Map> listMap = new ArrayList<Map>();

		if ("pc".equals(probetype)) {
			listMap = getDataPc(dataSourceTable, dataSourcebasename, tarTableName, month, targetBaseName, probetype, authBaseName);
		} else if ("app".equals(probetype)) {
			listMap = getDataApp(dataSourceTable, dataSourcebasename, tarTableName, month, targetBaseName, probetype, authBaseName,whereParameter,groupParameter);
		} 
		
		return listMap;
	}

	/**
	 * 根据类型获取数据
	 * 
	 * @param srctableName
	 * @param month
	 * @param pointer
	 * @param uparea
	 * @param area
	 * @param mhinterval
	 * @return
	 * @return List<Map>
	 */
	public List<Map> getDataPc(String dataSourceTable, String dataSourcebasename, String tarTableName, String month, String targetBaseName, String probetype, String authBaseName) {

		List<Map> listMap = new ArrayList<Map>();

		String sql = "";
		// String wherequery = "where " + area + " !='' and " + area + "!='-' ";

		String nextMonth = UtilDate.getNextMonth(month);
		String groupidsql = "(SELECT t.org_key, m.groupid,	m. NAME	FROM " + authBaseName + ".t_org t, ( SELECT j.org_id,j.groupid,i.name from " + authBaseName
				+ ".t_group_org_mapping i , (SELECT org_id, groupid" + " 	FROM " + authBaseName + ".t_group_org_mapping a, " + targetBaseName
				+ ".group_id b WHERE a.group_id = b.groupid ) j where j.groupid =i.parent_org_id  ) m WHERE t.id = m.org_id) n";

		sql = "select n.groupid groupid ,n.name groupname, '"
				+ month
				+ "' month,p.device_org org_id,COUNT(id) AS page_test_times,round(avg(CONVERT(loading_delay,decimal(10,2))),0)  page_avg_delay,round(avg(CONVERT(success_rate,decimal(10,2))),2) page_success_rate,round(min(CONVERT(loading_delay,decimal(10,2))),0) best_page_delay ,round(max(CONVERT(success_rate,decimal(10,2))),2) best_page_success_rate,p.bandwidth broadband_type"
				+ " from "
				+ dataSourceTable
				+ " p , "
				+ groupidsql
				+ "   where 	consumerid != '' AND businessid != '' AND p.bandwidth != '' AND mac != '' AND start_time >= UNIX_TIMESTAMP('"
				+ month
				+ "01') * 1000 AND start_time < UNIX_TIMESTAMP('"
				+ nextMonth
				+ "01') * 1000 and cast(loading_delay AS signed)>0 and cast(success_rate as signed) >=0 and cast(success_rate as signed) <=100 and p.device_org = n.org_key  GROUP BY p.device_org,p.bandwidth";

		ResultSet rs = null;
		String url = srcurl + dataSourcebasename;

		System.out.println(sql);
		try {
			start(url, dstuser, dstpassword);

			rs = statement.executeQuery(sql);
			while (rs.next()) {
				Map map = new HashMap();
				Map groupmap = new HashMap();

				String groupid = rs.getString("groupid");
				String groupname = rs.getString("groupname");
				String page_test_times = rs.getString("page_test_times");
				String page_avg_delay = rs.getString("page_avg_delay");
				String page_success_rate = rs.getString("page_success_rate");
				String best_page_delay = rs.getString("best_page_delay");
				String best_page_success_rate = rs.getString("best_page_success_rate");
				String broadband_type = rs.getString("broadband_type");

				map.put("groupid", groupid);
				map.put("groupname", groupname);
				map.put("page_test_times", page_test_times);
				map.put("page_avg_delay", page_avg_delay);
				map.put("page_success_rate", page_success_rate);
				map.put("best_page_delay", best_page_delay);

				map.put("best_page_success_rate", best_page_success_rate);
				map.put("month", month);
				map.put("broadband_type", broadband_type);
				map.put("probetype", probetype.toUpperCase());

				groupmap.put("groupid", groupid);
				groupmap.put("broadband_type", broadband_type);
				groupmap.put("percentnum", page_test_times);

				listMap.add(map);

				groupList.add(groupmap);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return listMap;
	}

	/**
	 * 根据类型获取数据
	 * 
	 * @param srctableName
	 * @param month
	 * @param pointer
	 * @param uparea
	 * @param area
	 * @param mhinterval
	 * @return
	 * @return List<Map>
	 */
	public List<Map> getDataApp(String dataSourceTable, String dataSourcebasename, String tarTableName, String month, String targetBaseName, String probetype, String authBaseName ,String whereParameter, String groupParameter) {

		List<Map> listMap = new ArrayList<Map>();

		String sql = "";
		String nextMonth = UtilDate.getNextMonth(month);
		
		String wparameter = "";
		
		String wheredata = "";
		String groupwhere = "";
		String period = "";

		if (whereParameter != null && !whereParameter.isEmpty()) {

			String[] wh = whereParameter.split(",");
			String[] wd = null;
			String[] wd2 = null;
			if(wh.length>1){
				 wd = wh[0].split("-");
				 wd2 = wh[1].split("-");
				 wheredata = "CASE  WHEN  (("+wd[0]+" <=CAST(FROM_UNIXTIME(time / 1000, '%H') AS SIGNED) and  CAST(FROM_UNIXTIME(time / 1000, '%H') AS SIGNED)<= "+wd[1]+") " +
						 "or ("+wd2[0]+" <=CAST(FROM_UNIXTIME(time / 1000, '%H') AS SIGNED) " +
						 "and  CAST(FROM_UNIXTIME(time / 1000, '%H') AS SIGNED)<= "+wd2[1]+")) THEN '忙时' ELSE '闲时' END time_period ,";
				 groupwhere = " ,time_period ";
				 
				 period = "q.time_period = (CASE  WHEN  (("+wd[0]+" <=CAST(FROM_UNIXTIME(time / 1000, '%H') AS SIGNED) and  CAST(FROM_UNIXTIME(time / 1000, '%H') AS SIGNED)<= "+wd[1]+") " +
						 "or ("+wd2[0]+" <=CAST(FROM_UNIXTIME(time / 1000, '%H') AS SIGNED) " +
						 "and  CAST(FROM_UNIXTIME(time / 1000, '%H') AS SIGNED)<= "+wd2[1]+")) THEN '忙时' ELSE '闲时' END) and ";
			}
			
		}

		String gparameter = "";// 分组参数
		String gwhe = "";// 分组引带的where相等条件
		String aliaspar = "";// 分组所引带的取值的及别名参数
		if (groupParameter != null && !groupParameter.isEmpty()) {

			if (groupParameter.equals("operator")) {

				gparameter = " p.operator_manual  , ";
				gwhe = "  and p.operator_manual =q." + groupParameter;
				aliaspar = " p.operator_manual " + groupParameter + " ,";
			} else {
				gparameter = ",p." + groupParameter;
				gwhe = "  and p." + groupParameter + "=q." + groupParameter;
				aliaspar = " " + groupParameter + " ,";
			}
		}

		

		String groupidsql = "(SELECT t.org_key,t.id org, m.groupid,	m. NAME,m.orgname	FROM " + authBaseName + ".t_org t, ( SELECT j.org_id,j.groupid,i.name,j.orgname from " + authBaseName
				+ ".t_group_org_mapping i , (SELECT org_id, groupid,name orgname  " + " 	FROM " + authBaseName + ".t_group_org_mapping a, " + targetBaseName
				+ ".group_id b WHERE a.group_id = b.groupid ) j where j.groupid =i.parent_org_id  ) m WHERE t.id = m.org_id) n";

		String sampleSql = "(select  "+wheredata+" " + aliaspar + "  count(id) new_sample_num ,p.device_org,p.android_ios,p.bandwidth  bandwidth from " + dataSourcebasename + "." + dataSourceTable
				+ " p , " + groupidsql + "  where  buffer_proportion != '' and  time >= UNIX_TIMESTAMP('" + month + "01') * 1000 AND time  < UNIX_TIMESTAMP('" + nextMonth
				+ "01') * 1000  and p.device_org = n.org_key GROUP BY p.device_org,p.bandwidth,p.android_ios "+groupwhere+" ) q ";

		sql = "select  "+wheredata+" " + aliaspar + "  round(avg(substring_index(delay, ',', 1)),0) first_buffer_delay,  COUNT(DISTINCT address) url_num, n.org org, round(avg(CONVERT(buffer_proportion,decimal(10,2))),2) AS avg_buffer_proportion , case p.android_ios when 'ios' then 'iOS' when 'android' then 'Android' end android_ios, COUNT(id)  as video_test_times,q.new_sample_num,'"
				+ month
				+ "' month,p.device_org org_id,n.orgname,n.groupid groupid ,n.name groupname,p.bandwidth bandwidth, round(avg(CONVERT(avg_delay,decimal(10,2))),0) AS avg_video_delay ,round(avg(CONVERT(buffer_times,SIGNED)),1)   as  video_cache_count,round(min(CONVERT(avg_delay,decimal(10,2))),0)  as best_video_delay, min(CONVERT(buffer_times,SIGNED))  best_cache_count,sum(case when delay != -330 then 1 else 0 end)/count(id) as success_rate "
				+ " from "
				+ dataSourcebasename
				+ "."
				+ dataSourceTable
				+ " p , "
				+ groupidsql 
				+ " ,"
				+ sampleSql
				+ "  where "+period+"	p.imei!=''  AND model != '' AND buffer_proportion != '' and UPPER(net_type) = 'WIFI'  AND p.operator_manual != ''  AND p.bandwidth != ''  AND time >= UNIX_TIMESTAMP('"
				+ month
				+ "01') * 1000 AND time < UNIX_TIMESTAMP('"
				+ nextMonth
				+ "01') * 1000 "+ gwhe + " " + wparameter + " and  cast(avg_delay AS signed)>0  and p.device_org = n.org_key  and p.device_org=q.device_org and p.bandwidth=q.bandwidth   and p.android_ios = q.android_ios  GROUP BY p.device_org,p.bandwidth, " + gparameter + " p.android_ios "+groupwhere+"";

		ResultSet rs = null;
		String url = srcurl + dataSourcebasename;

		System.out.println(sql);
		try {
			start(url, dstuser, dstpassword);

			rs = statement.executeQuery(sql);
			while (rs.next()) {
				Map map = new HashMap();
				Map groupmap = new HashMap();
				
				if (groupParameter != null && !groupParameter.isEmpty()) {
					groupmap.put("" + groupParameter + "", rs.getString("" + groupParameter + ""));
					map.put("" + groupParameter + "", rs.getString("" + groupParameter + ""));

				}
				if(whereParameter!= null && !whereParameter.isEmpty()){
					groupmap.put("period", rs.getString("time_period"));
					map.put("period", rs.getString("time_period"));
				}
				
				String groupid = rs.getString("groupid");
				String groupname = rs.getString("groupname");
				String video_test_times = rs.getString("video_test_times");
				String avg_video_delay = rs.getString("avg_video_delay");
				String video_cache_count = rs.getString("video_cache_count");
				String best_cache_count = rs.getString("best_cache_count");
				String best_video_delay = rs.getString("best_video_delay");
				String bandwidth = rs.getString("bandwidth");
				String android_ios = rs.getString("android_ios");
				String org_id = rs.getString("org_id");
				String orgname = rs.getString("orgname");
				String new_sample_num = rs.getString("new_sample_num");
				String avg_buffer_proportion = rs.getString("avg_buffer_proportion");
				String org = rs.getString("org");
				String success_rate = rs.getString("success_rate");
				String url_num = rs.getString("url_num");

				map.put("url_num", url_num);

				map.put("first_buffer_delay", rs.getString("first_buffer_delay"));
				map.put("avg_buffer_proportion", avg_buffer_proportion);
				map.put("new_sample_num", new_sample_num);
				map.put("groupid", groupid);
				map.put("orgid", org_id);
				map.put("orgname", orgname);
				map.put("groupname", groupname);
				map.put("video_test_times", video_test_times);
				map.put("avg_video_delay", avg_video_delay);
				map.put("video_cache_count", video_cache_count);
				map.put("best_cache_count", best_cache_count);

				map.put("best_video_delay", best_video_delay);
				map.put("month", month);
				map.put("broadband_type", bandwidth);
				map.put("probetype", android_ios);
				map.put("success_rate", success_rate);

				groupmap.put("orgid", org_id);
				groupmap.put("org", org);
				groupmap.put("broadband_type", bandwidth);
				groupmap.put("percentnum", video_test_times);
				groupmap.put("probetype", android_ios);
				listMap.add(map);

				groupList.add(groupmap);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return listMap;
	}

	/**
	 * 根据类型获取数据
	 * 
	 * @param srctableName
	 * @param month
	 * @param pointer
	 * @param uparea
	 * @param area
	 * @param mhinterval
	 * @return
	 * @return List<Map>
	 */
	public String getPercentData(String dataSourceTable, String dataSourcebasename, String authBaseName, String month, String orgid, String bandwidth, int percentnum, String sourceField,
			String tarField, String targetBaseName, String prtype, String whereParameter, String gparameter,String wherparameter) {

		List<Map> listMap = new ArrayList<Map>();

		String sql = "";
		String order = "";
		if (sourceField.contains("rate")) {
			order = "ORDER BY abs(" + sourceField + ") desc";
		} else {
			order = "ORDER BY abs(" + sourceField + ") asc";
		}

		String round = "";
		if (sourceField.contains("avg_delay") || sourceField.contains("count")) {
			round = "round(" + sourceField + ",0)";
		} else {
			round = "round(" + sourceField + ",2)";
		}

		if(sourceField.equals("delay")){
			round = " substring_index(delay, ',', 1) ";
		}
		
		String period = "";

		if (whereParameter != null && !whereParameter.isEmpty()) {
			String[] wh = whereParameter.split(",");
			String[] wd = null;
			String[] wd2 = null;
			if(wh.length>1){
				 wd = wh[0].split("-");
				 wd2 = wh[1].split("-");
				 
					 period = " and '"+wherparameter+"' = (CASE  WHEN  (("+wd[0]+" <=CAST(FROM_UNIXTIME(time / 1000, '%H') AS SIGNED) and  CAST(FROM_UNIXTIME(time / 1000, '%H') AS SIGNED)<= "+wd[1]+") " +
							 "or ("+wd2[0]+" <=CAST(FROM_UNIXTIME(time / 1000, '%H') AS SIGNED) " +
							 "and  CAST(FROM_UNIXTIME(time / 1000, '%H') AS SIGNED)<= "+wd2[1]+")) THEN '忙时' ELSE '闲时' END) ";
		}
		}
		String grouppar = "";// 分组参数
		if (gparameter != null && !gparameter.isEmpty()) {
			if (dataSourceTable.contains("pc")) {
				grouppar = " and " + ConfParser.groupparameter + "='" + gparameter + "' ";
			} else {
				grouppar = " and p.operator_manual='" + gparameter + "' ";
			}
		}

		
		String nextMonth = UtilDate.getNextMonth(month);

		sql = "select " + round + " " + tarField + "" + " from " + dataSourceTable + " p  where  imei!='' "+period+" AND model != '' AND UPPER(net_type) = 'WIFI'  AND p.bandwidth = '"
				+ bandwidth + "' AND  time >= UNIX_TIMESTAMP('" + month + "01') * 1000 AND time < UNIX_TIMESTAMP('" + nextMonth + "01') * 1000 and  cast(avg_delay AS signed)>0   and p.device_org='"
				+ orgid + "'  and buffer_proportion != '' AND p.operator_manual != ''  and android_ios='" + prtype.toLowerCase() + "'  "+grouppar+" " + order + "  limit " + percentnum + " ,1 ";

		ResultSet rs = null;
		String url = srcurl + dataSourcebasename;

		String data = "";

		System.out.println(sql);
		try {
			start(url, dstuser, dstpassword);

			rs = statement.executeQuery(sql);
			while (rs.next()) {
				Map map = new HashMap();
				Map groupmap = new HashMap();

				data = rs.getString(tarField);

			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return data;
	}

	/**
	 * 查询该月份的数据是否已存�?
	 * 
	 * @param dsttableName
	 * @param dstBaseName
	 * @param month
	 * @return
	 * @return boolean
	 */
	public boolean queryDataExist(String dsttableName, String dstBaseName, String month, String probetype) {
		String consql = "";
		if ("pc".equals(probetype)) {
			consql = "and probetype='" + probetype + "'";
		} else if ("app".equals(probetype)) {
			consql = "and (probetype='ios' or probetype='android')";
		}

		String sql = "select count(id) count from " + dsttableName + " where month = '" + month + "' " + consql + "";
		int count = 0;

		String url = dsturl.substring(0, dsturl.lastIndexOf("/") + 1) + dstBaseName;
		System.out.println(sql + "::::::查询月份数据是否已存在sql");
		ResultSet rs = null;
		boolean flage = false;
		try {
			start(url, dstuser, dstpassword);
			rs = statement.executeQuery(sql);
			while (rs.next()) {
				count = rs.getInt("count");
				if (count > 0) {
					flage = true;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			close();
		}
		return flage;
	}

	/**
	 * 删除已存在的月份数据
	 * 
	 * @param dsttableName
	 * @param dstBaseName
	 * @param month
	 * @return void
	 */
	public void delMonthData(String dsttableName, String dstBaseName, String month, String probetype) {

		String consql = "";
		if ("pc".equals(probetype)) {
			consql = "and probetype='" + probetype + "'";
		} else if ("app".equals(probetype)) {
			consql = "and (probetype='ios' or probetype='android')";
		}

		String sql = "delete from " + dsttableName + " where month= '" + month + "' " + consql + "";
		String url = dsturl.substring(0, dsturl.lastIndexOf("/") + 1) + dstBaseName;
		System.out.println(sql + "::::::查询月份数据是否已存在sql");
		try {
			start(url, dstuser, dstpassword);
			statement.execute(sql);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			close();
		}

	}

}
