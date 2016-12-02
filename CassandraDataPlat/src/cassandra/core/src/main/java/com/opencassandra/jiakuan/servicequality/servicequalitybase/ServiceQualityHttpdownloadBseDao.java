package com.opencassandra.jiakuan.servicequality.servicequalitybase;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.opencassandra.descfile.ConfParser;
import com.opencassandra.jiakuan.base.BaseDaoImp;
import com.opencassandra.service.utils.UtilDate;

public class ServiceQualityHttpdownloadBseDao extends BaseDaoImp {

	public static List<Map> groupList = new ArrayList<Map>();

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
	public List<Map> getDataByprobetype(String dataSourceTable, String dataSourcebasename, String tarTableName, String month, String targetBaseName, String probetype, String authBaseName,
			String whereParameter, String groupParameter) {
		List<Map> listMap = new ArrayList<Map>();

		if ("pc".equals(probetype)) {
			listMap = getDataPc(dataSourceTable, dataSourcebasename, tarTableName, month, targetBaseName, probetype, authBaseName, whereParameter, groupParameter);
		} else if ("app".equals(probetype)) {
			listMap = getDataApp(dataSourceTable, dataSourcebasename, tarTableName, month, targetBaseName, probetype, authBaseName, whereParameter, groupParameter);
		} else if ("".equals(probetype)) {

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
	public List<Map> getDataPc(String dataSourceTable, String dataSourcebasename, String tarTableName, String month, String targetBaseName, String probetype, String authBaseName,
			String whereParameter, String groupParameter) {

		List<Map> listMap = new ArrayList<Map>();

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
				 wheredata = "CASE  WHEN  (("+wd[0]+" <=CAST(FROM_UNIXTIME(start_time / 1000, '%H') AS SIGNED) and  CAST(FROM_UNIXTIME(start_time / 1000, '%H') AS SIGNED)<= "+wd[1]+") " +
						 "or ("+wd2[0]+" <=CAST(FROM_UNIXTIME(start_time / 1000, '%H') AS SIGNED) " +
						 "and  CAST(FROM_UNIXTIME(start_time / 1000, '%H') AS SIGNED)<= "+wd2[1]+")) THEN '忙时' ELSE '闲时' END time_period ,";
				 groupwhere = " ,time_period ";
				 
				 period = "q.time_period = (CASE  WHEN  (("+wd[0]+" <=CAST(FROM_UNIXTIME(start_time / 1000, '%H') AS SIGNED) and  CAST(FROM_UNIXTIME(start_time / 1000, '%H') AS SIGNED)<= "+wd[1]+") " +
						 "or ("+wd2[0]+" <=CAST(FROM_UNIXTIME(start_time / 1000, '%H') AS SIGNED) " +
						 "and  CAST(FROM_UNIXTIME(start_time / 1000, '%H') AS SIGNED)<= "+wd2[1]+")) THEN '忙时' ELSE '闲时' END) and ";
			}
			
		}

		String gparameter = "";
		String gwhe = "";
		String gqwhe = "";
		if (groupParameter != null && !groupParameter.isEmpty()) {
			gparameter = ",p." + groupParameter;
			gwhe = "  and p." + groupParameter + "=q." + groupParameter;
			// gqwhe = "  q."+groupParameter+" , ";
		}

		String sql = "";
		String groupidsql = "(SELECT t.org_key,t.id org, m.groupid,	m. NAME,m.orgname	FROM " + authBaseName + ".t_org t, ( SELECT j.org_id,j.groupid,i.name,j.orgname from " + authBaseName
				+ ".t_group_org_mapping i , (SELECT org_id, groupid,name orgname  " + " 	FROM " + authBaseName + ".t_group_org_mapping a, " + targetBaseName
				+ ".group_id b WHERE a.group_id = b.groupid ) j where j.groupid =i.parent_org_id  ) m WHERE t.id = m.org_id) n";

		String sampleSql = "(select "+wheredata+" count(id) new_sample_num , p.device_org,p.bandwidth " + gparameter + "  from " + dataSourcebasename + "." + dataSourceTable + " p , " + groupidsql
				+ "  where  start_time >= UNIX_TIMESTAMP('" + month + "01') * 1000 AND start_time  < UNIX_TIMESTAMP('" + nextMonth + "01') * 1000  and p.device_org = n.org_key and service_type='下载' "
				+ wparameter + " GROUP BY p.device_org,p.bandwidth " + gparameter + " "+groupwhere+" )  q ";

		sql = "select "+wheredata+"  COUNT(DISTINCT url) url_num  "
				+ gparameter
				+ ",  n.org org, n.groupid groupid ,n.name groupname, n.orgname , q.new_sample_num, '"
				+ month
				+ "' month,p.device_org org_id,COUNT(id) AS download_test_times,round(avg(CONVERT(overall_speed,decimal(10,2))),2)  avg_download_rate,round(max(CONVERT(overall_speed,decimal(10,2))),2) best_download_rate ,p.bandwidth broadband_type,sum(case when transfer_progress=100 then 1 else 0 end)/count(id) as success_rate"
				+ " from " + dataSourceTable + " p , " + groupidsql + ", " + sampleSql
				+ "  where "+period+"	consumerid != '' AND businessid != '' AND p.bandwidth != ''  AND p.os != ''  AND p.operator != '' AND mac != '' AND start_time >= UNIX_TIMESTAMP('" + month
				+ "01') * 1000 AND start_time < UNIX_TIMESTAMP('" + nextMonth
				+ "01') * 1000 and cast(overall_speed AS signed)>0  and p.device_org = n.org_key  and p.device_org=q.device_org and p.bandwidth=q.bandwidth  and service_type='下载' " + gwhe + " "
				+ wparameter + "  GROUP BY p.device_org,p.bandwidth  " + gparameter + " "+groupwhere+"";

		ResultSet rs = null;
		String url = srcurl + dataSourcebasename;

		System.out.println(sql);
		try {
			start(url, srcuser, srcpassword);

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
				String download_test_times = rs.getString("download_test_times");
				String broadband_type = rs.getString("broadband_type");
				String avg_download_rate = rs.getString("avg_download_rate");
				String best_download_rate = rs.getString("best_download_rate");
				String org = rs.getString("org");
				String success_rate = rs.getString("success_rate");

				String url_num = rs.getString("url_num");

				map.put("url_num", url_num);
				map.put("groupid", groupid);
				String orgid = rs.getString("org_id");

				map.put("orgid", orgid);
				map.put("download_test_times", download_test_times);
				map.put("avg_download_rate", avg_download_rate);
				map.put("groupname", groupname);
				map.put("best_download_rate", best_download_rate);
				map.put("month", month);
				map.put("broadband_type", broadband_type);
				map.put("probetype", probetype.toUpperCase());
				map.put("orgname", rs.getString("orgname"));
				map.put("new_sample_num", rs.getString("new_sample_num"));
				map.put("success_rate", success_rate);

				groupmap.put("orgid", orgid);
				groupmap.put("broadband_type", broadband_type);
				groupmap.put("org", org);
				groupmap.put("percentnum", download_test_times);
				groupmap.put("probetype", probetype.toUpperCase());

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
	public List<Map> getDataApp(String dataSourceTable, String dataSourcebasename, String tarTableName, String month, String targetBaseName, String probetype, String authBaseName,
			String whereParameter, String groupParameter) {

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

				gparameter = " operator_manual  , ";
				gwhe = "  and operator_manual =q." + groupParameter;
				aliaspar = " operator_manual " + groupParameter + " ,";
			} else {
				gparameter = ",p." + groupParameter;
				gwhe = "  and p." + groupParameter + "=q." + groupParameter;
				aliaspar = " " + groupParameter + " ,";
			}
		}

		String groupidsql = "(SELECT t.org_key,t.id org, m.groupid,	m. NAME,m.orgname	FROM " + authBaseName + ".t_org t, ( SELECT j.org_id,j.groupid,i.name,j.orgname from " + authBaseName
				+ ".t_group_org_mapping i , (SELECT org_id, groupid,name orgname  " + " 	FROM " + authBaseName + ".t_group_org_mapping a, " + targetBaseName
				+ ".group_id b WHERE a.group_id = b.groupid ) j where j.groupid =i.parent_org_id  ) m WHERE t.id = m.org_id) n";

		String sampleSql = "(select "+wheredata+" " + aliaspar + " count(id) new_sample_num ,p.device_org, p.android_ios,p.bandwidth bandwidth from " + dataSourcebasename + "."
				+ dataSourceTable + " p , " + groupidsql + "  where  time >= UNIX_TIMESTAMP('" + month + "01') * 1000 AND time  < UNIX_TIMESTAMP('" + nextMonth
				+ "01') * 1000  and p.device_org = n.org_key and file_type='download'  GROUP BY p.device_org,android_ios, " + gparameter + "  p.bandwidth "+groupwhere+" ) q ";

		sql = "select "+wheredata+" "
				+ aliaspar
				+ " COUNT(DISTINCT url) url_num,  n.org org, case p.android_ios when 'ios' then 'iOS' when 'android' then 'Android' end android_ios, COUNT(id)  as download_test_times,'"
				+ month
				+ "' month,n.groupid groupid ,n.orgname , q.new_sample_num, p.device_org org_id ,n.name groupname,p.bandwidth broadband_type,round(avg(CONVERT(avg_rate,decimal(10,2))),2) AS avg_download_rate ,max(CONVERT(avg_rate,decimal(10,2)))  as best_download_rate ,sum(case when success_rate=100 then 1 else 0 end)/count(id) as success_rate"
				+ " from "
				+ dataSourcebasename
				+ "."
				+ dataSourceTable
				+ " p , "
				+ groupidsql
				+ " ,"
				+ sampleSql
				+ "  where "+period+" "
				+ "  p.imei!='' AND model != '' and  p.bandwidth != ''   and UPPER(net_type) = 'WIFI' AND p.bandwidth != ''   AND time >= UNIX_TIMESTAMP('"
				+ month
				+ "01') * 1000 AND time < UNIX_TIMESTAMP('"
				+ nextMonth
				+ "01') * 1000 and  cast(avg_rate AS signed)>0 and p.device_org = n.org_key  and p.device_org=q.device_org and file_type='download' and p.bandwidth=q.bandwidth  and p.android_ios = q.android_ios "
				+ gwhe + " " + wparameter + " GROUP BY p.device_org,p.bandwidth, " + gparameter + "  p.android_ios "+groupwhere+"";

		ResultSet rs = null;
		String url = srcurl + dataSourcebasename;

		System.out.println(sql);
		try {
			start(url, srcuser, srcpassword);

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
				String download_test_times = rs.getString("download_test_times");
				String broadband_type = rs.getString("broadband_type");
				String avg_download_rate = rs.getString("avg_download_rate");
				String best_download_rate = rs.getString("best_download_rate");
				String android_ios = rs.getString("android_ios");
				String success_rate = rs.getString("success_rate");

				map.put("groupid", groupid);
				String orgid = rs.getString("org_id");
				String org = rs.getString("org");
				String url_num = rs.getString("url_num");

				map.put("url_num", url_num);
				map.put("orgid", orgid);
				map.put("best_download_rate", best_download_rate);
				map.put("avg_download_rate", avg_download_rate);
				map.put("groupname", groupname);
				map.put("download_test_times", download_test_times);
				map.put("month", month);
				map.put("broadband_type", broadband_type);
				map.put("probetype", android_ios);
				map.put("orgname", rs.getString("orgname"));
				map.put("new_sample_num", rs.getString("new_sample_num"));
				map.put("success_rate", success_rate);

				groupmap.put("orgid", orgid);
				groupmap.put("org", org);
				groupmap.put("broadband_type", broadband_type);
				groupmap.put("percentnum", download_test_times);
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

		String sql = "";
		String nextMonth = UtilDate.getNextMonth(month);

		String period = "";

		if (whereParameter != null && !whereParameter.isEmpty()) {
			String[] wh = whereParameter.split(",");
			String[] wd = null;
			String[] wd2 = null;
			if(wh.length>1){
				 wd = wh[0].split("-");
				 wd2 = wh[1].split("-");
				 
				 if(dataSourceTable.contains("pc")){
					 
					 period = " and '"+wherparameter+"' = (CASE  WHEN  (("+wd[0]+" <=CAST(FROM_UNIXTIME(start_time / 1000, '%H') AS SIGNED) and  CAST(FROM_UNIXTIME(start_time / 1000, '%H') AS SIGNED)<= "+wd[1]+") " +
							 "or ("+wd2[0]+" <=CAST(FROM_UNIXTIME(start_time / 1000, '%H') AS SIGNED) " +
							 "and  CAST(FROM_UNIXTIME(start_time / 1000, '%H') AS SIGNED)<= "+wd2[1]+")) THEN '忙时' ELSE '闲时' END) ";
				 }else{
					 period = " and '"+wherparameter+"' = (CASE  WHEN  (("+wd[0]+" <=CAST(FROM_UNIXTIME(time / 1000, '%H') AS SIGNED) and  CAST(FROM_UNIXTIME(time / 1000, '%H') AS SIGNED)<= "+wd[1]+") " +
							 "or ("+wd2[0]+" <=CAST(FROM_UNIXTIME(time / 1000, '%H') AS SIGNED) " +
							 "and  CAST(FROM_UNIXTIME(time / 1000, '%H') AS SIGNED)<= "+wd2[1]+")) THEN '忙时' ELSE '闲时' END) ";
				 }
			}
		}

		String grouppar = "";// 分组参数
		if (gparameter != null && !gparameter.isEmpty()) {
			if (dataSourceTable.contains("pc")) {
				grouppar = " and " + ConfParser.groupparameter + "='" + gparameter + "' ";
			} else {
				grouppar = " and operator_manual='" + gparameter + "' ";
			}
		}

		if (dataSourceTable.contains("pc")) {
			sql = "SELECT p.overall_speed " + tarField + " FROM " + dataSourceTable + " p WHERE consumerid != '' AND businessid != '' AND p.bandwidth != '' AND mac != '' and p.bandwidth ='"
					+ bandwidth + "' AND start_time >= UNIX_TIMESTAMP('" + month + "01') * 1000 AND start_time < UNIX_TIMESTAMP('" + nextMonth
					+ "01') * 1000 AND cast(overall_speed AS signed) > 0 AND p.device_org = '" + orgid + "' AND service_type = '下载' " + grouppar + "  " + period
					+ " ORDER BY ABS(overall_speed)  desc  limit " + percentnum + " ,1 ";
		} else {
			sql = "select round(" + sourceField + ",2)  " + tarField + "" + " from " + dataSourceTable + " p  where  imei!='' AND model != '' AND UPPER(net_type) = 'WIFI' AND p.bandwidth = '" + bandwidth
					+ "' AND  time >= UNIX_TIMESTAMP('" + month + "01') * 1000 AND time < UNIX_TIMESTAMP('" + nextMonth + "01') * 1000 and cast(avg_rate AS signed) >0  and p.device_org='" + orgid
					+ "' and android_ios='" + prtype.toLowerCase() + "' AND file_type = 'download'  " + grouppar + "  " + period + "  ORDER BY ABS(" + sourceField + ")   desc limit " + percentnum
					+ " ,1 ";
		}

		ResultSet rs = null;
		String url = srcurl + dataSourcebasename;

		String data = "";

		System.out.println(sql);
		try {
			start(url, dstuser, dstpassword);

			rs = statement.executeQuery(sql);
			while (rs.next()) {
				data = rs.getString(tarField);

			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return data;
	}

}
