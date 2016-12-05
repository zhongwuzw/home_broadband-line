package com.opencassandra.jiakuan.servicequality.servicequalitybase;


import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.opencassandra.descfile.ConfParser;
import com.opencassandra.jiakuan.base.BaseDaoImp;
import com.opencassandra.service.utils.UtilDate;

public class ServicequalityWebbrowsingBaseDao extends BaseDaoImp{

	
	
	public static List<Map> groupList = new ArrayList<Map>();

	
	
	/**
	 * 根据探针类型对数据进行分类处理
	 * @param dataSourceTable
	 * @param dataSourcebasename
	 * @param tarTableName
	 * @param month
	 * @param targetBaseName
	 * @param probetype
	 * @return
	 */
	public List<Map> getDataByprobetype(String dataSourceTable, String dataSourcebasename, String tarTableName,String month,String targetBaseName,String probetype,String authBaseName,String whereParameter, String groupParameter) {
		List<Map> listMap = new ArrayList<Map>();
		
		if("pc".equals(probetype)){
			listMap = getDataPc(dataSourceTable, dataSourcebasename, tarTableName, month, targetBaseName,probetype,authBaseName, whereParameter, groupParameter);
		}else if("app".equals(probetype)){
			listMap = getDataApp(dataSourceTable, dataSourcebasename, tarTableName, month, targetBaseName,probetype,authBaseName, whereParameter, groupParameter);
		}else if("".equals(probetype)){
			
		}
		return listMap;
	}
	

	/**
	 * 根据类型获取数据pc
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
	public List<Map> getDataPc(String dataSourceTable, String dataSourcebasename, String tarTableName,String month,String targetBaseName,String probetype,String authBaseName,String whereParameter, String groupParameter) {

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

		
		String groupidsql = "(SELECT t.org_key,t.id org, m.groupid,	m. NAME,m.orgname	FROM "+authBaseName+".t_org t, ( SELECT j.org_id,j.groupid,i.name,j.orgname from "+authBaseName+".t_group_org_mapping i , (SELECT org_id, groupid,name orgname  " +
				" 	FROM "+authBaseName+".t_group_org_mapping a, "+targetBaseName+".group_id b WHERE a.group_id = b.groupid ) j where j.groupid =i.parent_org_id  ) m WHERE t.id = m.org_id) n";
		
		String sampleSql = "(select "+wheredata+" count(id) new_sample_num ,p.device_org,p.bandwidth  " + gparameter + "  from "+dataSourcebasename+"."+dataSourceTable+" p , "+groupidsql+"  where  start_time >= UNIX_TIMESTAMP('"+month+"01') * 1000 AND start_time  < UNIX_TIMESTAMP('"+nextMonth+"01') * 1000  and p.device_org = n.org_key   " + wparameter + "  GROUP BY p.device_org,p.bandwidth   " + gparameter + " "+groupwhere+") q ";
		
		sql = "select "+wheredata+"  COUNT(DISTINCT web_site) url_num,  round(avg(CONVERT(ninety_loading,decimal(10,2))),0)  page_avg_ninetydelay, COUNT(id)  as page_test_times,q.new_sample_num,n.orgname ,'"+month+"' month,n.org,p.device_org org_id,p.bandwidth,round(avg(CONVERT(loading_delay,decimal(10,2))),0) AS page_avg_delay ,round(avg(CONVERT(success_rate,decimal(10,2))),2)  as  page_success_rate,round(min(CONVERT(loading_delay,decimal(10,2))),0)  as best_page_delay, round(max(CONVERT(success_rate,decimal(10,2))),2)   best_page_success_rate,n.groupid,n. NAME ,sum(case when success_rate=100 then 1 else 0 end)/count(id) as success_rate" +
				"  " + gparameter + "  from "+dataSourcebasename+"."+dataSourceTable+" p , "+groupidsql+" ,"+sampleSql+"  where "+period+" consumerid != '' AND businessid != '' AND p.bandwidth != ''  AND p.operator != ''   AND os != '' AND mac != '' AND start_time >= UNIX_TIMESTAMP('"+month+"01') * 1000 AND start_time < UNIX_TIMESTAMP('"+nextMonth+"01') * 1000 and  cast(loading_delay AS signed)>0 and cast(success_rate as signed) >=0 and cast(success_rate as signed) <=100  and p.device_org = n.org_key  and p.device_org=q.device_org and p.bandwidth=q.bandwidth   " + gwhe + " "
				+ wparameter + " GROUP BY p.device_org,p.bandwidth  " + gparameter + " "+groupwhere+" "; 
		
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
				
				map.put("page_avg_ninetydelay", rs.getString("page_avg_ninetydelay"));
				map.put("month", rs.getString("month"));
				map.put("orgid", rs.getString("org_id"));
				map.put("broadband_type", rs.getString("bandwidth"));
				map.put("page_avg_delay", rs.getString("page_avg_delay"));  
				map.put("page_success_rate", rs.getString("page_success_rate"));
				map.put("best_page_delay", rs.getString("best_page_delay"));
				map.put("best_page_success_rate", rs.getString("best_page_success_rate"));
				map.put("page_test_times", rs.getString("page_test_times"));
				map.put("orgname", rs.getString("orgname"));
				map.put("groupid", rs.getString("groupid"));
				map.put("groupname", rs.getString("NAME"));
				map.put("new_sample_num", rs.getString("new_sample_num"));
				map.put("probetype",probetype.toUpperCase());
				map.put("success_rate", rs.getString("success_rate"));
				String url_num = rs.getString("url_num");
				
				map.put("url_num", url_num);
				
				groupmap.put("orgid", rs.getString("org_id"));
				groupmap.put("broadband_type", rs.getString("bandwidth"));
				groupmap.put("org",  rs.getString("org"));
				groupmap.put("percentnum", rs.getString("page_test_times"));
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
	public List<Map> getDataApp(String dataSourceTable, String dataSourcebasename, String tarTableName,String month,String targetBaseName,String probetype,String authBaseName,String whereParameter, String groupParameter) {
		
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

		
		
		String groupidsql = "(SELECT t.org_key,t.id org, m.groupid,	m. NAME,m.orgname	FROM "+authBaseName+".t_org t, ( SELECT j.org_id,j.groupid,i.name,j.orgname from "+authBaseName+".t_group_org_mapping i , (SELECT org_id, groupid,name orgname  " +
				" 	FROM "+authBaseName+".t_group_org_mapping a, "+targetBaseName+".group_id b WHERE a.group_id = b.groupid ) j where j.groupid =i.parent_org_id  ) m WHERE t.id = m.org_id) n";
		
		String sampleSql = "(select "+wheredata+" " + aliaspar + "   count(id) new_sample_num ,p.device_org,p.android_ios, p.bandwidth bandwidth from "+dataSourcebasename+"."+dataSourceTable+" p , "+groupidsql+"  where  time >= UNIX_TIMESTAMP('"+month+"01') * 1000 AND time  < UNIX_TIMESTAMP('"+nextMonth+"01') * 1000  and p.device_org = n.org_key   GROUP BY p.device_org ,android_ios,p.bandwidth "+groupwhere+" ) q ";
		
		sql = "select "+wheredata+"  " + aliaspar + "   COUNT(DISTINCT web_sit) url_num, round(avg(CONVERT(eighty_loading,decimal(10,2))),0)  page_avg_ninetydelay, case p.android_ios when 'ios' then 'iOS' when 'android' then 'Android' end android_ios, COUNT(id)  as page_test_times,q.new_sample_num,n.orgname ,'"+month+"' month,n.org,p.device_org org_id,p.bandwidth bandwidth, round(avg(CONVERT(full_complete,decimal(10,2))),0) AS page_avg_delay , round(avg(CONVERT(success_rate,decimal(10,2))),2)  as  page_success_rate, round(min(CONVERT(full_complete,decimal(10,2))),0) as best_page_delay, round(min(CONVERT(success_rate,decimal(10,2))),2)  best_page_success_rate,n.groupid,n. NAME ,sum(case when success_rate=100 then 1 else 0 end)/count(id) as success_rate" +
				" from "+dataSourcebasename+"."+dataSourceTable+" p , "+groupidsql+" ,"+sampleSql+"  where "+period+"	p.imei!=''  AND model != ''  AND operator_manual != ''  and UPPER(net_type) = 'WIFI'  AND p.bandwidth != '' AND time >= UNIX_TIMESTAMP('"+month+"01') * 1000 AND time < UNIX_TIMESTAMP('"+nextMonth+"01') * 1000 and  cast(eighty_loading AS signed)>0 and cast(success_rate as signed) >=0 and cast(success_rate as signed) <=100  and p.device_org = n.org_key  and p.device_org=q.device_org  "
				+ gwhe + " " + wparameter + "  and p.bandwidth=q.bandwidth  and p.android_ios = q.android_ios GROUP BY p.device_org,q.bandwidth,  " + gparameter + "  p.android_ios "+groupwhere+""; 
		
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
				
				map.put("month", rs.getString("month"));
				map.put("orgid", rs.getString("org_id"));
				map.put("broadband_type", rs.getString("bandwidth"));
				map.put("page_avg_delay", rs.getString("page_avg_delay"));  
				map.put("page_success_rate", rs.getString("page_success_rate"));
				map.put("best_page_delay", rs.getString("best_page_delay"));
				map.put("best_page_success_rate", rs.getString("best_page_success_rate"));
				map.put("page_test_times", rs.getString("page_test_times"));
				map.put("orgname", rs.getString("orgname"));
				map.put("groupid", rs.getString("groupid"));
				map.put("groupname", rs.getString("NAME"));
				map.put("new_sample_num", rs.getString("new_sample_num"));
				map.put("probetype",rs.getString("android_ios"));
				map.put("page_avg_ninetydelay",rs.getString("page_avg_ninetydelay"));
				map.put("success_rate", rs.getString("success_rate"));
				String url_num = rs.getString("url_num");
				
				map.put("url_num", url_num);

				
				groupmap.put("orgid", rs.getString("org_id"));
				groupmap.put("broadband_type",  rs.getString("bandwidth"));
				groupmap.put("percentnum", rs.getString("page_test_times"));
				groupmap.put("org", rs.getString("org"));
				groupmap.put("page_avg_ninetydelay", rs.getString("page_avg_ninetydelay"));
				groupmap.put("probetype",rs.getString("android_ios"));
				
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
	public String getPercentData(String dataSourceTable, String dataSourcebasename,String authBaseName,String month,String orgid,String bandwidth,
			int percentnum,String sourceField,String tarField,String targetBaseName,String prtype , String whereParameter, String gparameter,String wherparameter) {
		
		List<Map> listMap = new ArrayList<Map>();
		
		String sql = "";
		//String wherequery = "where " + area + " !='' and " + area + "!='-' ";
		
		String nextMonth = UtilDate.getNextMonth(month);
		String order = "";
		if(sourceField.contains("rate")){
			order = "ORDER BY abs("+sourceField+") desc";
		}else {
			order = "ORDER BY abs("+sourceField+") asc";
		}
		String round = "";
		if(sourceField.contains("delay")||sourceField.contains("count")){
			round = "round("+sourceField+",0)";
		}else{
			round = "round("+sourceField+",2)";
		}
		
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

		
		if(dataSourceTable.contains("pc")){
			sql = "select "+round+" "+tarField+"" +
					" from "+dataSourceTable+" p   where 	consumerid != '' AND businessid != '' AND p.bandwidth != '' AND mac != '' AND start_time >= UNIX_TIMESTAMP('"+month+"01') * 1000 AND start_time < UNIX_TIMESTAMP('"+nextMonth+"01') * 1000 and cast(loading_delay AS signed)>0 and cast(success_rate as signed) >=0 and cast(success_rate as signed) <=100  and p.device_org='"+orgid+"' and p.bandwidth = '"+bandwidth+"'  " + grouppar + "  " + period + "   "+order+"  limit "+percentnum+" ,1 "; 
		}else{
			sql = "select "+round+" "+tarField+"" +
					" from "+dataSourceTable+" p  where 	" +
					" 	imei!='' AND model != '' AND operator_manual != '' AND UPPER(net_type) = 'WIFI' AND p.bandwidth != '' AND p.bandwidth = '"+bandwidth+"'   AND time >= UNIX_TIMESTAMP('"+month+"01') * 1000 AND time < UNIX_TIMESTAMP('"+nextMonth+"01') * 1000 and  cast(eighty_loading AS signed)>0 and cast(success_rate as signed) >=0 and cast(success_rate as signed) <=100  and p.device_org='"+orgid+"' and android_ios='"+prtype.toLowerCase()+"'  " + grouppar + "  " + period + "  "+order+"   limit "+percentnum+" ,1 "; 
			
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

	
	/**
	 * 查询该月份的数据是否已存�?
	 * 
	 * @param dsttableName
	 * @param dstBaseName
	 * @param month
	 * @return
	 * @return boolean
	 */
	public boolean queryDataExist(String dsttableName, String dstBaseName, String month,String probetype) {

		String consql = "";
		if("pc".equals(probetype)){
			consql = "and probetype='"+probetype+"'";
		}else if ("app".equals(probetype)){
			consql = "and (probetype='ios' or probetype='android')";
		}

		
		String sql = "select count(id) count from " + dsttableName + " where month = '" + month + "' "+consql+"";
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
	public void delMonthData(String dsttableName, String dstBaseName, String month,String probetype) {

		String consql = "";
		if("pc".equals(probetype)){
			consql = "and probetype='"+probetype+"'";
		}else if ("app".equals(probetype)){
			consql = "and (probetype='ios' or probetype='android')";
		}
		
		String sql = "delete from " + dsttableName + " where month= '" + month + "' "+consql+"";
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
