package com.opencassandra.jiakuan.servicequality.servicequalitybase;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.opencassandra.descfile.ConfParser;
import com.opencassandra.jiakuan.base.BaseDaoImp;
import com.opencassandra.service.utils.UtilDate;

public class ServiceQualityHttpUploadBseDao extends BaseDaoImp {

	public static List<Map> groupList = new ArrayList<Map>();

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
			String tarField, String targetBaseName, String prtype, String whereParameter, String gparameter) {

		String sql = "";

		String nextMonth = UtilDate.getNextMonth(month);

		String wparameter = "";
		if (whereParameter != null && !whereParameter.isEmpty()) {

		}

		String grouppar = "";// 分组参数
		if (gparameter != null && !gparameter.isEmpty()) {
			if (dataSourceTable.contains("pc")) {
				grouppar = " and " + ConfParser.groupparameter + "='" + gparameter + "' ";
			} else {
				grouppar = " and substring_index(test_description, '/', 1)='" + gparameter + "' ";
			}
		}

		if (dataSourceTable.contains("pc")) {
			sql = "SELECT p.overall_speed " + tarField + " FROM " + dataSourceTable
					+ " p WHERE  consumerid != '' AND businessid != '' AND p.bandwidth != ''  AND os != '' AND mac != ''  AND start_time >= UNIX_TIMESTAMP('" + month + "01') * 1000 AND start_time < UNIX_TIMESTAMP('"
					+ nextMonth + "01') * 1000 AND cast(overall_speed AS signed) > 0 AND p.device_org = '" + orgid + "' AND service_type = '上传'  " + grouppar + "  " + wparameter
					+ " ORDER BY ABS(overall_speed)  desc  limit " + percentnum + " ,1 ";
		} else {
			sql = "select round(" + sourceField + ",2)  " + tarField + "" + " from " + dataSourceTable + " p  where 	p.imei!=''  AND model != ''  and UPPER(net_type) = 'WIFI'  AND substring_index(test_description, '/', - 1) != '' and test_description LIKE '%/%'  AND substring_index(test_description, '/', - 1) = '" + bandwidth
					+ "' AND  time >= UNIX_TIMESTAMP('" + month + "01') * 1000 AND time < UNIX_TIMESTAMP('" + nextMonth + "01') * 1000 and cast(avg_rate AS signed) >0  and p.device_org='" + orgid
					+ "' and android_ios='" + prtype.toLowerCase() + "' AND file_type = 'upload'  " + grouppar + "  " + wparameter + "  ORDER BY ABS(" + sourceField + ")  desc  limit " + percentnum + " ,1 ";
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

		String sql = "";
		String nextMonth = UtilDate.getNextMonth(month);

		String wparameter = "";

		if (whereParameter != null && !whereParameter.isEmpty()) {

		}

		String gparameter = "";
		String gwhe = "";
		String gqwhe = "";
		if (groupParameter != null && !groupParameter.isEmpty()) {
			gparameter = ",p." + groupParameter;
			gwhe = "  and p." + groupParameter + "=q." + groupParameter;
			// gqwhe = "  q."+groupParameter+" , ";
		}

		String groupidsql = "(SELECT t.org_key,t.id org, m.groupid,	m. NAME,m.orgname	FROM " + authBaseName + ".t_org t, ( SELECT j.org_id,j.groupid,i.name,j.orgname from " + authBaseName
				+ ".t_group_org_mapping i , (SELECT org_id, groupid,name orgname  " + " 	FROM " + authBaseName + ".t_group_org_mapping a, " + targetBaseName
				+ ".group_id b WHERE a.group_id = b.groupid ) j where j.groupid =i.parent_org_id  ) m WHERE t.id = m.org_id) n";

		String sampleSql = "(select count(id) new_sample_num ,p.device_org,p.bandwidth  " + gparameter + " from " + dataSourcebasename + "." + dataSourceTable + " p , " + groupidsql
				+ "  where  start_time >= UNIX_TIMESTAMP('" + month + "01') * 1000 AND start_time  < UNIX_TIMESTAMP('" + nextMonth
				+ "01') * 1000 and service_type='上传'  and p.device_org = n.org_key  " + wparameter + " GROUP BY p.device_org,p.bandwidth  " + gparameter + ") q ";

		sql = "select COUNT(DISTINCT url) url_num, n.org,n.groupid groupid ,n.name groupname, n.orgname , q.new_sample_num, '"
				+ month
				+ "' month,p.device_org org_id,COUNT(id) AS upload_test_times,round(avg(CONVERT(overall_speed,decimal(10,2))),2)  avg_upload_rate,round(max(CONVERT(overall_speed,decimal(10,2))),2)  best_upload_rate ,p.bandwidth broadband_type"
				+ " " + gparameter + " from " + dataSourceTable + " p , " + groupidsql + ", " + sampleSql
				+ "  where 	consumerid != '' AND businessid != '' AND p.bandwidth != ''  AND os != '' AND mac != '' AND start_time >= UNIX_TIMESTAMP('" + month
				+ "01') * 1000 AND start_time < UNIX_TIMESTAMP('" + nextMonth
				+ "01') * 1000 and cast(overall_speed AS signed)>0  and p.device_org = n.org_key  and p.device_org=q.device_org and p.bandwidth=q.bandwidth  and service_type='上传'  " + gwhe + " "
				+ wparameter + "  GROUP BY p.device_org,p.bandwidth  " + gparameter + " ";

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

				String groupid = rs.getString("groupid");
				String groupname = rs.getString("groupname");
				String upload_test_times = rs.getString("upload_test_times");
				String broadband_type = rs.getString("broadband_type");
				String avg_upload_rate = rs.getString("avg_upload_rate");
				String best_upload_rate = rs.getString("best_upload_rate");

				map.put("groupid", groupid);
				String orgid = rs.getString("org_id");
				String org = rs.getString("org");

				map.put("orgid", orgid);
				map.put("avg_upload_rate", avg_upload_rate);
				map.put("best_upload_rate", best_upload_rate);
				map.put("groupname", groupname);
				map.put("upload_test_times", upload_test_times);
				map.put("month", month);
				map.put("broadband_type", broadband_type);
				map.put("probetype", probetype.toUpperCase());
				map.put("orgname", rs.getString("orgname"));
				map.put("new_sample_num", rs.getString("new_sample_num"));
				String url_num = rs.getString("url_num");

				map.put("url_num", url_num);

				groupmap.put("orgid", orgid);
				groupmap.put("broadband_type", broadband_type);
				groupmap.put("org", org);
				groupmap.put("percentnum", upload_test_times);
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

		if (whereParameter != null && !whereParameter.isEmpty()) {

		}

		String gparameter = "";// 分组参数
		String gwhe = "";// 分组引带的where相等条件
		String aliaspar = "";// 分组所引带的取值的及别名参数
		if (groupParameter != null && !groupParameter.isEmpty()) {

			if (groupParameter.equals("operator")) {

				gparameter = " substring_index(test_description, '/', 1)  , ";
				gwhe = "  and substring_index(test_description, '/', 1) =q." + groupParameter;
				aliaspar = " substring_index(test_description, '/', 1) " + groupParameter + " ,";
			} else {
				gparameter = ",p." + groupParameter;
				gwhe = "  and p." + groupParameter + "=q." + groupParameter;
				aliaspar = " " + groupParameter + " ,";
			}
		}

		String groupidsql = "(SELECT t.org_key,t.id org, m.groupid,	m. NAME,m.orgname	FROM " + authBaseName + ".t_org t, ( SELECT j.org_id,j.groupid,i.name,j.orgname from " + authBaseName
				+ ".t_group_org_mapping i , (SELECT org_id, groupid,name orgname  " + " 	FROM " + authBaseName + ".t_group_org_mapping a, " + targetBaseName
				+ ".group_id b WHERE a.group_id = b.groupid ) j where j.groupid =i.parent_org_id  ) m WHERE t.id = m.org_id) n";

		String sampleSql = "(select  " + aliaspar + "  count(id) new_sample_num ,p.device_org,p.android_ios, substring_index(test_description, '/', - 1) bandwidth from " + dataSourcebasename + "."
				+ dataSourceTable + " p , " + groupidsql + "  where  time >= UNIX_TIMESTAMP('" + month + "01') * 1000 AND time  < UNIX_TIMESTAMP('" + nextMonth
				+ "01') * 1000  and p.device_org = n.org_key and file_type='upload' "+gwhe+"  GROUP BY p.device_org,android_ios,  " + gparameter + "  substring_index(test_description, '/', - 1)  ) q ";

		sql = "select  "
				+ aliaspar
				+ "  COUNT(DISTINCT url) url_num, n.org org, case p.android_ios when 'ios' then 'iOS' when 'android' then 'Android' end android_ios, COUNT(id)  as upload_test_times,'"
				+ month
				+ "' month,n.groupid groupid ,n.orgname , q.new_sample_num, p.device_org org_id ,n.name groupname,substring_index(test_description, '/', - 1) broadband_type,round(avg(CONVERT(avg_rate,decimal(10,2))),2)  AS avg_upload_rate ,round(max(CONVERT(avg_rate,decimal(10,2))),2) as best_upload_rate "
				+ " from "
				+ dataSourcebasename
				+ "."
				+ dataSourceTable
				+ " p , "
				+ groupidsql
				+ " ,"
				+ sampleSql
				+ "  where 	p.imei!=''  AND model != ''  and UPPER(net_type) = 'WIFI'  AND substring_index(test_description, '/', - 1) != ''   and p.android_ios = q.android_ios  and test_description LIKE '%/%'  AND time >= UNIX_TIMESTAMP('"
				+ month
				+ "01') * 1000 AND time < UNIX_TIMESTAMP('"
				+ nextMonth
				+ "01') * 1000 and  cast(avg_rate AS signed)>0 and p.device_org = n.org_key and file_type='upload'  and p.device_org=q.device_org and substring_index(test_description, '/', - 1)=q.bandwidth  "
				+ gwhe + " " + wparameter + "   GROUP BY p.device_org,substring_index(test_description, '/', - 1), " + gparameter + "  p.android_ios";

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
				String groupid = rs.getString("groupid");
				String groupname = rs.getString("groupname");
				String upload_test_times = rs.getString("upload_test_times");
				String broadband_type = rs.getString("broadband_type");
				String avg_upload_rate = rs.getString("avg_upload_rate");
				String best_upload_rate = rs.getString("best_upload_rate");
				String android_ios = rs.getString("android_ios");

				map.put("groupid", groupid);
				String orgid = rs.getString("org_id");
				String org = rs.getString("org");

				String url_num = rs.getString("url_num");

				map.put("url_num", url_num);

				map.put("orgid", orgid);
				map.put("avg_upload_rate", avg_upload_rate);
				map.put("best_upload_rate", best_upload_rate);
				map.put("groupname", groupname);
				map.put("upload_test_times", upload_test_times);
				map.put("month", month);
				map.put("broadband_type", broadband_type);
				map.put("probetype", android_ios);
				map.put("orgname", rs.getString("orgname"));
				map.put("new_sample_num", rs.getString("new_sample_num"));

				groupmap.put("orgid", orgid);
				groupmap.put("broadband_type", broadband_type);
				groupmap.put("org", org);
				groupmap.put("percentnum", upload_test_times);
				groupmap.put("probetype", android_ios);
				listMap.add(map);

				groupList.add(groupmap);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return listMap;
	}

}
