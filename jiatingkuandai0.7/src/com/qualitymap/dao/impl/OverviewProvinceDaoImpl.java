package com.qualitymap.dao.impl;

import java.util.List;
import java.util.Map;

import org.hibernate.SQLQuery;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.transform.Transformers;

import com.qualitymap.dao.OverviewProvinceDao;
import com.qualitymap.vo.OverviewProvince;

/**
 * 省级纬度数据分析
 * @author：kxc
 * @date：Apr 12, 2016
 */
public class OverviewProvinceDaoImpl implements OverviewProvinceDao {

	private SessionFactory sessionFactory;

	public void setSessionFactory(SessionFactory sessionFactory) {
		this.sessionFactory = sessionFactory;
	}

	public Session getSession() {
		return sessionFactory.getCurrentSession();
	}
	@Override
	public void saveProvice(OverviewProvince viewPro) {
		// TODO Auto-generated method stub
		getSession().save(viewPro);
	}

	@Override
	public void deletProvice(OverviewProvince viewPro) {
		// TODO Auto-generated method stub
		getSession().delete(viewPro);
	}

	@Override
	public List<OverviewProvince> findProvice(String groupid,String month) {
		// TODO Auto-generated method stub
		SQLQuery query = getSession().createSQLQuery("select month,province, CONCAT(broadband_type,'M') broadband_type,sum(testtimes) testtimes ,groupid  from overview_province where groupid in ("+groupid+") and month<='"+month+"' and broadband_type in('20','50','100') group by province ,broadband_type ,month order by month desc");
		List<OverviewProvince> proList = query.setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return proList;
	}

	@Override
	public void updateProvice(OverviewProvince viewPro) {
		// TODO Auto-generated method stub
		getSession().update(viewPro);
		
	}

	@Override
	public List<OverviewProvince> findByMonth(String month,String group) {
		String sql = "select sum(testtimes) testtimes ,province  from overview_province where month='"+month+"' and groupid in ("+group+") group by groupid ORDER BY testtimes desc";
		List<OverviewProvince> proList = getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
				//getSession().createSQLQuery(sql).list();
		return proList;
	}

	@Override
	public List<Map<String, Object>> getBroadbandData(String month, String groupid) {
		String sql = "select a.* from (select sum(new_user_num) new_user_num ,CONCAT(broadband_type,'M') broadband_type from overview_province where month='"+month+"' and groupid in ("+groupid+") group by broadband_type order by sum(new_user_num) desc limit 0,10 ) a order by a.broadband_type+0 desc";
		List<Map<String, Object>> proList = getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
				//getSession().createSQLQuery(sql).list();
		return proList;
	}

	@Override
	public List<Map<String, Object>> getProvinceUserPercent(String month, String groupid) {
		String sql = "select ifnull(sum(new_user_num),0) new_user_num ,province from overview_province where month='"+month+"' and groupid in ("+groupid+") group by groupid order by new_user_num DESC limit 0,10";
		List<Map<String, Object>> proList = getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return proList;
	}

	/**
	 * 获取宽带类型累计和本月用户数
	 */
	@Override
	public List<Map<String, Object>> getBroadbandTypeData(String month,String groupid) {
		String sql = " SELECT ifnull(thisMonth,'N/A') thisMonth,ifnull(accumulat,'N/A') accumulat ,type FROM (SELECT  sum(new_user_num) thisMonth,sum(accumulativ_num) accumulat ,probetype type FROM overview_kpi WHERE `month` ='" + month + "' and groupid in ("+groupid+")  GROUP BY probetype order by sum(new_user_num) desc) s";

		List<Map<String, Object>> queryList = this.getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return queryList;
	}
	/**
	 * 根据groupid，month获取样本总数
	 */
	@Override
	public String getTotalSampleNum(String month, String groupid,String broadType) {
		String sql = "select IFNULL(sum(new_sample_num),0) new_sample_num from overview_province where  month= '"+month+"' and groupid = '"+groupid+"' and broadband_type = '"+broadType+"' ";
		SQLQuery query = this.getSession().createSQLQuery(sql);    
	    String new_sample_num = query.uniqueResult().toString(); 
		return new_sample_num;
	}
	/**
	 * 根据groupid获取用户(报告中的用户字段)
	 */
	@Override
	public String getUsernumByGroupId(String month, String groupid,String broadType) {
		String sql = "select IFNULL(sum(new_user_num),0) new_user_num from overview_province where  month= '"+month+"' and groupid = '"+groupid+"' and broadband_type = '"+broadType+"' ";
		SQLQuery query = this.getSession().createSQLQuery(sql);    
	    String new_user_num = query.uniqueResult().toString(); 
		return new_user_num;
	}
	/**
	 * 根据groupid获取终端(报告中的终端字段)
	 */
	@Override
	public String getTerminalnumByGroupId(String month, String groupid,String broadType) {
		String sql = "select IFNULL(sum(terminal_num),0) new_terminal_num from overview_province where  month= '"+month+"' and groupid = '"+groupid+"' and broadband_type = '"+broadType+"' ";
		SQLQuery query = this.getSession().createSQLQuery(sql);    
	    String new_terminal_num = query.uniqueResult().toString(); 
		return new_terminal_num;
	}
	/**
	 * 下面6个接口为查询测试报告结果需要
	 * @param groupid
	 * @param month
	 * @param broadband_type
	 * @return
	 */
	@Override
	public List<Map<String, Object>> getwebratedata(String groupid, String month, String broadband_type) {
		String sql = " SELECT \"页面元素打开成功率\" typename, orgname, CONCAT( ROUND(SUM(page_success_rate*page_test_times)/SUM(page_test_times),2),\"%\") value,probetype type ,ifnull(sum(new_sample_num),0)/2 new_sample_num ,ifnull(sum(case_num),0)/2 case_num FROM servicequality_orgid_webbrowsing WHERE `month` ='" + month + "' and groupid in ("+groupid+") and broadband_type='"+broadband_type+"'  GROUP BY probetype,orgname ";

		List<Map<String, Object>> queryList = this.getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return queryList;
	}
	@Override
	public List<Map<String, Object>> getwebdelaydata(String groupid, String month, String broadband_type) {
		String sql = " SELECT \"页面显示时延\" typename, orgname,CONCAT( ROUND(SUM(page_avg_delay*page_test_times)/SUM(page_test_times),0),\"ms\") value,probetype type,ifnull(sum(new_sample_num),0)/2 new_sample_num ,ifnull(sum(case_num),0)/2 case_num FROM servicequality_orgid_webbrowsing WHERE `month` ='" + month + "' and groupid in ("+groupid+") and broadband_type='"+broadband_type+"'  GROUP BY probetype ,orgname";
		
		List<Map<String, Object>> queryList = this.getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return queryList;
	}
	@Override
	public List<Map<String, Object>> getwebninetydelaydata(String groupid, String month, String broadband_type) {
		String sql = " SELECT \"90%页面时延\" typename, orgname,CONCAT( ROUND(SUM(page_avg_ninetydelay*page_test_times)/SUM(page_test_times),0),\"ms\") value,probetype type,ifnull(sum(new_sample_num),0)/2 new_sample_num ,ifnull(sum(case_num),0)/2 case_num FROM servicequality_orgid_webbrowsing WHERE `month` ='" + month + "' and groupid in ("+groupid+") and broadband_type='"+broadband_type+"'  GROUP BY probetype ,orgname";
		
		List<Map<String, Object>> queryList = this.getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return queryList;
	}
	@Override
	public List<Map<String, Object>> getvideocachedata(String groupid, String month, String broadband_type) {
		String sql = " SELECT \"视频卡顿次数\" typename, orgname,CONCAT( ROUND( SUM(video_cache_count*video_test_times)/SUM(video_test_times),2),\"次\") value ,probetype type,ifnull(sum(new_sample_num),0)/2 new_sample_num ,ifnull(sum(case_num),0)/2 case_num FROM servicequality_orgid_video WHERE `month` ='" + month + "' and groupid in ("+groupid+") and broadband_type='"+broadband_type+"'  GROUP BY probetype ,orgname";

		List<Map<String, Object>> queryList = this.getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return queryList;
	}
	@Override
	public List<Map<String, Object>> getvideoproportiondata(String groupid, String month, String broadband_type) {
		String sql = " SELECT \"视频缓冲占比\" typename, orgname, CONCAT( ROUND( SUM(avg_buffer_proportion*video_test_times)/SUM(video_test_times),0),\"%\") value,probetype type ,ifnull(sum(new_sample_num),0)/2 new_sample_num ,ifnull(sum(case_num),0)/2 case_num FROM servicequality_orgid_video WHERE `month` ='" + month + "' and groupid in ("+groupid+") and broadband_type='"+broadband_type+"'  GROUP BY probetype ,orgname";
		
		List<Map<String, Object>> queryList = this.getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return queryList;
	}
	
	@Override
	public List<Map<String, Object>> getvideodelaydata(String groupid, String month, String broadband_type) {
		String sql = " SELECT \"视频加载时长\" typename, orgname, CONCAT( ROUND( SUM(avg_video_delay*video_test_times)/SUM(video_test_times),0),\"ms\") value,probetype type ,ifnull(sum(new_sample_num),0)/2 new_sample_num ,ifnull(sum(case_num),0)/2 case_num FROM servicequality_orgid_video WHERE `month` ='" + month + "' and groupid in ("+groupid+") and broadband_type='"+broadband_type+"'  GROUP BY probetype ,orgname";
		
		List<Map<String, Object>> queryList = this.getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return queryList;
	}
	@Override
	public List<Map<String, Object>> gethttpuploaddata(String groupid, String month, String broadband_type) {
		String sql = " SELECT \"上传速率\" typename, orgname, CONCAT( ROUND( SUM(avg_upload_rate*upload_test_times)/SUM(upload_test_times),2),\"Mpbs\") value ,probetype type,ifnull(sum(new_sample_num),0) new_sample_num ,ifnull(sum(case_num),0) case_num FROM servicequality_orgid_httpupload WHERE `month` ='" + month + "' and groupid in ("+groupid+") and broadband_type='"+broadband_type+"'  GROUP BY probetype,orgname ";
		
		List<Map<String, Object>> queryList = this.getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return queryList;
	}
	@Override
	public List<Map<String, Object>> gethttpdownloaddata(String groupid, String month, String broadband_type) {
		String sql = " SELECT \"下载速率\" typename, orgname, CONCAT( ROUND(SUM(avg_download_rate*download_test_times)/SUM(download_test_times),2),\"Mpbs\") value,probetype type ,ifnull(sum(new_sample_num),0) new_sample_num ,ifnull(sum(case_num),0) case_num FROM servicequality_orgid_httpdownload  WHERE `month` ='" + month + "' and groupid in ("+groupid+") and broadband_type='"+broadband_type+"'  GROUP BY probetype ,orgname";
		
		List<Map<String, Object>> queryList = this.getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return queryList;
	}
	
	//--------------------------------------------------------------------------------------------------------------
	/**
	 * 下面6个接口为查询测试报告中获取探针类型维度数据需要
	 * @param groupid
	 * @param month
	 * @param broadband_type
	 * @return
	 */
	@Override
	public List<Map<String, Object>> getratewebdata(String groupid, String month, String broadband_type) {
		String sql = " SELECT \"页面元素打开成功率\" typename ,ifnull(sum(new_sample_num),0)/2 new_sample_num ,ifnull(sum(case_num),0)/2 case_num,  CONCAT( ROUND(SUM(page_success_rate*page_test_times)/SUM(page_test_times),2),\"%\") value,probetype type  FROM servicequality_orgid_webbrowsing WHERE `month` ='" + month + "' and groupid in ("+groupid+") and broadband_type='"+broadband_type+"'  GROUP BY probetype ";
		
		List<Map<String, Object>> queryList = this.getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return queryList;
	}
	@Override
	public List<Map<String, Object>> getdelaywebdata(String groupid, String month, String broadband_type) {
		String sql = " SELECT \"页面显示时延\" typename,ifnull(sum(new_sample_num),0)/2 new_sample_num ,ifnull(sum(case_num),0)/2 case_num, CONCAT( ROUND(SUM(page_avg_delay*page_test_times)/SUM(page_test_times),0),\"ms\") value,probetype type FROM servicequality_orgid_webbrowsing WHERE `month` ='" + month + "' and groupid in ("+groupid+") and broadband_type='"+broadband_type+"'  GROUP BY probetype ";
		 
		List<Map<String, Object>> queryList = this.getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return queryList;
	}
	
	@Override
	public List<Map<String, Object>> getcachevideodata(String groupid, String month, String broadband_type) {
		String sql = " SELECT \"视频卡顿次数\" typename ,ifnull(sum(new_sample_num),0)/2 new_sample_num ,ifnull(sum(case_num),0)/2 case_num, CONCAT( ROUND( SUM(video_cache_count*video_test_times)/SUM(video_test_times),2),\"次\") value ,probetype type FROM servicequality_orgid_video WHERE `month` ='" + month + "' and groupid in ("+groupid+") and broadband_type='"+broadband_type+"'  GROUP BY probetype ";
		
		List<Map<String, Object>> queryList = this.getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return queryList;
	}
	@Override
	public List<Map<String, Object>> getdelayninetywebdata(String groupid, String month, String broadband_type) {
		String sql = " SELECT \"90%页面时延\" typename,ifnull(sum(new_sample_num),0)/2 new_sample_num ,ifnull(sum(case_num),0)/2 case_num, CONCAT( ROUND(SUM(page_avg_ninetydelay*page_test_times)/SUM(page_test_times),0),\"ms\") value,probetype type FROM servicequality_orgid_webbrowsing WHERE `month` ='" + month + "' and groupid in ("+groupid+") and broadband_type='"+broadband_type+"'  GROUP BY probetype ";
		
		List<Map<String, Object>> queryList = this.getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return queryList;
	}
	
	@Override
	public List<Map<String, Object>> getproportionvideodata(String groupid, String month, String broadband_type) {
		String sql = " SELECT \"视频卡顿次数\" typename ,ifnull(sum(new_sample_num),0)/2 new_sample_num ,ifnull(sum(case_num),0)/2 case_num, CONCAT( ROUND( SUM(avg_buffer_proportion*video_test_times)/SUM(video_test_times),2),\"%\") value ,probetype type FROM servicequality_orgid_video WHERE `month` ='" + month + "' and groupid in ("+groupid+") and broadband_type='"+broadband_type+"'  GROUP BY probetype ";
		
		List<Map<String, Object>> queryList = this.getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return queryList;
	}
	@Override
	public List<Map<String, Object>> getdelayvideodata(String groupid, String month, String broadband_type) {
		String sql = " SELECT \"视频加载时长\" typename ,ifnull(sum(new_sample_num),0)/2 new_sample_num ,ifnull(sum(case_num),0)/2 case_num, CONCAT( ROUND( SUM(avg_video_delay*video_test_times)/SUM(video_test_times),0),\"ms\") value,probetype type  FROM servicequality_orgid_video WHERE `month` ='" + month + "' and groupid in ("+groupid+") and broadband_type='"+broadband_type+"'  GROUP BY probetype ";
		
		List<Map<String, Object>> queryList = this.getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return queryList;
	}
	@Override
	public List<Map<String, Object>> getuploadhttpdata(String groupid, String month, String broadband_type) {
		String sql = " SELECT \"上传速率\" typename ,ifnull(sum(new_sample_num),0) new_sample_num ,ifnull(sum(case_num),0) case_num, CONCAT( ROUND( SUM(avg_upload_rate*upload_test_times)/SUM(upload_test_times),2),\"Mpbs\") value ,probetype type FROM servicequality_orgid_httpupload WHERE `month` ='" + month + "' and groupid in ("+groupid+") and broadband_type='"+broadband_type+"'  GROUP BY probetype ";
		
		List<Map<String, Object>> queryList = this.getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return queryList;
	}
	@Override
	public List<Map<String, Object>> getdownloadhttpdata(String groupid, String month, String broadband_type) {
		String sql = " SELECT \"下载速率\" typename ,ifnull(sum(new_sample_num),0) new_sample_num ,ifnull(sum(case_num),0) case_num, CONCAT( ROUND(SUM(avg_download_rate*download_test_times)/SUM(download_test_times),2),\"Mpbs\") value,probetype type  FROM servicequality_orgid_httpdownload  WHERE `month` ='" + month + "' and groupid in ("+groupid+") and broadband_type='"+broadband_type+"'  GROUP BY probetype ";
		
		List<Map<String, Object>> queryList = this.getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return queryList;
	}
	//-----------------------------------------------------------------------------------------------------------------------------------------
	/**
	 * 根据groupid获取有效样本
	 */
	@Override
	public String getValidSampleNum(String month, String groupid,String broadType) {
		String sql = "select IFNULL(sum(testtimes),0) ping_test_times from overview_province where  month= '"+month+"' and groupid = '"+groupid+"' and broadband_type = '"+broadType+"' ";
		SQLQuery query = this.getSession().createSQLQuery(sql);    
	    String ping_test_times = query.uniqueResult().toString(); 
		return ping_test_times;
	}
	
	
	
	/**
	 * 获取质量分析 即 上下月中数据的增减情况
	 */
	@Override
	public List<Map<String, Object>> getHttpdownloadServiceQualityCompare( String month,String permonth,String groupid,String broadband_type) {
		String sql = "SELECT a.probetype, (avg_download_rate - pre_avg_download_rate) differentials   FROM  " +
				"( SELECT probetype FROM servicequality_groupid_httpdownload b WHERE `month` IN ('"+permonth+"', '"+month+"') AND groupid IN ("+groupid+") AND broadband_type = '"+broadband_type+"'  group by probetype) a" +
				" LEFT JOIN " +
				"( SELECT CAST( avg_download_rate AS DECIMAL (9, 4) ) avg_download_rate, probetype FROM servicequality_groupid_httpdownload WHERE MONTH = '"+month+"' AND groupid IN ("+groupid+") AND broadband_type = '"+broadband_type+"'  ) b ON a.probetype = b.probetype" +
				" LEFT JOIN " +
				"( SELECT CAST( avg_download_rate AS DECIMAL (9, 4) ) pre_avg_download_rate, probetype FROM servicequality_groupid_httpdownload WHERE MONTH = '"+permonth+"' AND groupid IN ("+groupid+") AND broadband_type = '"+broadband_type+"'  ) c ON a.probetype = c.probetype" +
				" where a.probetype in ('Android','PC')";
		
		List<Map<String, Object>> queryList = this.getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return queryList;
	}
	
	@Override
	public List<Map<String, Object>> getVideoServiceQualityCompare( String month,String permonth,String groupid,String broadband_type) {
		String sql = "SELECT a.probetype, (avg_buffer_proportion - pre_avg_buffer_proportion) differentials   FROM  " +
				"( SELECT probetype FROM servicequality_groupid_video b WHERE `month` IN ('"+permonth+"', '"+month+"') AND groupid IN ("+groupid+") AND broadband_type = '"+broadband_type+"' group by probetype ) a" +
				" LEFT JOIN " +
				"( SELECT CAST( avg_buffer_proportion AS DECIMAL (9, 4) ) avg_buffer_proportion, probetype FROM servicequality_groupid_video WHERE MONTH = '"+month+"' AND groupid IN ("+groupid+") AND broadband_type = '"+broadband_type+"'  ) b ON a.probetype = b.probetype" +
				" LEFT JOIN " +
				"( SELECT CAST( avg_buffer_proportion AS DECIMAL (9, 4) ) pre_avg_buffer_proportion, probetype FROM servicequality_groupid_video WHERE MONTH = '"+permonth+"' AND groupid IN ("+groupid+") AND broadband_type = '"+broadband_type+"'  ) c ON a.probetype = c.probetype" +
				" where a.probetype in ('iOS')";
		
		
		List<Map<String, Object>> queryList = this.getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return queryList;
	}
	
	@Override
	public List<Map<String, Object>> getWebbrowsingServiceQualityCompare( String month,String permonth,String groupid,String broadband_type) {
		String sql = "SELECT a.probetype, (page_avg_ninetydelay - pre_page_avg_ninetydelay) differentials   FROM  " +
				"( SELECT probetype FROM servicequality_groupid_webbrowsing b WHERE `month` IN ('"+permonth+"', '"+month+"') AND groupid IN ("+groupid+") AND broadband_type = '"+broadband_type+"' group by probetype) a" +
				" LEFT JOIN " +
				"( SELECT CAST( page_avg_ninetydelay AS DECIMAL (9, 4) ) page_avg_ninetydelay, probetype FROM servicequality_groupid_webbrowsing WHERE MONTH = '"+month+"' AND groupid IN ("+groupid+") AND broadband_type = '"+broadband_type+"'  ) b ON a.probetype = b.probetype" +
				" LEFT JOIN " +
				"( SELECT CAST( page_avg_ninetydelay AS DECIMAL (9, 4) ) pre_page_avg_ninetydelay, probetype FROM servicequality_groupid_webbrowsing WHERE MONTH = '"+permonth+"' AND groupid IN ("+groupid+") AND broadband_type = '"+broadband_type+"'  ) c ON a.probetype = c.probetype" +
				" where a.probetype in ('Android','PC')";
		
		List<Map<String, Object>> queryList = this.getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return queryList;
	}
	
	
	/**
	 * 获取本省的结果数据在全国的排名
	 */
	@Override
	public String getResultOrder(String probetype ,String fieldName, String tableName ,String dataq,String datatype,String month, String groupid,String broadType) {
		String sql = "select count(id) avg_order from "+tableName+" where month= '"+month+"' and broadband_type = '"+broadType+"'  and probetype='"+probetype+"' and " +
				"abs("+fieldName+") "+dataq+" (select abs("+fieldName+") from "+tableName+" where  month= '"+month+"' and groupid = '"+groupid+"' and broadband_type = '"+broadType+"'  and probetype='"+probetype+"' )";
		SQLQuery query = this.getSession().createSQLQuery(sql);  
		String avg_delay_order = query.uniqueResult().toString(); 
		return avg_delay_order;
	}
	
	/**
	 * 获取本省的结果数据
	 */
	@Override
	public List<Map<String, Object>> getProvinceResult(String probetype ,String fieldName, String tableName,String dataq,String datatype , String month, String groupid,String broadType) {
		String sql = "select "+fieldName+" avg_data from "+tableName+" where  month= '"+month+"' and " +
				"groupid = '"+groupid+"' and broadband_type = '"+broadType+"' and probetype='"+probetype+"'";
		SQLQuery query = this.getSession().createSQLQuery(sql);  

		List<Map<String, Object>> queryList =query.setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return queryList;
	}

	/**
	 * 
	 */
	@Override
	public List<Map<String, Object>> getBestResult(String probetype ,String fieldName, String tableName ,String dataq,String datatype,String month, String broadType) {
		
		String sql = "select "+fieldName+" best_data  from "+tableName+"  where month= '"+month+"' and broadband_type = '"+broadType+"'  and probetype='"+probetype+"' ";
		SQLQuery query = this.getSession().createSQLQuery(sql);  
		List<Map<String, Object>> queryList =query.setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return queryList;
	}

	@Override
	public List<Map<String, Object>> getProvinceName(String groupid) {
		
		String sql = "select province groupname from overview_province where groupid in ("+groupid+") GROUP BY groupid";
		SQLQuery query = this.getSession().createSQLQuery(sql);  
		List<Map<String, Object>> queryList =query.setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return queryList;
	}

	@Override
	public List<Map<String, Object>> getServiceQualityCompare(String probetype, String fieldName, String tableName, String dataq, String datatype, String month, String premonth, String groupid,
			String broadband_type) {
		
 	
		
		
		String sql = "SELECT avg_data ,pre_avg_data FROM ( SELECT probetype FROM "+tableName+" b WHERE" +
				" `month` IN ('"+month+"','"+premonth+"') and groupid='"+groupid+"' and broadband_type='"+broadband_type+"' and probetype='"+probetype+"'  group by probetype ) a" +
				" LEFT JOIN ( SELECT  "+fieldName+" avg_data, probetype, MONTH FROM "+tableName+" " +
				" WHERE	month='"+month+"' and groupid='"+groupid+"' and broadband_type='"+broadband_type+"' and probetype='"+probetype+"' ) b ON a.probetype = b.probetype " +
				" LEFT JOIN ( SELECT  "+fieldName+" pre_avg_data, probetype, MONTH FROM "+tableName+" " +
				" WHERE	month='"+premonth+"' and groupid='"+groupid+"' and broadband_type='"+broadband_type+"' and probetype='"+probetype+"'  ) c ON a.probetype = c.probetype " ;
		
	/*	String sql = "select round(("+fieldName+" - (select "+fieldName+" from "+tableName+" where month='"+permonth+"' and groupid='"+groupid+"' and broadband_type='"+broadband_type+"' and probetype='"+probetype+"')),2) avg_data " +
				" from  "+tableName+"  where  month='"+month+"' and groupid='"+groupid+"' and broadband_type='"+broadband_type+"' and probetype='"+probetype+"' ";
		*/
		List<Map<String, Object>> queryList = this.getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return queryList;
	}
}







