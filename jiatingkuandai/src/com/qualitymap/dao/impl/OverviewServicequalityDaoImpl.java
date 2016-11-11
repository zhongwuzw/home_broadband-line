package com.qualitymap.dao.impl;

import java.util.List;
import java.util.Map;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.transform.Transformers;

import com.qualitymap.dao.OverviewServicequalityDao;

/**
 * 平台业务质量统计
 * 
 * @author：kxc
 * @date：Apr 12, 2016
 */
public class OverviewServicequalityDaoImpl implements OverviewServicequalityDao {

	private SessionFactory sessionFactory;

	public void setSessionFactory(SessionFactory sessionFactory) {
		this.sessionFactory = sessionFactory;
	}

	public Session getSession() {
		return sessionFactory.getCurrentSession();
	}

	/**
	 * 获取用户指标统计
	 */
	@Override
	public List<Map<String, Object>> getHttpDownloadUserIndicator(String groupid, String month, String broadband_type,String group) {
		String sql = "select probetype ,sum(download_test_times) validsample,  CONCAT(round(sum(download_test_times*success_rate)/sum(download_test_times),2)*100,'%') successratevalue ,  CONCAT(round(sum(download_test_times*avg_download_rate)/sum(download_test_times),2),\"Mbps\") avgvalue, '互联网下载-速率' kpiname ,CONCAT(round(sum(download_test_times*top95_download_rate)/sum(download_test_times),2),\"Mbps\") percent95 ,CONCAT(round(sum(download_test_times*top85_download_rate)/sum(download_test_times),2),\"Mbps\") percent85 , CONCAT(round(sum(download_test_times*top75_download_rate)/sum(download_test_times),2),\"Mbps\") percent75    from servicequality_orgid_httpdownload d where month='"
				+ month + "'  "+group+" and month='" + month + "' and broadband_type='" + broadband_type + "' group by probetype";
		List<Map<String, Object>> downloadList = getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return downloadList;

	}

	@Override
	public List<Map<String, Object>> getHttpDownloadSuccessUserIndicator(String groupid, String month, String broadband_type,String group) {

		String sql = "select probetype ,sum(download_test_times) validsample,  CONCAT(round(sum(download_test_times*success_rate)/sum(download_test_times),2)*100,'%') avgvalue , '互联网下载-成功率' kpiname , ifnull(round(sum(download_test_times*top95_success_rate)/sum(download_test_times),1),'N/A') percent95 , IFNULL(round(sum(download_test_times*top85_success_rate)/sum(download_test_times),1),'N/A') percent85, IFNULL(round(sum(download_test_times*top75_success_rate)/sum(download_test_times),1),'N/A') percent75  from servicequality_orgid_httpdownload d where month='"
				+ month + "' "+group+" and month='" + month + "' and broadband_type='" + broadband_type + "'  group by probetype ";
		List<Map<String, Object>> downloadList = getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return downloadList;

	}

	@Override
	public List<Map<String, Object>> getHttpUploadUserIndicator(String groupid, String month, String broadband_type,String group) {

		String sql = "select *  from servicequality_groupid_httpupload where month='" + month + "' and groupid ='" + groupid + "' and month='" + month + "' and broadband_type='" + broadband_type
				+ "' ";
		List<Map<String, Object>> downloadList = getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return downloadList;

	}

	@Override
	public List<Map<String, Object>> getVideoProportionUserIndicator(String groupid, String month, String broadband_type,String group) {

		String sql = "select probetype , 'H5视频-缓冲时长占比' kpiname ,  CONCAT(round(sum(video_test_times*success_rate)/sum(video_test_times),2)*100,'%') successratevalue ,  "
				+ "  CONCAT(round(sum(video_test_times*top95_avg_buffer_proportion)/(sum(video_test_times)),2)*100,'%') percent95 ,CONCAT(round(sum(video_test_times*top85_avg_buffer_proportion)/(sum(video_test_times)),2)*100,'%')  percent85,CONCAT(round(sum(video_test_times*top75_avg_buffer_proportion)/(sum(video_test_times)),2)*100,'%') percent75,"
				+ "sum(video_test_times) validsample ,CONCAT(round(sum(video_test_times*avg_buffer_proportion)/sum(video_test_times),2)*100,'%') avgvalue " + " from servicequality_orgid_video where month='" + month
				+ "' "+group+" and month='" + month + "' and broadband_type='" + broadband_type + "'   group by probetype ";
		List<Map<String, Object>> proportionList = getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return proportionList;

	}

	@Override
	public List<Map<String, Object>> getVideoDelayUserIndicator(String groupid, String month, String broadband_type,String group) {

		
		String sql = "select probetype , 'H5视频-首次缓冲时延' kpiname ,  CONCAT(round(sum(video_test_times*success_rate)/sum(video_test_times),2)*100,'%') successratevalue , "
				+ " CONCAT(round(sum(video_test_times*top95_first_buffer_delay)/(sum(video_test_times)),0),\"ms\") percent95 ,CONCAT(round(sum(video_test_times*top85_first_buffer_delay)/(sum(video_test_times)),0),\"ms\") percent85,CONCAT(round(sum(video_test_times*top75_first_buffer_delay)/(sum(video_test_times)),0),\"ms\") percent75,"
				+ "sum(video_test_times) validsample ,CONCAT(round(sum(video_test_times*first_buffer_delay)/sum(video_test_times),0),\"ms\") avgvalue " + " from servicequality_orgid_video where month='"
				+ month + "' "+group+" and month='" + month + "' and broadband_type='" + broadband_type + "'   group by probetype ";
		List<Map<String, Object>> avgdelayList = getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return avgdelayList;

	}

	@Override
	public List<Map<String, Object>> getVideoCache_countUserIndicator(String groupid, String month, String broadband_type,String group) {
		

		String sql = "select probetype , 'H5视频-卡顿次数' kpiname ,  CONCAT(round(sum(video_test_times*success_rate)/sum(video_test_times),2)*100,'%') successratevalue , "
				+ " CONCAT(round(sum(video_test_times*top95_cache_count)/(sum(video_test_times)),1),\"次\") percent95 ,CONCAT(round(sum(video_test_times*top85_cache_count)/(sum(video_test_times)),1),\"次\") percent85,CONCAT(round(sum(video_test_times*top75_cache_count)/(sum(video_test_times)),1),\"次\") percent75,"
				+ "sum(video_test_times) validsample , CONCAT(round(sum(video_test_times*video_cache_count)/sum(video_test_times),1),\"次\") avgvalue " + " from servicequality_orgid_video where month='"
				+ month + "' "+group+" and month='" + month + "' and broadband_type='" + broadband_type + "'   group by probetype ";
		List<Map<String, Object>> avgdelayList = getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return avgdelayList;

	}

	@Override
	public List<Map<String, Object>> getVideoPaly_successUserIndicator(String groupid, String month, String broadband_type,String group) {

		String sql = "select probetype , 'H5视频-播放成功率' kpiname ,  CONCAT(round(sum(video_test_times*success_rate)/sum(video_test_times),2)*100,'%') successratevalue , "
				+ " ifnull(round(sum(video_test_times*top95_avg_paly_success)/(sum(video_test_times)),2),'N/A') percent95 ,IFNULL(round(sum(video_test_times*top85_avg_paly_success)/(sum(video_test_times)),2),'N/A') percent85,IFNULL(round(sum(video_test_times*top75_avg_paly_success)/(sum(video_test_times)),2),'N/A') percent75,"
				+ "sum(video_test_times) validsample ,CONCAT(round(sum(video_test_times*success_rate)/sum(video_test_times),2)*100,'%') avgvalue " + " from servicequality_orgid_video where month='" + month
				+ "' "+group+" and month='" + month + "' and broadband_type='" + broadband_type + "'   group by probetype ";
		List<Map<String, Object>> avgdelayList = getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return avgdelayList;

	}

	@Override
	public List<Map<String, Object>> getWebbrowsingninetydelayUserIndicator(String groupid, String month, String broadband_type,String group) {
		

		String sql = "select probetype , '网页浏览-90%加载时延' kpiname ,  CONCAT(round(sum(page_test_times*success_rate)/sum(page_test_times),2)*100,'%') successratevalue , "
				+ " CONCAT(round(sum(page_test_times*top95_page_avg_ninetydelay)/(sum(page_test_times)),0),\"ms\") percent95 ,CONCAT(round(sum(page_test_times*top85_page_avg_ninetydelay)/(sum(page_test_times)),0),\"ms\") percent85,CONCAT(round(sum(page_test_times*top75_page_avg_ninetydelay)/(sum(page_test_times)),0),\"ms\") percent75,"
				+ "sum(page_test_times) validsample ,CONCAT(round(sum(page_test_times*page_avg_ninetydelay)/sum(page_test_times),0),\"ms\")  avgvalue "
				+ " from servicequality_orgid_webbrowsing where month='" + month + "' "+group+" and month='" + month + "' and broadband_type='" + broadband_type
				+ "'   group by probetype ";
		List<Map<String, Object>> avgdelayList = getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return avgdelayList;

	}

	@Override
	public List<Map<String, Object>> getWebbrowsingAvgdelayUserIndicator(String groupid, String month, String broadband_type,String group) {
		

		String sql = "select probetype , '网页浏览-100%加载时延' kpiname ,  CONCAT(round(sum(page_test_times*success_rate)/sum(page_test_times),2)*100,'%') successratevalue , "
				+ " CONCAT(round(sum(page_test_times*top95_page_delay)/(sum(page_test_times)),0),\"ms\") percent95 , CONCAT(round(sum(page_test_times*top85_page_delay)/(sum(page_test_times)),0),\"ms\") percent85, CONCAT(round(sum(page_test_times*top75_page_delay)/(sum(page_test_times)),0),\"ms\") percent75,"
				+ "sum(page_test_times) validsample ,CONCAT(round(sum(page_test_times*page_avg_delay)/sum(page_test_times),0),\"ms\") avgvalue " + " from servicequality_orgid_webbrowsing where month='"
				+ month + "' "+group+" and month='" + month + "' and broadband_type='" + broadband_type + "'   group by probetype ";
		List<Map<String, Object>> avgdelayList = getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return avgdelayList;

	}

	@Override
	public List<Map<String, Object>> getWebbrowsingsuccessUserIndicator(String groupid, String month, String broadband_type,String group) {

		String sql = "select probetype , '网页浏览-100%页面元素打开成功率' kpiname ,  CONCAT(round(sum(page_test_times*success_rate)/sum(page_test_times),2)*100,'%') successratevalue , "
				+ " CONCAT(round(sum(page_test_times*top95_page_success_rate)/(sum(page_test_times)),1),'%') percent95 ,CONCAT(round(sum(page_test_times*top85_page_success_rate)/(sum(page_test_times)),1),'%') percent85,CONCAT(round(sum(page_test_times*top75_page_success_rate)/(sum(page_test_times)),1),'%') percent75,"
				+ "sum(page_test_times) validsample ,CONCAT(round(sum(page_test_times*page_success_rate)/sum(page_test_times),1),'%') avgvalue " + " from servicequality_orgid_webbrowsing where month='" + month
				+ "' "+group+" and month='" + month + "' and broadband_type='" + broadband_type + "'   group by probetype ";
		List<Map<String, Object>> avgdelayList = getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return avgdelayList;

	}
	
	@Override
	public List<Map<String, Object>> getWebbrowsingVisitSuccessRate(String groupid, String month, String broadband_type,String group) {

		String sql = "select probetype , '网页浏览-访问成功率' kpiname ,  "
				+ " IFNULL(CONCAT(round(sum(page_test_times*top95_success_rate)/(sum(page_test_times)),1),'%'),'N/A') percent95 ,IFNULL(CONCAT(round(sum(page_test_times*top85_success_rate)/(sum(page_test_times)),1),'%'),'N/A') percent85,IFNULL(CONCAT(round(sum(page_test_times*top75_success_rate)/(sum(page_test_times)),1),'%'),'N/A') percent75,"
				+ "sum(page_test_times) validsample ,IFNULL(CONCAT(round(sum(page_test_times*success_rate)/sum(page_test_times),1)*100,'%'),'N/A') avgvalue " + " from servicequality_orgid_webbrowsing where month='" + month
				+ "' "+group+" and month='" + month + "' and broadband_type='" + broadband_type + "'   group by probetype ";
		List<Map<String, Object>> avgdelayList = getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return avgdelayList;

	}

	// ----------------------------------------------------------------------上为获取用户指标统计---------------------------------------------------------------------

	// ----------------------------------------------------------------------下为获取地域用户指标统计---------------------------------------------------------------------
	@Override
	public List<Map<String, Object>> getTerritoryHttpDownload(String groupid, String month, String broadband_type) {

		String sql = "";
		
		if(groupid.contains(",")){
			sql = "select url_num , '互联网下载' testtype,  groupname cityname ,sum(new_sample_num) samplenum , sum(download_test_times) internetdownload , sum(download_test_times) validsample  from servicequality_orgid_httpdownload where  " +
					" groupid in (" + groupid + ") and month='" + month + "' and broadband_type='" + broadband_type + "'   group by groupname";
		}else{
			sql = "select url_num , '互联网下载' testtype,  orgname cityname ,sum(new_sample_num) samplenum , sum(download_test_times) internetdownload , sum(download_test_times) validsample  from servicequality_orgid_httpdownload where " + "  groupid ='"
				+ groupid + "' and month='" + month + "' and broadband_type='" + broadband_type + "'   group by orgname";
		}
		
		List<Map<String, Object>> avgdelayList = getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return avgdelayList;

	}

	@Override
	public List<Map<String, Object>> getTerritoryHttpUpload(String groupid, String month, String broadband_type) {

		String sql = "";
		if(groupid.contains(",")){
			sql = "select  groupname cityname , url_num , '互联网下载' testtype, sum(new_sample_num) samplenum , sum(upload_test_times) internetdownload , sum(upload_test_times) validsample from servicequality_orgid_httpdownload where " +
					"  groupid in ("+ groupid + ") and month='" + month + "' and broadband_type='" + broadband_type + "'   group by groupname";
		}else{
			sql = "select  orgname cityname , url_num , '互联网下载' testtype, "
					+ "sum(new_sample_num) samplenum , sum(upload_test_times) internetdownload , sum(upload_test_times) validsample from servicequality_orgid_httpdownload where " + "  groupid ='"
					+ groupid + "' and month='" + month + "' and broadband_type='" + broadband_type + "'   group by orgname ";
		}
		List<Map<String, Object>> avgdelayList = getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return avgdelayList;

	}

	@Override
	public List<Map<String, Object>> getTerritoryVideo(String groupid, String month, String broadband_type) {

		String sql = "";
		if(groupid.contains(",")){
			sql = "select url_num , 'H5视频' testtype,  groupname cityname ," + "sum(new_sample_num) samplenum , sum(video_test_times) h5video , sum(video_test_times) validsample from servicequality_orgid_video where "
					+ "  groupid in (" + groupid + ") and month='" + month + "' and broadband_type='" + broadband_type + "'   group by groupname ";
		}else{
			sql = "select url_num , 'H5视频' testtype,  orgname cityname ," + "sum(new_sample_num) samplenum , sum(video_test_times) h5video , sum(video_test_times) validsample from servicequality_orgid_video where "
					+ "  groupid ='" + groupid + "' and month='" + month + "' and broadband_type='" + broadband_type + "'   group by orgname ";
		}
		List<Map<String, Object>> avgdelayList = getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return avgdelayList;

	}

	@Override
	public List<Map<String, Object>> getTerritoryWebbrowsing(String groupid, String month, String broadband_type) {

		String sql = "";
		if(groupid.contains(",")){
			sql = "select  url_num , '网页浏览' testtype, groupname cityname ," + "sum(new_sample_num) samplenum , sum(page_test_times) webbrowse , sum(page_test_times) validsample from servicequality_orgid_webbrowsing where "
					+ "  groupid in (" + groupid + ") and month='" + month + "' and broadband_type='" + broadband_type + "'   group by groupname ";
		}else{
			sql = "select  url_num , '网页浏览' testtype, orgname cityname ," + "sum(new_sample_num) samplenum , sum(page_test_times) webbrowse , sum(page_test_times) validsample from servicequality_orgid_webbrowsing where "
					+ "  groupid ='" + groupid + "' and month='" + month + "' and broadband_type='" + broadband_type + "'   group by orgname ";
		}
		List<Map<String, Object>> avgdelayList = getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return avgdelayList;

	}
	//----------------------------------------------------------------------------------------------------------------------------------

	@Override
	public List<Map<String, Object>> getCityrateHttpdownload(String groupid, String month, String broadband_type) {

		String sql = "";
		if(groupid.contains(",")){
			sql = "SELECT groupname cityname,  CASE probetype WHEN 'Android' THEN '安卓' WHEN 'PC' THEN '桌面' WHEN 'iOS' THEN '苹果'  END usertype, '互联网下载' kpitypename,'速率' fieldname, " +
					" CONCAT(round(sum(success_rate*download_test_times)/sum(download_test_times),2)*100,'%') successratevalue ,  CONCAT(round(sum(avg_download_rate*download_test_times)/sum(download_test_times),2),'Mbps') avgvalue, CONCAT(round(sum(top75_download_rate*download_test_times)/sum(download_test_times),2),'Mbps') percent75,CONCAT(round(sum(top85_download_rate*download_test_times)/sum(download_test_times),2),'Mbps') percent85,CONCAT(round(sum(top95_download_rate*download_test_times)/sum(download_test_times),2),'Mbps') percent95 " +
					"  FROM  servicequality_orgid_httpdownload  where `month`='"+month+"' and groupid in("+groupid+") and broadband_type='"+broadband_type+"' group by groupname,probetype";
		}else{
			sql = "SELECT orgname cityname,  CASE probetype WHEN 'Android' THEN '安卓' WHEN 'PC' THEN '桌面' WHEN 'iOS' THEN '苹果'  END usertype, '互联网下载' kpitypename,'速率' fieldname, " +
					" CONCAT(round(success_rate,2)*100,'%') successratevalue ,  CONCAT(avg_download_rate,'Mbps') avgvalue, CONCAT(top75_download_rate,'Mbps') percent75,CONCAT(top85_download_rate,'Mbps') percent85,CONCAT(top95_download_rate,'Mbps') percent95 " +
					"  FROM servicequality_orgid_httpdownload where `month`='"+month+"' and groupid='"+groupid+"' and broadband_type='"+broadband_type+"' GROUP BY orgname,	probetype";
		}
		
		List<Map<String, Object>> avgdelayList = getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return avgdelayList;

	}

	@Override
	public List<Map<String, Object>> getCitysuccessHttpdownload(String groupid, String month, String broadband_type) {

		String sql = "SELECT orgname cityname, CASE probetype WHEN 'Android' THEN '安卓' WHEN 'PC' THEN '桌面' WHEN 'iOS' THEN '苹果'  END usertype, '互联网下载' kpitypename,'成功率' fieldname, " +
				" ifnull(success_rate*100,'N/A') avgvalue ,IFNULL(top75_success_rate,'N/A') percent75,ifNULL(top85_success_rate,'N/A') percent85,ifNULL(top95_success_rate*100,'N/A') percent95 " +
				"  FROM servicequality_orgid_httpdownload where `month`='"+month+"' and groupid='"+groupid+"' and broadband_type='"+broadband_type+"'  group by groupname,probetype";
		List<Map<String, Object>> avgdelayList = getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return avgdelayList;

	}

	@Override
	public List<Map<String, Object>> getCityNinetyDelayWebbrowsing(String groupid, String month, String broadband_type) {
		
		String sql = "";
		
		if(groupid.contains(",")){
			sql = "SELECT groupname cityname, CASE probetype WHEN 'Android' THEN '安卓' WHEN 'PC' THEN '桌面' WHEN 'iOS' THEN '苹果'  END usertype, '网页浏览' kpitypename,'90%加载时延' fieldname, " +
					" CONCAT(round(sum(success_rate*page_test_times)/sum(page_test_times),2)*100,'%') successratevalue ,  CONCAT(ROUND(sum(page_avg_ninetydelay*page_test_times)/sum(page_test_times),0),'ms') avgvalue ,CONCAT(ROUND(sum(top75_page_avg_ninetydelay*page_test_times)/sum(page_test_times),0),'ms') percent75,CONCAT(ROUND(sum(top85_page_avg_ninetydelay*page_test_times)/sum(page_test_times),0),'ms') percent85,CONCAT(ROUND(sum(top95_page_avg_ninetydelay*page_test_times)/sum(page_test_times),0),'ms') percent95 " +
					"  FROM servicequality_orgid_webbrowsing  where `month`='"+month+"' and groupid in("+groupid+") and broadband_type='"+broadband_type+"' group by groupname,probetype";
		}else{
			sql = "SELECT orgname cityname, CASE probetype WHEN 'Android' THEN '安卓' WHEN 'PC' THEN '桌面' WHEN 'iOS' THEN '苹果'  END usertype, '网页浏览' kpitypename,'90%加载时延' fieldname, " +
					" CONCAT(round(success_rate,2)*100,'%') successratevalue ,  CONCAT(ROUND(page_avg_ninetydelay,0),'ms') avgvalue ,CONCAT(ROUND(top75_page_avg_ninetydelay,0),'ms') percent75,CONCAT(ROUND(top85_page_avg_ninetydelay,0),'ms') percent85,CONCAT(ROUND(top95_page_avg_ninetydelay,0),'ms') percent95 " +
					"  FROM servicequality_orgid_webbrowsing  where `month`='"+month+"' and groupid='"+groupid+"' and broadband_type='"+broadband_type+"'  group by groupname,probetype";
		}

		List<Map<String, Object>> avgdelayList = getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return avgdelayList;

	}

	@Override
	public List<Map<String, Object>> getCityAvgDelayWebbrowsing(String groupid, String month, String broadband_type) {

		String sql = "";
		if(groupid.contains(",")){
			sql = "SELECT groupname cityname, CASE probetype WHEN 'Android' THEN '安卓' WHEN 'PC' THEN '桌面' WHEN 'iOS' THEN '苹果'  END usertype, '网页浏览' kpitypename,'100%加载时延' fieldname, " +
					" CONCAT(round(sum(success_rate*page_test_times)/sum(page_test_times),2)*100,'%') successratevalue ,  CONCAT(ROUND(sum(page_avg_delay*page_test_times)/sum(page_test_times),0),'ms') avgvalue ,CONCAT(ROUND(sum(top75_page_delay*page_test_times)/sum(page_test_times),0),'ms') percent75,CONCAT(ROUND(sum(top85_page_delay*page_test_times)/sum(page_test_times),0),'ms') percent85,CONCAT(ROUND(sum(top85_page_delay*page_test_times)/sum(page_test_times),0),'ms') percent95 " +
					"  FROM servicequality_orgid_webbrowsing  where `month`='"+month+"' and groupid in("+groupid+") and broadband_type='"+broadband_type+"' group by groupname,probetype";
		}else{
			sql = "SELECT orgname cityname, CASE probetype WHEN 'Android' THEN '安卓' WHEN 'PC' THEN '桌面' WHEN 'iOS' THEN '苹果'  END usertype, '网页浏览' kpitypename,'100%加载时延' fieldname, " +
					" CONCAT(round(success_rate,2)*100,'%') successratevalue ,  CONCAT(ROUND(page_avg_delay,0),'ms') avgvalue ,CONCAT(ROUND(top75_page_delay,0),'ms') percent75,CONCAT(ROUND(top85_page_delay,0),'ms') percent85,CONCAT(ROUND(top95_page_delay,0),'ms') percent95 " +
					"  FROM servicequality_orgid_webbrowsing  where `month`='"+month+"' and groupid='"+groupid+"' and broadband_type='"+broadband_type+"'  group by groupname,probetype";
		}
		
		List<Map<String, Object>> avgdelayList = getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return avgdelayList;

	}

	@Override
	public List<Map<String, Object>> getCityPageSuccessRateWebbrowsing(String groupid, String month, String broadband_type) {

		String sql = "";
		if(groupid.contains(",")){
			sql = "SELECT groupname cityname, CASE probetype WHEN 'Android' THEN '安卓' WHEN 'PC' THEN '桌面' WHEN 'iOS' THEN '苹果'  END usertype, '网页浏览' kpitypename,'100%页面元素打开成功率' fieldname, " +
					"  CONCAT(round(sum(success_rate*page_test_times)/sum(page_test_times),2)*100,'%') successratevalue , IFNULL(round(sum(page_success_rate*page_test_times)/sum(page_test_times),2),'N/A') avgvalue ,IFNULL(round(sum(top75_page_success_rate*page_test_times)/sum(page_test_times),2),'N/A') percent75,IFNULL(round(sum(top85_page_success_rate*page_test_times)/sum(page_test_times),2),'N/A') percent85,IFNULL(round(sum(top95_page_success_rate*page_test_times)/sum(page_test_times),2),'N/A') percent95 " +
					"  FROM servicequality_orgid_webbrowsing where `month`='"+month+"' and groupid in ("+groupid+") and broadband_type='"+broadband_type+"' group by groupname,probetype";
		}else{
			sql = "SELECT orgname cityname, CASE probetype WHEN 'Android' THEN '安卓' WHEN 'PC' THEN '桌面' WHEN 'iOS' THEN '苹果'  END usertype, '网页浏览' kpitypename,'100%页面元素打开成功率' fieldname, " +
					"  CONCAT(round(success_rate,2)*100,'%') successratevalue , IFNULL(page_success_rate,'N/A') avgvalue ,IFNULL(top75_page_success_rate,'N/A') percent75,ifNULL(top85_page_success_rate,'N/A') percent85,ifNULL(top95_page_success_rate,'N/A') percent95 " +
					"  FROM servicequality_orgid_webbrowsing where `month`='"+month+"' and groupid='"+groupid+"' and broadband_type='"+broadband_type+"'  group by groupname,probetype";
		}
		
		List<Map<String, Object>> avgdelayList = getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return avgdelayList;

	}

	@Override
	public List<Map<String, Object>> getCitySuccessWebbrowsing(String groupid, String month, String broadband_type) {

		String sql = "SELECT orgname cityname, CASE probetype WHEN 'Android' THEN '安卓' WHEN 'PC' THEN '桌面' WHEN 'iOS' THEN '苹果'  END usertype, '网页浏览' kpitypename,'访问成功率' fieldname, " +
				" CONCAT(round(success_rate,2)*100,'%') successratevalue ,  ifnull(round(success_rate,2)*100,'N/A') avgvalue ,IFNULL(top75_success_rate*100,'N/A') percent75,ifNULL(top85_success_rate*100,'N/A') percent85,ifNULL(top95_success_rate*100,'N/A') percent95 " +
				"  FROM servicequality_orgid_webbrowsing where `month`='"+month+"' and groupid='"+groupid+"' and broadband_type='"+broadband_type+"'  group by groupname,probetype";
		List<Map<String, Object>> avgdelayList = getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return avgdelayList;

	}

	@Override
	public List<Map<String, Object>> getCityVideoDelayVideo(String groupid, String month, String broadband_type) {
		
		String sql = "";
		if(groupid.contains(",")){
			sql = "SELECT groupname cityname, CASE probetype WHEN 'Android' THEN '安卓' WHEN 'PC' THEN '桌面' WHEN 'iOS' THEN '苹果'  END usertype, 'H5视频' kpitypename,'首次缓冲时延' fieldname, " +
					" CONCAT(round(sum(success_rate*video_test_times)/sum(video_test_times),2)*100,'%') successratevalue ,   CONCAT(ROUND(sum(first_buffer_delay*video_test_times)/sum(video_test_times),0),'ms') avgvalue ,CONCAT(ROUND(sum(top75_first_buffer_delay*video_test_times)/sum(video_test_times),0),'ms') percent75,CONCAT(ROUND(sum(top85_first_buffer_delay*video_test_times)/sum(video_test_times),0),'ms') percent85,CONCAT(ROUND(sum(top95_first_buffer_delay*video_test_times)/sum(video_test_times),0),'ms') percent95 " +
					"  FROM servicequality_orgid_video  where `month`='"+month+"' and groupid in("+groupid+") and broadband_type='"+broadband_type+"' GROUP BY groupname,probetype";
		}else{
			sql = "SELECT orgname cityname, CASE probetype WHEN 'Android' THEN '安卓' WHEN 'PC' THEN '桌面' WHEN 'iOS' THEN '苹果'  END usertype, 'H5视频' kpitypename,'首次缓冲时延' fieldname, " +
					" CONCAT(round(success_rate,2)*100,'%') successratevalue ,   CONCAT(ROUND(first_buffer_delay,0),'ms') avgvalue ,CONCAT(ROUND(top75_first_buffer_delay,0),'ms') percent75,CONCAT(ROUND(top85_first_buffer_delay,0),'ms') percent85,CONCAT(ROUND(top95_first_buffer_delay,0),'ms') percent95 " +
					"  FROM servicequality_orgid_video  where `month`='"+month+"' and groupid='"+groupid+"' and broadband_type='"+broadband_type+"' GROUP BY orgname,	probetype";
		}

		List<Map<String, Object>> avgdelayList = getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return avgdelayList;

	}

	@Override
	public List<Map<String, Object>> getCitycacheCountVideo(String groupid, String month, String broadband_type) {

		String sql = "";
		if(groupid.contains(",")){
			sql = "SELECT groupname cityname, CASE probetype WHEN 'Android' THEN '安卓' WHEN 'PC' THEN '桌面' WHEN 'iOS' THEN '苹果'  END usertype, 'H5视频' kpitypename,'卡顿次数' fieldname, " +
					"  CONCAT(round(sum(success_rate*video_test_times)/sum(video_test_times),2)*100,'%') successratevalue ,  CONCAT(ROUND(sum(video_cache_count*video_test_times)/sum(video_test_times),1),'次') avgvalue ,CONCAT(ROUND(sum(top75_cache_count*video_test_times)/sum(video_test_times),0),'次') percent75,CONCAT(ROUND(sum(top85_cache_count*video_test_times)/sum(video_test_times),0),'次') percent85,CONCAT(ROUND(sum(top95_cache_count*video_test_times)/sum(video_test_times),0),'次') percent95 " +
					"  FROM servicequality_orgid_video  where `month`='"+month+"' and groupid in("+groupid+") and broadband_type='"+broadband_type+"' group by groupname,probetype";
		}else{
			sql = "SELECT orgname cityname, CASE probetype WHEN 'Android' THEN '安卓' WHEN 'PC' THEN '桌面' WHEN 'iOS' THEN '苹果'  END usertype, 'H5视频' kpitypename,'卡顿次数' fieldname, " +
					"  CONCAT(round(success_rate,2)*100,'%') successratevalue ,  CONCAT(ROUND(video_cache_count,1),'次') avgvalue ,CONCAT(ROUND(top75_cache_count,0),'次') percent75,CONCAT(ROUND(top85_cache_count,0),'次') percent85,CONCAT(ROUND(top95_cache_count,0),'次') percent95 " +
					"  FROM servicequality_orgid_video  where `month`='"+month+"' and groupid='"+groupid+"' and broadband_type='"+broadband_type+"' GROUP BY orgname,	probetype";
		}
		
		List<Map<String, Object>> avgdelayList = getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return avgdelayList;

	}

	@Override
	public List<Map<String, Object>> getCityPlaySuccessVideo(String groupid, String month, String broadband_type) {

		String sql = "SELECT orgname cityname, CASE probetype WHEN 'Android' THEN '安卓' WHEN 'PC' THEN '桌面' WHEN 'iOS' THEN '苹果'  END usertype, 'H5视频' kpitypename,'播放成功率' fieldname, " +
				"  CONCAT(round(success_rate,2)*100,'%') successratevalue ,  ifnull(avg_paly_success*100,'N/A') avgvalue ,IFNULL(top75_avg_paly_success*100,'N/A') percent75,ifNULL(top85_avg_paly_success*100,'N/A') percent85,ifNULL(top95_avg_paly_success*100,'N/A') percent95 " +
				"  FROM servicequality_orgid_video where `month`='"+month+"' and groupid='"+groupid+"' and broadband_type='"+broadband_type+"'";
		List<Map<String, Object>> avgdelayList = getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return avgdelayList;

	}

	@Override
	public List<Map<String, Object>> getCityBufferproportionVideo(String groupid, String month, String broadband_type) {

		String sql = "";
		if(groupid.contains(",")){
			sql = "SELECT groupname cityname, CASE probetype WHEN 'Android' THEN '安卓' WHEN 'PC' THEN '桌面' WHEN 'iOS' THEN '苹果'  END usertype, 'H5视频' kpitypename,'缓冲时长占比' fieldname, " +
					" CONCAT(round(sum(success_rate*video_test_times)/sum(video_test_times),2)*100,'%') successratevalue ,   CONCAT(ROUND(sum(avg_buffer_proportion*video_test_times)/sum(video_test_times),2)*100,'%') avgvalue ,CONCAT(ROUND(sum(top75_avg_buffer_proportion*video_test_times)/sum(video_test_times),2)*100,'%') percent75,CONCAT(ROUND(sum(top85_avg_buffer_proportion*video_test_times)/sum(video_test_times),2)*100,'%') percent85,CONCAT(ROUND(sum(top95_avg_buffer_proportion*video_test_times)/sum(video_test_times),2)*100,'%') percent95 " +
					"  FROM servicequality_orgid_video  where `month`='"+month+"' and groupid in("+groupid+") and broadband_type='"+broadband_type+"' group by groupname,probetype";
		}else{
			sql = "SELECT orgname cityname, CASE probetype WHEN 'Android' THEN '安卓' WHEN 'PC' THEN '桌面' WHEN 'iOS' THEN '苹果'  END usertype, 'H5视频' kpitypename,'缓冲时长占比' fieldname, " +
					" CONCAT(round(success_rate,2)*100,'%') successratevalue ,   CONCAT(ROUND(avg_buffer_proportion,2)*100,'%') avgvalue ,CONCAT(ROUND(top75_avg_buffer_proportion,2)*100,'%') percent75,CONCAT(ROUND(top85_avg_buffer_proportion,2)*100,'%') percent85,CONCAT(ROUND(top95_avg_buffer_proportion,2)*100,'%') percent95 " +
					"  FROM servicequality_orgid_video  where `month`='"+month+"' and groupid='"+groupid+"' and broadband_type='"+broadband_type+"'  GROUP BY orgname,	probetype";
		}
		
		List<Map<String, Object>> avgdelayList = getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return avgdelayList;

	}

	//------------------------------------------------------------------------竞品对标数据分析-----------------------------------------------------------------------

	@Override
	public List<Map<String, Object>> getSpecialCaseHttpDownloadRate(String groupid, String month, String broadband_type) {

		String sql = "select  CASE probetype WHEN 'Android' THEN '安卓' WHEN 'PC' THEN '桌面' WHEN 'iOS' THEN '苹果'  END  probetype  ,  case  when operator LIKE '移动' or '中国移动' then '移动' when operator LIKE '联通' or '中国联通' then '联通' when operator LIKE '电信' or '中国电信' THEN '电信' end  operator, " +
				" '互联网下载速率' kpiname ,  CONCAT(round(sum(download_test_times*success_rate)/sum(download_test_times),2)*100,'%') successratevalue ,  "
				+ "  CONCAT(round(sum(download_test_times*top95_download_rate)/(sum(download_test_times)),2),\"Mbps\") percent95 ,CONCAT(round(sum(download_test_times*top85_download_rate)/(sum(download_test_times)),2),\"Mbps\")  percent85,CONCAT(round(sum(download_test_times*top75_download_rate)/(sum(download_test_times)),2),\"Mbps\") percent75,"
				+ " CONCAT(round(sum(download_test_times*avg_download_rate)/sum(download_test_times),2),\"Mbps\") avgvalue " + " from servicequality_operator_httpdownload where month='" + month
				+ "' and  groupid in (" + groupid + ")   and broadband_type='" + broadband_type + "' and(operator LIKE '%移动' or operator LIKE '%联通' or operator LIKE '%电信')  group by probetype, " +
				" case  when operator LIKE '移动' or '中国移动' then '移动' when operator LIKE '联通' or '中国联通' then '联通' when operator LIKE '电信' or '中国电信' THEN '电信' end  order by  CASE probetype WHEN 'Android' THEN '1' WHEN 'PC' THEN '3' WHEN 'iOS' THEN '2'  END,case  when operator LIKE '移动' or'中国移动' then '1移动' when operator LIKE '联通' or '中国联通' then '3联通' when operator LIKE '电信' or '中国电信' THEN '2电信' end  ";
		List<Map<String, Object>> proportionList = getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return proportionList;

	}

	@Override
	public List<Map<String, Object>> getSpecialCaseHttpUpload(String groupid, String month, String broadband_type) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Map<String, Object>> getSpecialCasebufferVideo(String groupid, String month, String broadband_type) {

		String sql = "select CASE probetype WHEN 'Android' THEN '安卓' WHEN 'PC' THEN '桌面' WHEN 'iOS' THEN '苹果'  END  probetype ,  case  when operator LIKE '移动' or '中国移动' then '移动' when operator LIKE '联通' or '中国联通' then '联通' when operator LIKE '电信' or '中国电信' THEN '电信' end  operator, " +
				" 'H5视频-缓冲时长占比' kpiname ,  CONCAT(round(sum(video_test_times*success_rate)/sum(video_test_times),2)*100,'%') successratevalue ,  "
				+ "  CONCAT(round(sum(video_test_times*top95_avg_buffer_proportion)/(sum(video_test_times)),2)*100,'%') percent95 ,CONCAT(round(sum(video_test_times*top85_avg_buffer_proportion)/(sum(video_test_times)),2)*100,'%')  percent85,CONCAT(round(sum(video_test_times*top75_avg_buffer_proportion)/(sum(video_test_times)),2)*100,'%') percent75,"
				+ " CONCAT(round(sum(video_test_times*avg_buffer_proportion)/sum(video_test_times),2)*100,'%') avgvalue " + " from servicequality_operator_video where month='" + month
				+ "' and groupid in (" + groupid + ") and broadband_type='" + broadband_type + "' and(operator LIKE '%移动' or operator LIKE '%联通' or operator LIKE '%电信')  group by probetype, " +
				" case  when operator LIKE '移动' or '中国移动' then '移动' when operator LIKE '联通' or '中国联通' then '联通' when operator LIKE '电信' or '中国电信' THEN '电信' end  order by  CASE probetype WHEN 'Android' THEN '1' WHEN 'PC' THEN '3' WHEN 'iOS' THEN '2'  END,case  when operator LIKE '移动' or'中国移动' then '1移动' when operator LIKE '联通' or '中国联通' then '3联通' when operator LIKE '电信' or '中国电信' THEN '2电信' end  ";
		List<Map<String, Object>> proportionList = getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return proportionList;

	}

	@Override
	public List<Map<String, Object>> getSpecialCaseFirstDelayVideo(String groupid, String month, String broadband_type) {

		String sql = "select CASE probetype WHEN 'Android' THEN '安卓' WHEN 'PC' THEN '桌面' WHEN 'iOS' THEN '苹果'  END  probetype ,  case  when operator LIKE '移动' or '中国移动' then '移动' when operator LIKE '联通' or '中国联通' then '联通' when operator LIKE '电信' or '中国电信' THEN '电信' end  operator, " +
				" 'H5视频首次缓冲时延' kpiname ,  CONCAT(round(sum(video_test_times*success_rate)/sum(video_test_times),2)*100,'%') successratevalue ,  "
				+ "  CONCAT(round(sum(video_test_times*top95_first_buffer_delay)/(sum(video_test_times)),0),'ms') percent95 ,CONCAT(round(sum(video_test_times*top85_first_buffer_delay)/(sum(video_test_times)),0),'ms')  percent85,CONCAT(round(sum(video_test_times*top75_first_buffer_delay)/(sum(video_test_times)),0),'ms') percent75,"
				+ " CONCAT(round(sum(video_test_times*first_buffer_delay)/sum(video_test_times),0),'ms') avgvalue " + " from servicequality_operator_video where month='" + month
				+ "' and  groupid in (" + groupid + ")  and broadband_type='" + broadband_type + "' and(operator LIKE '%移动' or operator LIKE '%联通' or operator LIKE '%电信')  group by probetype, " +
				" case  when operator LIKE '移动' or '中国移动' then '移动' when operator LIKE '联通' or '中国联通' then '联通' when operator LIKE '电信' or '中国电信' THEN '电信' end  order by  CASE probetype WHEN 'Android' THEN '1' WHEN 'PC' THEN '3' WHEN 'iOS' THEN '2'  END,case  when operator LIKE '移动' or'中国移动' then '1移动' when operator LIKE '联通' or '中国联通' then '3联通' when operator LIKE '电信' or '中国电信' THEN '2电信' end  ";
		List<Map<String, Object>> proportionList = getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return proportionList;

	}

	@Override
	public List<Map<String, Object>> getSpecialCaseCacheCountVideo(String groupid, String month, String broadband_type) {

		String sql = "select CASE probetype WHEN 'Android' THEN '安卓' WHEN 'PC' THEN '桌面' WHEN 'iOS' THEN '苹果'  END  probetype ,  case  when operator LIKE '移动' or '中国移动' then '移动' when operator LIKE '联通' or '中国联通' then '联通' when operator LIKE '电信' or '中国电信' THEN '电信' end  operator, " +
				" 'H5视频卡顿次数' kpiname ,  CONCAT(round(sum(video_test_times*success_rate)/sum(video_test_times),2)*100,'%') successratevalue ,  "
				+ "  CONCAT(round(sum(video_test_times*top95_cache_count)/(sum(video_test_times)),1),\"次\") percent95 ,CONCAT(round(sum(video_test_times*top85_cache_count)/(sum(video_test_times)),1),\"次\")  percent85,CONCAT(round(sum(video_test_times*top75_cache_count)/(sum(video_test_times)),1),\"次\") percent75,"
				+ " CONCAT(round(sum(video_test_times*video_cache_count)/sum(video_test_times),1),\"次\") avgvalue " + " from servicequality_operator_video where month='" + month
				+ "' and  groupid in (" + groupid + ")  and broadband_type='" + broadband_type + "' and(operator LIKE '%移动' or operator LIKE '%联通' or operator LIKE '%电信')  group by probetype, " +
				" case  when operator LIKE '移动' or '中国移动' then '移动' when operator LIKE '联通' or '中国联通' then '联通' when operator LIKE '电信' or '中国电信' THEN '电信' end  order by  CASE probetype WHEN 'Android' THEN '1' WHEN 'PC' THEN '3' WHEN 'iOS' THEN '2'  END,case  when operator LIKE '移动' or'中国移动' then '1移动' when operator LIKE '联通' or '中国联通' then '3联通' when operator LIKE '电信' or '中国电信' THEN '2电信' end  ";
		List<Map<String, Object>> proportionList = getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return proportionList;

	}

	@Override
	public List<Map<String, Object>> getSpecialCaseNinetyDelayWebbrowsing(String groupid, String month, String broadband_type) {

		String sql = "select CASE probetype WHEN 'Android' THEN '安卓' WHEN 'PC' THEN '桌面' WHEN 'iOS' THEN '苹果'  END  probetype ,  case  when operator LIKE '移动' or '中国移动' then '移动' when operator LIKE '联通' or '中国联通' then '联通' when operator LIKE '电信' or '中国电信' THEN '电信' end  operator, " +
				" '网页浏览90%加载时延' kpiname ,  CONCAT(round(sum(page_test_times*success_rate)/sum(page_test_times),2)*100,'%') successratevalue ,  "
				+ "  CONCAT(round(sum(page_test_times*top95_page_avg_ninetydelay)/(sum(page_test_times)),0),'ms') percent95 ,CONCAT(round(sum(page_test_times*top85_page_avg_ninetydelay)/(sum(page_test_times)),0),'ms')  percent85,CONCAT(round(sum(page_test_times*top75_page_avg_ninetydelay)/(sum(page_test_times)),0),'ms') percent75,"
				+ " CONCAT(round(sum(page_test_times*page_avg_ninetydelay)/sum(page_test_times),0),'ms') avgvalue " + " from servicequality_operator_webbrowsing where month='" + month
				+ "' and  groupid in (" + groupid + ")  and broadband_type='" + broadband_type + "' and(operator LIKE '%移动' or operator LIKE '%联通' or operator LIKE '%电信')  group by probetype, " +
				" case  when operator LIKE '移动' or '中国移动' then '移动' when operator LIKE '联通' or '中国联通' then '联通' when operator LIKE '电信' or '中国电信' THEN '电信' end  order by  CASE probetype WHEN 'Android' THEN '1' WHEN 'PC' THEN '3' WHEN 'iOS' THEN '2'  END,case  when operator LIKE '移动' or'中国移动' then '1移动' when operator LIKE '联通' or '中国联通' then '3联通' when operator LIKE '电信' or '中国电信' THEN '2电信' end  ";
		List<Map<String, Object>> proportionList = getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return proportionList;

	}

	@Override
	public List<Map<String, Object>> getSpecialCaseAvgDelayWebbrowsing(String groupid, String month, String broadband_type) {

		String sql = "select CASE probetype WHEN 'Android' THEN '安卓' WHEN 'PC' THEN '桌面' WHEN 'iOS' THEN '苹果'  END  probetype ,  case  when operator LIKE '移动' or '中国移动' then '移动' when operator LIKE '联通' or '中国联通' then '联通' when operator LIKE '电信' or '中国电信' THEN '电信' end  operator, " +
				" '网页浏览100%加载时延' kpiname ,  CONCAT(round(sum(page_test_times*success_rate)/sum(page_test_times),2)*100,'%') successratevalue ,  "
				+ "  CONCAT(round(sum(page_test_times*top95_page_delay)/(sum(page_test_times)),0),'ms') percent95 ,CONCAT(round(sum(page_test_times*top85_page_delay)/(sum(page_test_times)),0),'ms')  percent85,CONCAT(round(sum(page_test_times*top75_page_delay)/(sum(page_test_times)),0),'ms') percent75,"
				+ " CONCAT(round(sum(page_test_times*page_avg_delay)/sum(page_test_times),0),'ms') avgvalue " + " from servicequality_operator_webbrowsing where month='" + month
				+ "' and  groupid in (" + groupid + ")  and broadband_type='" + broadband_type + "' and(operator LIKE '%移动' or operator LIKE '%联通' or operator LIKE '%电信')  group by probetype, " +
				" case  when operator LIKE '移动' or '中国移动' then '移动' when operator LIKE '联通' or '中国联通' then '联通' when operator LIKE '电信' or '中国电信' THEN '电信' end   order by  CASE probetype WHEN 'Android' THEN '1' WHEN 'PC' THEN '3' WHEN 'iOS' THEN '2'  END,case  when operator LIKE '移动' or'中国移动' then '1移动' when operator LIKE '联通' or '中国联通' then '3联通' when operator LIKE '电信' or '中国电信' THEN '2电信' end ";
		List<Map<String, Object>> proportionList = getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return proportionList;

	}


	
	
}
