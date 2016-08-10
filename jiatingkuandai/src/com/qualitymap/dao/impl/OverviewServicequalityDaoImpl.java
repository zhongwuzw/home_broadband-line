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
	public List<Map<String, Object>> getHttpDownloadUserIndicator(String groupid, String month, String broadband_type) {

		String sql = "select probetype ,sum(download_test_times) validsample,  CONCAT(round(sum(download_test_times*avg_download_rate)/sum(download_test_times),2),\"Mbps\") avgvalue, '互联网下载-速率' kpiname ,CONCAT(round(sum(download_test_times*top95_download_rate)/sum(download_test_times),2),\"Mbps\") percent95 ,CONCAT(round(sum(download_test_times*top85_download_rate)/sum(download_test_times),2),\"Mbps\") percent85 , CONCAT(round(sum(download_test_times*top75_download_rate)/sum(download_test_times),2),\"Mbps\") percent75    from servicequality_orgid_httpdownload d where month='"
				+ month + "' and groupid ='" + groupid + "' and month='" + month + "' and broadband_type='" + broadband_type + "' group by probetype";
		List<Map<String, Object>> downloadList = getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return downloadList;

	}

	@Override
	public List<Map<String, Object>> getHttpDownloadSuccessUserIndicator(String groupid, String month, String broadband_type) {

		String sql = "select probetype ,sum(download_test_times) validsample,  CONCAT(round(sum(download_test_times*success_rate)/sum(download_test_times),1)*100,'%') avgvalue , '互联网下载-成功率' kpiname , ifnull(round(sum(download_test_times*top95_success_rate)/sum(download_test_times),1),'N/A') percent95 , IFNULL(round(sum(download_test_times*top85_success_rate)/sum(download_test_times),1),'N/A') percent85, IFNULL(round(sum(download_test_times*top75_success_rate)/sum(download_test_times),1),'N/A') percent75  from servicequality_orgid_httpdownload d where month='"
				+ month + "' and groupid ='" + groupid + "' and month='" + month + "' and broadband_type='" + broadband_type + "'  group by probetype ";
		List<Map<String, Object>> downloadList = getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return downloadList;

	}

	@Override
	public List<Map<String, Object>> getHttpUploadUserIndicator(String groupid, String month, String broadband_type) {

		String sql = "select *  from servicequality_groupid_httpupload where month='" + month + "' and groupid ='" + groupid + "' and month='" + month + "' and broadband_type='" + broadband_type
				+ "' ";
		List<Map<String, Object>> downloadList = getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return downloadList;

	}

	@Override
	public List<Map<String, Object>> getVideoProportionUserIndicator(String groupid, String month, String broadband_type) {

		String sql = "select probetype , 'H5视频-缓冲时长占比' kpiname ,  "
				+ "  CONCAT(round(video_test_times*top95_avg_buffer_proportion/(sum(video_test_times)),2)*100,'%') percent95 ,CONCAT(round(video_test_times*top85_avg_buffer_proportion/(sum(video_test_times)),2)*100,'%')  percent85,CONCAT(round(video_test_times*top75_avg_buffer_proportion/(sum(video_test_times)),2)*100,'%') percent75,"
				+ "sum(video_test_times) validsample ,CONCAT(round(video_test_times*avg_buffer_proportion/sum(video_test_times),2)*100,'%') avgvalue " + " from servicequality_orgid_video where month='" + month
				+ "' and groupid ='" + groupid + "' and month='" + month + "' and broadband_type='" + broadband_type + "'   group by probetype ";
		List<Map<String, Object>> proportionList = getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return proportionList;

	}

	@Override
	public List<Map<String, Object>> getVideoDelayUserIndicator(String groupid, String month, String broadband_type) {

		String sql = "select probetype , 'H5视频-播放平均时延' kpiname ,  "
				+ " CONCAT(round(sum(video_test_times*top95_video_delay)/(sum(video_test_times)),0),\"ms\") percent95 ,CONCAT(round(sum(video_test_times*top85_video_delay)/(sum(video_test_times)),0),\"ms\") percent85,CONCAT(round(sum(video_test_times*top75_video_delay)/(sum(video_test_times)),0),\"ms\") percent75,"
				+ "sum(video_test_times) validsample ,CONCAT(round(sum(video_test_times*avg_video_delay)/sum(video_test_times),0),\"ms\") avgvalue " + " from servicequality_orgid_video where month='"
				+ month + "' and groupid ='" + groupid + "' and month='" + month + "' and broadband_type='" + broadband_type + "'   group by probetype ";
		List<Map<String, Object>> avgdelayList = getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return avgdelayList;

	}

	@Override
	public List<Map<String, Object>> getVideoCache_countUserIndicator(String groupid, String month, String broadband_type) {

		String sql = "select probetype , 'H5视频-卡顿次数' kpiname ,  "
				+ " CONCAT(round(sum(video_test_times*top95_cache_count)/(sum(video_test_times)),1),\"次\") percent95 ,CONCAT(round(sum(video_test_times*top85_cache_count)/(sum(video_test_times)),1),\"次\") percent85,CONCAT(round(sum(video_test_times*top75_cache_count)/(sum(video_test_times)),1),\"次\") percent75,"
				+ "sum(video_test_times) validsample , CONCAT(round(sum(video_test_times*video_cache_count)/sum(video_test_times),1),\"次\") avgvalue " + " from servicequality_orgid_video where month='"
				+ month + "' and groupid ='" + groupid + "' and month='" + month + "' and broadband_type='" + broadband_type + "'   group by probetype ";
		List<Map<String, Object>> avgdelayList = getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return avgdelayList;

	}

	@Override
	public List<Map<String, Object>> getVideoPaly_successUserIndicator(String groupid, String month, String broadband_type) {

		String sql = "select probetype , 'H5视频-播放成功率' kpiname ,  "
				+ " ifnull(round(sum(video_test_times*top95_avg_paly_success)/(sum(video_test_times)),2),'N/A') percent95 ,IFNULL(round(sum(video_test_times*top85_avg_paly_success)/(sum(video_test_times)),2),'N/A') percent85,IFNULL(round(sum(video_test_times*top75_avg_paly_success)/(sum(video_test_times)),2),'N/A') percent75,"
				+ "sum(video_test_times) validsample ,CONCAT(round(sum(video_test_times*success_rate)/sum(video_test_times),2)*100,'%') avgvalue " + " from servicequality_orgid_video where month='" + month
				+ "' and groupid ='" + groupid + "' and month='" + month + "' and broadband_type='" + broadband_type + "'   group by probetype ";
		List<Map<String, Object>> avgdelayList = getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return avgdelayList;

	}

	@Override
	public List<Map<String, Object>> getWebbrowsingninetydelayUserIndicator(String groupid, String month, String broadband_type) {

		String sql = "select probetype , '网页浏览-90%加载时延' kpiname ,  "
				+ " CONCAT(round(sum(page_test_times*top95_page_avg_ninetydelay)/(sum(page_test_times)),0),\"ms\") percent95 ,CONCAT(round(sum(page_test_times*top85_page_avg_ninetydelay)/(sum(page_test_times)),0),\"ms\") percent85,CONCAT(round(sum(page_test_times*top75_page_avg_ninetydelay)/(sum(page_test_times)),0),\"ms\") percent75,"
				+ "sum(page_test_times) validsample ,CONCAT(round(sum(page_test_times*page_avg_ninetydelay)/sum(page_test_times),0),\"ms\")  avgvalue "
				+ " from servicequality_orgid_webbrowsing where month='" + month + "' and groupid ='" + groupid + "' and month='" + month + "' and broadband_type='" + broadband_type
				+ "'   group by probetype ";
		List<Map<String, Object>> avgdelayList = getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return avgdelayList;

	}

	@Override
	public List<Map<String, Object>> getWebbrowsingAvgdelayUserIndicator(String groupid, String month, String broadband_type) {

		String sql = "select probetype , '网页浏览-100%加载时延' kpiname ,  "
				+ " CONCAT(round(sum(page_test_times*top95_page_delay)/(sum(page_test_times)),0),\"ms\") percent95 , CONCAT(round(sum(page_test_times*top85_page_delay)/(sum(page_test_times)),0),\"ms\") percent85, CONCAT(round(sum(page_test_times*top75_page_delay)/(sum(page_test_times)),0),\"ms\") percent75,"
				+ "sum(page_test_times) validsample ,CONCAT(round(sum(page_test_times*page_avg_delay)/sum(page_test_times),0),\"ms\") avgvalue " + " from servicequality_orgid_webbrowsing where month='"
				+ month + "' and groupid ='" + groupid + "' and month='" + month + "' and broadband_type='" + broadband_type + "'   group by probetype ";
		List<Map<String, Object>> avgdelayList = getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return avgdelayList;

	}

	@Override
	public List<Map<String, Object>> getWebbrowsingsuccessUserIndicator(String groupid, String month, String broadband_type) {

		String sql = "select probetype , '网页浏览-100%页面元素打开成功率' kpiname ,  "
				+ " CONCAT(round(sum(page_test_times*top95_page_success_rate)/(sum(page_test_times)),1),'%') percent95 ,CONCAT(round(sum(page_test_times*top85_page_success_rate)/(sum(page_test_times)),1),'%') percent85,CONCAT(round(sum(page_test_times*top75_page_success_rate)/(sum(page_test_times)),1),'%') percent75,"
				+ "sum(page_test_times) validsample ,CONCAT(round(sum(page_test_times*page_success_rate)/sum(page_test_times),1),'%') avgvalue " + " from servicequality_orgid_webbrowsing where month='" + month
				+ "' and groupid ='" + groupid + "' and month='" + month + "' and broadband_type='" + broadband_type + "'   group by probetype ";
		List<Map<String, Object>> avgdelayList = getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return avgdelayList;

	}
	
	@Override
	public List<Map<String, Object>> getWebbrowsingVisitSuccessRate(String groupid, String month, String broadband_type) {

		String sql = "select probetype , '网页浏览-访问成功率' kpiname ,  "
				+ " IFNULL(CONCAT(round(sum(page_test_times*top95_success_rate)/(sum(page_test_times)),1),'%'),'N/A') percent95 ,IFNULL(CONCAT(round(sum(page_test_times*top85_success_rate)/(sum(page_test_times)),1),'%'),'N/A') percent85,IFNULL(CONCAT(round(sum(page_test_times*top75_success_rate)/(sum(page_test_times)),1),'%'),'N/A') percent75,"
				+ "sum(page_test_times) validsample ,IFNULL(CONCAT(round(sum(page_test_times*success_rate)/sum(page_test_times),1)*100,'%'),'N/A') avgvalue " + " from servicequality_orgid_webbrowsing where month='" + month
				+ "' and groupid ='" + groupid + "' and month='" + month + "' and broadband_type='" + broadband_type + "'   group by probetype ";
		List<Map<String, Object>> avgdelayList = getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return avgdelayList;

	}

	// ----------------------------------------------------------------------上为获取用户指标统计---------------------------------------------------------------------

	// ----------------------------------------------------------------------下为获取地域用户指标统计---------------------------------------------------------------------
	@Override
	public List<Map<String, Object>> getTerritoryHttpDownload(String groupid, String month, String broadband_type) {

		String sql = "select url_num , '互联网下载' testtype,  orgname cityname ," + "sum(new_sample_num) samplenum , sum(download_test_times) internetdownload , sum(download_test_times) validsample  from servicequality_orgid_httpdownload where " + "  groupid ='"
				+ groupid + "' and month='" + month + "' and broadband_type='" + broadband_type + "'   group by orgname ";
		List<Map<String, Object>> avgdelayList = getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return avgdelayList;

	}

	@Override
	public List<Map<String, Object>> getTerritoryHttpUpload(String groupid, String month, String broadband_type) {

		String sql = "select  orgname cityname , url_num , '互联网下载' testtype, "
				+ "sum(new_sample_num) samplenum , sum(upload_test_times) internetdownload , sum(upload_test_times) validsample from servicequality_orgid_httpdownload where " + "  groupid ='"
				+ groupid + "' and month='" + month + "' and broadband_type='" + broadband_type + "'   group by orgname ";
		List<Map<String, Object>> avgdelayList = getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return avgdelayList;

	}

	@Override
	public List<Map<String, Object>> getTerritoryVideo(String groupid, String month, String broadband_type) {

		String sql = "select url_num , 'H5视频' testtype,  orgname cityname ," + "sum(new_sample_num) samplenum , sum(video_test_times) h5video , sum(video_test_times) validsample from servicequality_orgid_video where "
				+ "  groupid ='" + groupid + "' and month='" + month + "' and broadband_type='" + broadband_type + "'   group by orgname ";
		List<Map<String, Object>> avgdelayList = getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return avgdelayList;

	}

	@Override
	public List<Map<String, Object>> getTerritoryWebbrowsing(String groupid, String month, String broadband_type) {

		String sql = "select  url_num , '网页浏览' testtype, orgname cityname ," + "sum(new_sample_num) samplenum , sum(page_test_times) webbrowse , sum(page_test_times) validsample from servicequality_orgid_webbrowsing where "
				+ "  groupid ='" + groupid + "' and month='" + month + "' and broadband_type='" + broadband_type + "'   group by orgname ";
		List<Map<String, Object>> avgdelayList = getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return avgdelayList;

	}
	//----------------------------------------------------------------------------------------------------------------------------------

	@Override
	public List<Map<String, Object>> getCityrateHttpdownload(String groupid, String month, String broadband_type) {

		String sql = "SELECT orgname cityname,  CASE probetype WHEN 'Android' THEN '安卓' WHEN 'PC' THEN '桌面' WHEN 'iOS' THEN '苹果'  END usertype, '互联网下载' kpitypename,'速率' fieldname, " +
				" CONCAT(avg_download_rate,'Mbps') avgvalue, CONCAT(top75_download_rate,'Mbps') percent75,CONCAT(top85_download_rate,'Mbps') percent85,CONCAT(top95_download_rate,'Mbps') percent95 " +
				"  FROM servicequality_orgid_httpdownload where `month`='"+month+"' and groupid='"+groupid+"' and broadband_type='"+broadband_type+"'";
		List<Map<String, Object>> avgdelayList = getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return avgdelayList;

	}

	@Override
	public List<Map<String, Object>> getCitysuccessHttpdownload(String groupid, String month, String broadband_type) {

		String sql = "SELECT orgname cityname, CASE probetype WHEN 'Android' THEN '安卓' WHEN 'PC' THEN '桌面' WHEN 'iOS' THEN '苹果'  END usertype, '互联网下载' kpitypename,'成功率' fieldname, " +
				" ifnull(avg_success_rate*100,'N/A') avgvalue ,IFNULL(top75_success_rate,'N/A') percent75,ifNULL(top85_success_rate,'N/A') percent85,ifNULL(top95_success_rate*100,'N/A') percent95 " +
				"  FROM servicequality_orgid_httpdownload where `month`='"+month+"' and groupid='"+groupid+"' and broadband_type='"+broadband_type+"'";
		List<Map<String, Object>> avgdelayList = getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return avgdelayList;

	}

	@Override
	public List<Map<String, Object>> getCityNinetyDelayWebbrowsing(String groupid, String month, String broadband_type) {

		String sql = "SELECT orgname cityname, CASE probetype WHEN 'Android' THEN '安卓' WHEN 'PC' THEN '桌面' WHEN 'iOS' THEN '苹果'  END usertype, '网页浏览' kpitypename,'90%加载时延' fieldname, " +
				" CONCAT(ROUND(page_avg_ninetydelay,0),'ms') avgvalue ,CONCAT(ROUND(top75_page_avg_ninetydelay,0),'ms') percent75,CONCAT(ROUND(top85_page_avg_ninetydelay,0),'ms') percent85,CONCAT(ROUND(top95_page_avg_ninetydelay,0),'ms') percent95 " +
				"  FROM servicequality_orgid_webbrowsing  where `month`='"+month+"' and groupid='"+groupid+"' and broadband_type='"+broadband_type+"'";
		List<Map<String, Object>> avgdelayList = getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return avgdelayList;

	}

	@Override
	public List<Map<String, Object>> getCityAvgDelayWebbrowsing(String groupid, String month, String broadband_type) {

		String sql = "SELECT orgname cityname, CASE probetype WHEN 'Android' THEN '安卓' WHEN 'PC' THEN '桌面' WHEN 'iOS' THEN '苹果'  END usertype, '网页浏览' kpitypename,'100%加载时延' fieldname, " +
				" CONCAT(ROUND(page_avg_delay,0),'ms') avgvalue ,CONCAT(ROUND(top75_page_delay,0),'ms') percent75,CONCAT(ROUND(top85_page_delay,0),'ms') percent85,CONCAT(ROUND(top95_page_delay,0),'ms') percent95 " +
				"  FROM servicequality_orgid_webbrowsing  where `month`='"+month+"' and groupid='"+groupid+"' and broadband_type='"+broadband_type+"'";
		List<Map<String, Object>> avgdelayList = getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return avgdelayList;

	}

	@Override
	public List<Map<String, Object>> getCityPageSuccessRateWebbrowsing(String groupid, String month, String broadband_type) {

		String sql = "SELECT orgname cityname, CASE probetype WHEN 'Android' THEN '安卓' WHEN 'PC' THEN '桌面' WHEN 'iOS' THEN '苹果'  END usertype, '网页浏览' kpitypename,'100%页面元素打开成功率' fieldname, " +
				" IFNULL(page_success_rate,'N/A') avgvalue ,IFNULL(top75_page_success_rate,'N/A') percent75,ifNULL(top85_page_success_rate,'N/A') percent85,ifNULL(top95_page_success_rate,'N/A') percent95 " +
				"  FROM servicequality_orgid_webbrowsing where `month`='"+month+"' and groupid='"+groupid+"' and broadband_type='"+broadband_type+"'";
		List<Map<String, Object>> avgdelayList = getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return avgdelayList;

	}

	@Override
	public List<Map<String, Object>> getCitySuccessWebbrowsing(String groupid, String month, String broadband_type) {

		String sql = "SELECT orgname cityname, CASE probetype WHEN 'Android' THEN '安卓' WHEN 'PC' THEN '桌面' WHEN 'iOS' THEN '苹果'  END usertype, '网页浏览' kpitypename,'访问成功率' fieldname, " +
				" ifnull(round(success_rate,2)*100,'N/A') avgvalue ,IFNULL(top75_success_rate*100,'N/A') percent75,ifNULL(top85_success_rate*100,'N/A') percent85,ifNULL(top95_success_rate*100,'N/A') percent95 " +
				"  FROM servicequality_orgid_webbrowsing where `month`='"+month+"' and groupid='"+groupid+"' and broadband_type='"+broadband_type+"'";
		List<Map<String, Object>> avgdelayList = getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return avgdelayList;

	}

	@Override
	public List<Map<String, Object>> getCityVideoDelayVideo(String groupid, String month, String broadband_type) {

		String sql = "SELECT orgname cityname, CASE probetype WHEN 'Android' THEN '安卓' WHEN 'PC' THEN '桌面' WHEN 'iOS' THEN '苹果'  END usertype, 'H5视频' kpitypename,'播放平均时延' fieldname, " +
				" CONCAT(ROUND(avg_video_delay,0),'ms') avgvalue ,CONCAT(ROUND(top75_video_delay,0),'ms') percent75,CONCAT(ROUND(top85_video_delay,0),'ms') percent85,CONCAT(ROUND(top95_video_delay,0),'ms') percent95 " +
				"  FROM servicequality_orgid_video  where `month`='"+month+"' and groupid='"+groupid+"' and broadband_type='"+broadband_type+"'";
		List<Map<String, Object>> avgdelayList = getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return avgdelayList;

	}

	@Override
	public List<Map<String, Object>> getCitycacheCountVideo(String groupid, String month, String broadband_type) {

		String sql = "SELECT orgname cityname, CASE probetype WHEN 'Android' THEN '安卓' WHEN 'PC' THEN '桌面' WHEN 'iOS' THEN '苹果'  END usertype, 'H5视频' kpitypename,'卡顿次数' fieldname, " +
				" CONCAT(ROUND(video_cache_count,1),'次') avgvalue ,CONCAT(ROUND(top75_cache_count,0),'次') percent75,CONCAT(ROUND(top85_cache_count,0),'次') percent85,CONCAT(ROUND(top95_cache_count,0),'次') percent95 " +
				"  FROM servicequality_orgid_video  where `month`='"+month+"' and groupid='"+groupid+"' and broadband_type='"+broadband_type+"'";
		List<Map<String, Object>> avgdelayList = getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return avgdelayList;

	}

	@Override
	public List<Map<String, Object>> getCityPlaySuccessVideo(String groupid, String month, String broadband_type) {

		String sql = "SELECT orgname cityname, CASE probetype WHEN 'Android' THEN '安卓' WHEN 'PC' THEN '桌面' WHEN 'iOS' THEN '苹果'  END usertype, 'H5视频' kpitypename,'播放成功率' fieldname, " +
				" ifnull(avg_paly_success*100,'N/A') avgvalue ,IFNULL(top75_avg_paly_success*100,'N/A') percent75,ifNULL(top85_avg_paly_success*100,'N/A') percent85,ifNULL(top95_avg_paly_success*100,'N/A') percent95 " +
				"  FROM servicequality_orgid_video where `month`='"+month+"' and groupid='"+groupid+"' and broadband_type='"+broadband_type+"'";
		List<Map<String, Object>> avgdelayList = getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return avgdelayList;

	}

	@Override
	public List<Map<String, Object>> getCityBufferproportionVideo(String groupid, String month, String broadband_type) {

		String sql = "SELECT orgname cityname, CASE probetype WHEN 'Android' THEN '安卓' WHEN 'PC' THEN '桌面' WHEN 'iOS' THEN '苹果'  END usertype, 'H5视频' kpitypename,'缓冲时长占比' fieldname, " +
				" CONCAT(ROUND(avg_buffer_proportion,2)*100,'%') avgvalue ,CONCAT(ROUND(top75_avg_buffer_proportion,2)*100,'%') percent75,CONCAT(ROUND(top85_avg_buffer_proportion,2)*100,'%') percent85,CONCAT(ROUND(top95_avg_buffer_proportion,2)*100,'%') percent95 " +
				"  FROM servicequality_orgid_video  where `month`='"+month+"' and groupid='"+groupid+"' and broadband_type='"+broadband_type+"'";
		List<Map<String, Object>> avgdelayList = getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return avgdelayList;

	}

	
	
}
