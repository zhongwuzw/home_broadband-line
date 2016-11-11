package com.qualitymap.dao.impl;

import java.util.List;
import java.util.Map;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.transform.Transformers;

import com.qualitymap.dao.ServicequalityperiodDao;

/**
 * 
 * @author kongxiangchun
 * 
 */
public class ServicequalityperiodDaoImpl implements ServicequalityperiodDao {

	private SessionFactory sessionFactory;

	public void setSessionFactory(SessionFactory sessionFactory) {
		this.sessionFactory = sessionFactory;
	}

	public Session getSession() {
		return sessionFactory.getCurrentSession();
	}

	@Override
	public List<Map<String, Object>> getPeriodrateHttpdownload(String groupid, String month, String broadband_type) {

		String sql = "";
		if (groupid.contains(",")) {
			sql = "SELECT period,  CASE probetype WHEN 'Android' THEN '安卓' WHEN 'PC' THEN '桌面' WHEN 'iOS' THEN '苹果'  END usertype, '互联网下载' kpitypename,'速率' fieldname, "
					+ " CONCAT(round(sum(success_rate*download_test_times)/sum(download_test_times),2)*100,'%') successratevalue ,  CONCAT(round(sum(avg_download_rate*download_test_times)/sum(download_test_times),2),'Mbps') avgvalue, CONCAT(round(sum(top75_download_rate*download_test_times)/sum(download_test_times),2),'Mbps') percent75,CONCAT(round(sum(top85_download_rate*download_test_times)/sum(download_test_times),2),'Mbps') percent85,CONCAT(round(sum(top95_download_rate*download_test_times)/sum(download_test_times),2),'Mbps') percent95 "
					+ "  FROM  servicequality_period_httpdownload  where `month`='" + month + "' and groupid in(" + groupid + ") and broadband_type='" + broadband_type
					+ "'  group by period,probetype";
		} else {
			sql = "SELECT period,  CASE probetype WHEN 'Android' THEN '安卓' WHEN 'PC' THEN '桌面' WHEN 'iOS' THEN '苹果'  END usertype, '互联网下载' kpitypename,'速率' fieldname, "
					+ " CONCAT(round(success_rate,2)*100,'%') successratevalue ,  CONCAT(avg_download_rate,'Mbps') avgvalue, CONCAT(top75_download_rate,'Mbps') percent75,CONCAT(top85_download_rate,'Mbps') percent85,CONCAT(top95_download_rate,'Mbps') percent95 "
					+ "  FROM servicequality_period_httpdownload where `month`='" + month + "' and groupid='" + groupid + "' and broadband_type='" + broadband_type + "'  GROUP BY period,	probetype";
		}

		List<Map<String, Object>> avgdelayList = getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return avgdelayList;

	}

	@Override
	public List<Map<String, Object>> getPeriodNinetyDelayWebbrowsing(String groupid, String month, String broadband_type) {

		String sql = "";

		if (groupid.contains(",")) {
			sql = "SELECT period, CASE probetype WHEN 'Android' THEN '安卓' WHEN 'PC' THEN '桌面' WHEN 'iOS' THEN '苹果'  END usertype, '网页浏览' kpitypename,'90%加载时延' fieldname, "
					+ " CONCAT(round(sum(success_rate*page_test_times)/sum(page_test_times),2)*100,'%') successratevalue ,  CONCAT(ROUND(sum(page_avg_ninetydelay*page_test_times)/sum(page_test_times),0),'ms') avgvalue ,CONCAT(ROUND(sum(top75_page_avg_ninetydelay*page_test_times)/sum(page_test_times),0),'ms') percent75,CONCAT(ROUND(sum(top85_page_avg_ninetydelay*page_test_times)/sum(page_test_times),0),'ms') percent85,CONCAT(ROUND(sum(top95_page_avg_ninetydelay*page_test_times)/sum(page_test_times),0),'ms') percent95 "
					+ "  FROM servicequality_period_webbrowsing  where `month`='" + month + "' and groupid in(" + groupid + ") and broadband_type='" + broadband_type + "'  group by period,probetype";
		} else {
			sql = "SELECT period, CASE probetype WHEN 'Android' THEN '安卓' WHEN 'PC' THEN '桌面' WHEN 'iOS' THEN '苹果'  END usertype, '网页浏览' kpitypename,'90%加载时延' fieldname, "
					+ " CONCAT(round(success_rate,2)*100,'%') successratevalue ,  CONCAT(ROUND(page_avg_ninetydelay,0),'ms') avgvalue ,CONCAT(ROUND(top75_page_avg_ninetydelay,0),'ms') percent75,CONCAT(ROUND(top85_page_avg_ninetydelay,0),'ms') percent85,CONCAT(ROUND(top95_page_avg_ninetydelay,0),'ms') percent95 "
					+ "  FROM servicequality_period_webbrowsing  where `month`='" + month + "' and groupid='" + groupid + "' and broadband_type='" + broadband_type + "'   group by period,probetype";
		}

		List<Map<String, Object>> avgdelayList = getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return avgdelayList;

	}

	@Override
	public List<Map<String, Object>> getPeriodAvgDelayWebbrowsing(String groupid, String month, String broadband_type) {

		String sql = "";
		if (groupid.contains(",")) {
			sql = "SELECT period, CASE probetype WHEN 'Android' THEN '安卓' WHEN 'PC' THEN '桌面' WHEN 'iOS' THEN '苹果'  END usertype, '网页浏览' kpitypename,'100%加载时延' fieldname, "
					+ " CONCAT(round(sum(success_rate*page_test_times)/sum(page_test_times),2)*100,'%') successratevalue ,  CONCAT(ROUND(sum(page_avg_delay*page_test_times)/sum(page_test_times),0),'ms') avgvalue ,CONCAT(ROUND(sum(top75_page_delay*page_test_times)/sum(page_test_times),0),'ms') percent75,CONCAT(ROUND(sum(top85_page_delay*page_test_times)/sum(page_test_times),0),'ms') percent85,CONCAT(ROUND(sum(top85_page_delay*page_test_times)/sum(page_test_times),0),'ms') percent95 "
					+ "  FROM servicequality_period_webbrowsing  where `month`='" + month + "' and groupid in(" + groupid + ") and broadband_type='" + broadband_type + "'  group by period,probetype";
		} else {
			sql = "SELECT period, CASE probetype WHEN 'Android' THEN '安卓' WHEN 'PC' THEN '桌面' WHEN 'iOS' THEN '苹果'  END usertype, '网页浏览' kpitypename,'100%加载时延' fieldname, "
					+ " CONCAT(round(success_rate,2)*100,'%') successratevalue ,  CONCAT(ROUND(page_avg_delay,0),'ms') avgvalue ,CONCAT(ROUND(top75_page_delay,0),'ms') percent75,CONCAT(ROUND(top85_page_delay,0),'ms') percent85,CONCAT(ROUND(top95_page_delay,0),'ms') percent95 "
					+ "  FROM servicequality_period_webbrowsing  where `month`='" + month + "' and groupid='" + groupid + "' and broadband_type='" + broadband_type + "'   group by period,probetype";
		}

		List<Map<String, Object>> avgdelayList = getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return avgdelayList;

	}

	@Override
	public List<Map<String, Object>> getPeriodVideoDelayVideo(String groupid, String month, String broadband_type) {

		String sql = "";
		if (groupid.contains(",")) {
			sql = "SELECT period, CASE probetype WHEN 'Android' THEN '安卓' WHEN 'PC' THEN '桌面' WHEN 'iOS' THEN '苹果'  END usertype, 'H5视频' kpitypename,'首次缓冲时延' fieldname, "
					+ " CONCAT(round(sum(success_rate*video_test_times)/sum(video_test_times),2)*100,'%') successratevalue ,   CONCAT(ROUND(sum(first_buffer_delay*video_test_times)/sum(video_test_times),0),'ms') avgvalue ,CONCAT(ROUND(sum(top75_first_buffer_delay*video_test_times)/sum(video_test_times),0),'ms') percent75,CONCAT(ROUND(sum(top85_first_buffer_delay*video_test_times)/sum(video_test_times),0),'ms') percent85,CONCAT(ROUND(sum(top95_first_buffer_delay*video_test_times)/sum(video_test_times),0),'ms') percent95 "
					+ "  FROM servicequality_period_video  where `month`='" + month + "' and groupid in(" + groupid + ") and broadband_type='" + broadband_type + "'   group by period,probetype";
		} else {
			sql = "SELECT period, CASE probetype WHEN 'Android' THEN '安卓' WHEN 'PC' THEN '桌面' WHEN 'iOS' THEN '苹果'  END usertype, 'H5视频' kpitypename,'首次缓冲时延' fieldname, "
					+ " CONCAT(round(success_rate,2)*100,'%') successratevalue ,   CONCAT(ROUND(first_buffer_delay,0),'ms') avgvalue ,CONCAT(ROUND(top75_first_buffer_delay,0),'ms') percent75,CONCAT(ROUND(top85_first_buffer_delay,0),'ms') percent85,CONCAT(ROUND(top95_first_buffer_delay,0),'ms') percent95 "
					+ "  FROM servicequality_period_video  where `month`='" + month + "' and groupid='" + groupid + "' and broadband_type='" + broadband_type + "'   group by period,	probetype";
		}

		List<Map<String, Object>> avgdelayList = getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return avgdelayList;

	}

	@Override
	public List<Map<String, Object>> getPeriodcacheCountVideo(String groupid, String month, String broadband_type) {

		String sql = "";
		if (groupid.contains(",")) {
			sql = "SELECT period, CASE probetype WHEN 'Android' THEN '安卓' WHEN 'PC' THEN '桌面' WHEN 'iOS' THEN '苹果'  END usertype, 'H5视频' kpitypename,'卡顿次数' fieldname, "
					+ "  CONCAT(round(sum(success_rate*video_test_times)/sum(video_test_times),2)*100,'%') successratevalue ,  CONCAT(ROUND(sum(video_cache_count*video_test_times)/sum(video_test_times),1),'次') avgvalue ,CONCAT(ROUND(sum(top75_cache_count*video_test_times)/sum(video_test_times),0),'次') percent75,CONCAT(ROUND(sum(top85_cache_count*video_test_times)/sum(video_test_times),0),'次') percent85,CONCAT(ROUND(sum(top95_cache_count*video_test_times)/sum(video_test_times),0),'次') percent95 "
					+ "  FROM servicequality_period_video  where `month`='" + month + "' and groupid in(" + groupid + ") and broadband_type='" + broadband_type + "'   group by period,probetype";
		} else {
			sql = "SELECT period, CASE probetype WHEN 'Android' THEN '安卓' WHEN 'PC' THEN '桌面' WHEN 'iOS' THEN '苹果'  END usertype, 'H5视频' kpitypename,'卡顿次数' fieldname, "
					+ "  CONCAT(round(success_rate,2)*100,'%') successratevalue ,  CONCAT(ROUND(video_cache_count,1),'次') avgvalue ,CONCAT(ROUND(top75_cache_count,0),'次') percent75,CONCAT(ROUND(top85_cache_count,0),'次') percent85,CONCAT(ROUND(top95_cache_count,0),'次') percent95 "
					+ "  FROM servicequality_period_video  where `month`='" + month + "' and groupid='" + groupid + "' and broadband_type='" + broadband_type + "'   group by period,	probetype";
		}

		List<Map<String, Object>> avgdelayList = getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return avgdelayList;

	}

	@Override
	public List<Map<String, Object>> getPeriodBufferproportionVideo(String groupid, String month, String broadband_type) {

		String sql = "";
		if (groupid.contains(",")) {
			sql = "SELECT period, CASE probetype WHEN 'Android' THEN '安卓' WHEN 'PC' THEN '桌面' WHEN 'iOS' THEN '苹果'  END usertype, 'H5视频' kpitypename,'缓冲时长占比' fieldname, "
					+ " CONCAT(round(sum(success_rate*video_test_times)/sum(video_test_times),2)*100,'%') successratevalue ,   CONCAT(ROUND(sum(avg_buffer_proportion*video_test_times)/sum(video_test_times),2)*100,'%') avgvalue ,CONCAT(ROUND(sum(top75_avg_buffer_proportion*video_test_times)/sum(video_test_times),2)*100,'%') percent75,CONCAT(ROUND(sum(top85_avg_buffer_proportion*video_test_times)/sum(video_test_times),2)*100,'%') percent85,CONCAT(ROUND(sum(top95_avg_buffer_proportion*video_test_times)/sum(video_test_times),2)*100,'%') percent95 "
					+ "  FROM servicequality_period_video  where `month`='" + month + "' and groupid in(" + groupid + ") and broadband_type='" + broadband_type + "'   group by period,probetype";
		} else {
			sql = "SELECT period, CASE probetype WHEN 'Android' THEN '安卓' WHEN 'PC' THEN '桌面' WHEN 'iOS' THEN '苹果'  END usertype, 'H5视频' kpitypename,'缓冲时长占比' fieldname, "
					+ " CONCAT(round(success_rate,2)*100,'%') successratevalue ,   CONCAT(ROUND(avg_buffer_proportion,2)*100,'%') avgvalue ,CONCAT(ROUND(top75_avg_buffer_proportion,2)*100,'%') percent75,CONCAT(ROUND(top85_avg_buffer_proportion,2)*100,'%') percent85,CONCAT(ROUND(top95_avg_buffer_proportion,2)*100,'%') percent95 "
					+ "  FROM servicequality_period_video  where `month`='" + month + "' and groupid='" + groupid + "' and broadband_type='" + broadband_type + "'    group by period,	probetype";
		}

		List<Map<String, Object>> avgdelayList = getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return avgdelayList;

	}

}
