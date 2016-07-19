package com.qualitymap.dao.impl;

import java.util.List;
import java.util.Map;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.transform.Transformers;

import com.qualitymap.dao.OverviewServicequalityDao;
import com.qualitymap.vo.OverviewServicequality;

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

	@Override
	public void savequality(OverviewServicequality serquality) {
		// TODO Auto-generated method stub
		getSession().save(serquality);
	}

	@Override
	public void deletequality(OverviewServicequality serquality) {
		// TODO Auto-generated method stub
		getSession().delete(serquality);
	}

	@Override
	public List<OverviewServicequality> findquality() {
		// TODO Auto-generated method stub
		List<OverviewServicequality> qualityList = getSession().createQuery("from OverviewServicequality").list();
		return qualityList;
	}

	@Override
	public void updatequality(OverviewServicequality serquality) {
		// TODO Auto-generated method stub
		getSession().update(serquality);
	}

	/**
	 * 获取上下月的平均时延
	 */
	@Override
	public List<Map<String, Object>> getAvgDelay(String yearMonth, String lastMonth, String groupid) {
		String sql = " SELECT sum(testtimes * avg_delay) /SUM(testtimes) avg_delay,`month` " + " FROM overview_servicequality WHERE" + " `month` in (" + yearMonth + "," + lastMonth
				+ ") AND groupid IN (" + groupid + ") GROUP BY `month` order by month";

		List<Map<String, Object>> queryList = this.getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return queryList;
	}

	/**
	 * 获取上下月的时延达标率
	 */
	@Override
	public List<Map<String, Object>> getDelayStandardRate(String yearMonth, String lastMonth, String groupid) {
		String sql = " SELECT sum(testtimes * delay_standard_rate) /SUM(testtimes) delay_standard_rate,`month` " + " FROM overview_servicequality WHERE" + " `month` in (" + yearMonth + ","
				+ lastMonth + ") AND groupid IN (" + groupid + ") GROUP BY `month` order by month ";

		List<Map<String, Object>> queryList = this.getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return queryList;
	}

	/**
	 * 获取上下月平均页面元素打开成功率
	 */
	@Override
	public List<Map<String, Object>> getAvgPageSuccessRate(String yearMonth, String lastMonth, String groupid) {
		String sql = " SELECT sum(testtimes * avg_page_success_rate) /SUM(testtimes) avg_page_success_rate,`month` " + " FROM overview_servicequality WHERE" + " `month` in (" + yearMonth + ","
				+ lastMonth + ") AND groupid IN (" + groupid + ") GROUP BY `month` order by month ";

		List<Map<String, Object>> queryList = this.getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return queryList;
	}

	/**
	 * 获取上下月页面元素打开达标率
	 */
	@Override
	public List<Map<String, Object>> getPageStandardRate(String yearMonth, String lastMonth, String groupid) {
		String sql = " SELECT sum(testtimes * page_standard_rate) /SUM(testtimes) page_standard_rate,`month` " + " FROM overview_servicequality WHERE" + " `month` in (" + yearMonth + "," + lastMonth
				+ ") AND groupid IN (" + groupid + ") GROUP BY `month` order by month";

		List<Map<String, Object>> queryList = this.getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return queryList;
	}

	/**
	 * 获取上下月90%用户页面时延
	 */
	@Override
	public List<Map<String, Object>> getTop90PageDelay(String yearMonth, String lastMonth, String groupid) {
		String sql = " SELECT sum(testtimes * top90_page_delay) /SUM(testtimes) top90_page_delay,`month` " + " FROM overview_servicequality WHERE" + " `month` in (" + yearMonth + ","
				+ lastMonth + ") AND groupid IN (" + groupid + ") GROUP BY `month` order by month ";

		List<Map<String, Object>> queryList = this.getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return queryList;
	}
	
	/**
	 * 获取上下月90%用户页面元素打开成功率
	 */
	@Override
	public List<Map<String, Object>> getTop90PageSuccessRate(String yearMonth, String lastMonth, String groupid) {
		String sql = " SELECT sum(testtimes * top90_page_success_rate) /SUM(testtimes) top90_page_success_rate,`month` " + " FROM overview_servicequality WHERE" + " `month` in (" + yearMonth + "," + lastMonth
				+ ") AND groupid IN (" + groupid + ") GROUP BY `month` order by month";

		List<Map<String, Object>> queryList = this.getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return queryList;
	}
	
	/**
	 * 获取上下月平均ping时延(MS)
	 */
	@Override
	public List<Map<String, Object>> getAvgPingDelay(String yearMonth, String lastMonth, String groupid) {
		String sql = " SELECT sum(testtimes * avg_ping_delay) /SUM(testtimes) avg_ping_delay,`month` " + " FROM overview_servicequality WHERE" + " `month` in (" + yearMonth + "," + lastMonth
				+ ") AND groupid IN (" + groupid + ") GROUP BY `month` order by month";

		List<Map<String, Object>> queryList = this.getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return queryList;
	}
	
	/**
	 * 获取上下月90%用户ping时延
	 */
	@Override
	public List<Map<String, Object>> getTop90PingDelay(String yearMonth, String lastMonth, String groupid) {
		String sql = " SELECT sum(testtimes * top90_ping_delay) /SUM(testtimes) top90_ping_delay,`month` " + " FROM overview_servicequality WHERE" + " `month` in (" + yearMonth + "," + lastMonth
				+ ") AND groupid IN (" + groupid + ") GROUP BY `month` order by month";

		List<Map<String, Object>> queryList = this.getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return queryList;
	}
	
	/**
	 * 获取上下月ping丢包率
	 */
	@Override
	public List<Map<String, Object>> getPingLossRate(String yearMonth, String lastMonth, String groupid) {
		String sql = " SELECT sum(testtimes * ping_loss_rate) /SUM(testtimes) ping_loss_rate,`month` " + " FROM overview_servicequality WHERE" + " `month` in (" + yearMonth + "," + lastMonth
				+ ") AND groupid IN (" + groupid + ") GROUP BY `month` order by month";

		List<Map<String, Object>> queryList = this.getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return queryList;
	}
	
	/**
	 * 获取上下月90%用户ping丢包率
	 */
	@Override
	public List<Map<String, Object>> getTop90PingLossRate(String yearMonth, String lastMonth, String groupid) {
		String sql = " SELECT sum(testtimes * top90_ping_loss_rate) /SUM(testtimes) top90_ping_loss_rate,`month` " + " FROM overview_servicequality WHERE" + " `month` in (" + yearMonth + "," + lastMonth
				+ ") AND groupid IN (" + groupid + ") GROUP BY `month` order by month";

		List<Map<String, Object>> queryList = this.getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return queryList;
	}
}
