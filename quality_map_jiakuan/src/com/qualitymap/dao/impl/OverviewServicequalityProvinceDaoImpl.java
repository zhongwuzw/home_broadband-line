package com.qualitymap.dao.impl;

import java.util.List;
import java.util.Map;

import org.hibernate.Hibernate;
import org.hibernate.SQLQuery;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.transform.Transformers;

import com.qualitymap.dao.OverviewServicequalityProvinceDao;
import com.qualitymap.service.OverviewServicequalityProvinceService;
import com.qualitymap.vo.OverviewServicequalityProvince;

/**
 * 业务质量数据统计
 * @author：kxc
 * @date：Apr 11, 2016
 */
public class OverviewServicequalityProvinceDaoImpl implements OverviewServicequalityProvinceDao {

	private SessionFactory sessionFactory;

	
	@Override
	public void saveQualityProvince(OverviewServicequalityProvince serqualityprovince) {
		// TODO Auto-generated method stub
		getSession().save(serqualityprovince);
	}

	@Override
	public void deleteqQualityProvinc(OverviewServicequalityProvince serqualityprovince) {
		// TODO Auto-generated method stub
		getSession().delete(serqualityprovince);
	}

	@Override
	public List<OverviewServicequalityProvince> findQualityProvinc() {
		// TODO Auto-generated method stub
		List<OverviewServicequalityProvince> qualityProvinceList = getSession().createQuery("from OverviewServicequalityProvince").list();
		return qualityProvinceList;
	}

	@Override
	public void updateQualityProvinc(OverviewServicequalityProvince serqualityprovince) {
		// TODO Auto-generated method stub
		getSession().update(serqualityprovince);
		
	}

	
	public void setSessionFactory(SessionFactory sessionFactory) {
		this.sessionFactory = sessionFactory;
	}

	public Session getSession() {
		return sessionFactory.getCurrentSession();
	}

	/**
	 * 获取平均时延趋势
	 */
	@Override
	public List<Map<String, Object>> getAvgDelayData(String groupid) {
		String sql = " SELECT sum(avg_delay*testtimes)/sum(testtimes) avg_delay ,month FROM overview_servicequality_province WHERE `groupid` in ("+groupid+")  GROUP BY month,groupid ";

		SQLQuery sqlQuery = this.getSession().createSQLQuery(sql);
		
		  //sqlQuery.addScalar("avg_delay", Hibernate.);
		/*sqlQuery.addScalar("avg_delay");
		sqlQuery.addScalar("month");*/
		List<Map<String, Object>> queryList =sqlQuery.setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return queryList;
	}

	@Override
	public List<Map<String, Object>> getAvgDelayOrder(String thismonth,String premonth,String groupid) {
		String sql = " SELECT ifnull(sum(avg_delay*testtimes)/sum(testtimes),0) avg_delay ," +
				"ifnull((SELECT ifnull(sum(avg_delay * testtimes) / sum(testtimes),0) preavg FROM overview_servicequality_province b WHERE `month` = '"+premonth+"' and b.groupid = a.groupid GROUP BY groupid ),0) preavg " +
				",province FROM overview_servicequality_province a WHERE `groupid` in ("+groupid+") and month = '"+thismonth+"' GROUP BY groupid order by avg_delay ";
		//in("+premonth+","+thismonth+") order by avg_delay ";
		
		List<Map<String, Object>> queryList = this.getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return queryList;
	}

	@Override
	public List<Map<String, Object>> getPageSuccessData(String groupid) {
		String sql = " SELECT sum(page_success_rate*testtimes)/sum(testtimes) page_success_rate ,month FROM overview_servicequality_province WHERE `groupid` in ("+groupid+")  GROUP BY month,groupid ";

		List<Map<String, Object>> queryList = this.getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return queryList;
	}

	@Override
	public List<Map<String, Object>> getPageSuccessOrder(String thismonth, String premonth, String groupid) {
		String sql = " SELECT ifnull(sum(page_success_rate*testtimes)/sum(testtimes),0) page_success_rate ," +
				"ifnull((SELECT ifnull(sum(page_success_rate * testtimes) / sum(testtimes),0) pre_page_success_rate FROM overview_servicequality_province b WHERE `month` = '"+premonth+"' and b.groupid = a.groupid GROUP BY groupid ),0) pre_page_success_rate " +
				",province FROM overview_servicequality_province a WHERE `groupid` in ("+groupid+") and month = '"+thismonth+"' GROUP BY groupid order by page_success_rate desc ";
		
		List<Map<String, Object>> queryList = this.getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return queryList;
	}
}
