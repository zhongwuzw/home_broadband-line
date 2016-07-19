package com.qualitymap.dao.impl;

import java.util.List;
import java.util.Map;

import org.hibernate.SQLQuery;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.transform.Transformers;

import com.qualitymap.dao.OverviewKpiApiDao;
import com.qualitymap.vo.OverviewKpiApi;

/**
 * 概览信息统计
 * @author：kxc
 * @date：Apr 12, 2016
 */
public class OverviewKpiApiDaoImpl implements OverviewKpiApiDao{

	private SessionFactory sessionFactory;

	public void setSessionFactory(SessionFactory sessionFactory) {
		this.sessionFactory = sessionFactory;
	}

	public Session getSession() {
		return sessionFactory.getCurrentSession();
	}
	@Override
	public void saveType(OverviewKpiApi kpi) {
		// TODO Auto-generated method stub
		getSession().save(kpi);
	}

	@Override
	public void deletType(OverviewKpiApi kpi) {
		// TODO Auto-generated method stub
		getSession().delete(kpi);
	}

	@Override
	public List<OverviewKpiApi> find() {
		// TODO Auto-generated method stub
		List<OverviewKpiApi> kpiList = getSession().createQuery("from OverviewKpiApi").list();
		return kpiList;
	}

	@Override
	public void updateType(OverviewKpiApi kpi) {
		// TODO Auto-generated method stub
		getSession().update(kpi);
	}

	/**
	 * 获取累计注册用户数及增长比
	 */
	@Override
	public List<Map<String, Object>> getRegusernameNum(String premonth,String thismonth, String userName) {
		String sql = "select ifnull(total_reg_user_num,'N/A') regusername_num,month from overview_kpi_api where  month in ("+premonth+","+thismonth+") and username = '"+userName+"' group by month order by month";
		SQLQuery query = this.getSession().createSQLQuery(sql);    
		List<Map<String, Object>> regusernamelList = query.setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list(); 
		return regusernamelList;
	}

	/**
	 * 获取累计使用用户数及增长比
	 */
	@Override
	public List<Map<String, Object>> getCustomersNum(String premonth,String thismonth, String userName) {
		String sql = "select IFNULL(accumulativ_user_num,'N/A') customers_num ,month from overview_kpi_api where  month in ("+premonth+","+thismonth+") and username ='"+userName+"' group by month order by month";
		SQLQuery query = this.getSession().createSQLQuery(sql);    
		List<Map<String, Object>> customersNumList = query.setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list(); 
		return customersNumList;
	}

	
}
