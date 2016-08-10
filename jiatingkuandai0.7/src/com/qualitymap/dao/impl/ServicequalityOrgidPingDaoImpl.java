package com.qualitymap.dao.impl;

import java.util.List;
import java.util.Map;

import org.hibernate.SQLQuery;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.transform.Transformers;

import com.qualitymap.dao.ServicequalityOrgidPingDao;

/**
 * 
 * @author kongxiangchun
 * 
 */
public class ServicequalityOrgidPingDaoImpl implements ServicequalityOrgidPingDao {

	private SessionFactory sessionFactory;

	
	
	public void setSessionFactory(SessionFactory sessionFactory) {
		this.sessionFactory = sessionFactory;
	}

	public Session getSession() {
		return sessionFactory.getCurrentSession();
	}

	/**
	 * 获取宽带品质监测报告详情
	 */
	@Override
	public List<Map<String, Object>> getPingReportItem(String groupid, String month, String broadband_type) {
		String sql = "select * from servicequality_orgid_ping where groupid='"+groupid+"' and month = '"+month+"' and broadband_type= '"+broadband_type+"' ";

		List<Map<String, Object>> queryList = this.getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return queryList;
	}
	/**
	 * 获取城市list
	 */
	@Override
	public List<Map<String, Object>> getCityList(String month,String groupid,String broadType) {
		String sql = " SELECT DISTINCT orgname  FROM servicequality_orgid_ping WHERE `month` ='" + month + "'  and groupid = '"+groupid+"' and broadband_type = '"+broadType+"' ";

		List<Map<String, Object>> queryList = this.getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return queryList;
	}
	

	/**
	 * 根据groupid获取账户数(报告中的地区字段)
	 */
	@Override
	public String getOrgnumByGroupId(String month, String groupid,String broadType) {
		String sql = "select count(DISTINCT orgname)  org_num from servicequality_orgid_ping where  month= '"+month+"' and groupid = '"+groupid+"' and broadband_type = '"+broadType+"' ";
		SQLQuery query = this.getSession().createSQLQuery(sql);    
	    String org_num = query.uniqueResult().toString(); 
		return org_num;
	}

}