package com.qualitymap.dao.impl;

import java.util.List;
import java.util.Map;

import org.hibernate.SQLQuery;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.transform.Transformers;

import com.qualitymap.dao.TerminalOsDao;
import com.qualitymap.vo.TerminalOs;

public class TerminalOsDaoImpl implements TerminalOsDao {

	private SessionFactory sessionFactory;

	@Override
	public void saveOs(TerminalOs terminalOs) {
		// TODO Auto-generated method stub
		getSession().save(terminalOs);
	}

	@Override
	public void deleteOs(TerminalOs terminalOs) {
		// TODO Auto-generated method stub
		getSession().delete(terminalOs);
	}

	@Override
	public List<TerminalOs> findOs() {
		// TODO Auto-generated method stub
		List<TerminalOs> osList = getSession().createQuery("from TerminalOs").list();
		return osList;
	}

	@Override
	public void updateOs(TerminalOs terminalOs) {
		// TODO Auto-generated method stub
		getSession().update(terminalOs);
	}

	public void setSessionFactory(SessionFactory sessionFactory) {
		this.sessionFactory = sessionFactory;
	}

	public Session getSession() {
		return sessionFactory.getCurrentSession();
	}

	@Override
	public List<Map<String, Object>> getPlatformPercent(String month,String groupid) {
		String sql = " SELECT sum(testtimes) value, platform lable FROM terminal_os WHERE `month` ='" + month + "' and groupid in ("+groupid+")  GROUP BY lable ";

		List<Map<String, Object>> queryList = this.getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return queryList;
	}

	@Override
	public List<Map<String, Object>> getPlatData(String month,String groupid) {
		String sql = " SELECT sum(new_user_num) thisMonth,sum(accumulativ_num) accumulat ,platform type FROM terminal_os WHERE `month` ='" + month + "' and groupid in ("+groupid+")  GROUP BY type ";

		List<Map<String, Object>> queryList = this.getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return queryList;
	}

	/**
	 * 获取平台分布数据
	 */
	@Override
	public List<Map<String, Object>> getPlatformDistribution(String month,String groupid) {
		String sql = " SELECT ifnull(sum(testtimes),0) testtimes,platform type FROM terminal_os WHERE `month` ='" + month + "'  and groupid in ("+groupid+")  GROUP BY platform ";

		List<Map<String, Object>> queryList = this.getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return queryList;
	}

	/**
	 * 获取平台数据详细信息
	 */
	@Override
	public List<Map<String, Object>> getPlatformDetailData(String month,String groupid) {
		String sql = " SELECT ifnull(sum(testtimes),0) testtimes,platform,ifnull(sum(new_user_num),0) new_user_num,ifnull(sum(accumulativ_num),0) accumulativ_num, ifnull(sum(new_terminal_num),0) new_terminal_num,ifnull(sum(accumulativ_terminal_num),0) accumulativ_terminal_num,round(sum(new_terminal_num)/(select sum(new_terminal_num) from terminal_os where groupid  in ("+groupid+") and month='"+month+"'),4)  newterminalpercent ,month FROM terminal_os WHERE  groupid in ("+groupid+") and month='"+month+"'  group by platform ";

		List<Map<String, Object>> queryList = this.getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return queryList;
	}
	
	/**
	 * 获取平台list
	 */
	@Override
	public List<Map<String, Object>> getPlatformList(String month,String groupid) {
		String sql = " SELECT DISTINCT platform type FROM terminal_os WHERE `month` ='" + month + "'  and groupid = '"+groupid+"' ";

		List<Map<String, Object>> queryList = this.getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return queryList;
	}
	

}