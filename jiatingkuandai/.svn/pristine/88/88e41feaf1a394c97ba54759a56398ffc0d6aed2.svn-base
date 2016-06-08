package com.qualitymap.dao.impl;

import java.util.List;
import java.util.Map;

import org.hibernate.SQLQuery;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.transform.Transformers;

import com.qualitymap.dao.OverviewKpiDao;
import com.qualitymap.vo.OverviewKpi;

/**
 * 概览信息统计
 * @author：kxc
 * @date：Apr 12, 2016
 */
public class OverviewKpiDaoImpl implements OverviewKpiDao{

	private SessionFactory sessionFactory;

	public void setSessionFactory(SessionFactory sessionFactory) {
		this.sessionFactory = sessionFactory;
	}

	public Session getSession() {
		return sessionFactory.getCurrentSession();
	}
	@Override
	public void saveType(OverviewKpi kpi) {
		// TODO Auto-generated method stub
		getSession().save(kpi);
	}

	@Override
	public void deletType(OverviewKpi kpi) {
		// TODO Auto-generated method stub
		getSession().delete(kpi);
	}

	@Override
	public List<OverviewKpi> find() {
		// TODO Auto-generated method stub
		List<OverviewKpi> kpiList = getSession().createQuery("from OverviewKpi").list();
		return kpiList;
	}

	@Override
	public void updateType(OverviewKpi kpi) {
		// TODO Auto-generated method stub
		getSession().update(kpi);
	}

	/**
	 * 获取累计用户
	 */
	@Override
	public String getAccumulativnum(String month, String groupid) {
		String sql = "select ifnull(sum(accumulativ_num),0) accumulativ_num from overview_kpi where  month= '"+month+"' and groupid in ("+groupid+")";
		SQLQuery query = this.getSession().createSQLQuery(sql);    
	    String strUsernum = query.uniqueResult().toString(); 
		return strUsernum;
	}

	/**
	 * 获取参测省份数
	 */
	@Override
	public String getProvincenum(String month, String groupid) {
		String sql = "select count(DISTINCT groupid) test_province_num from overview_kpi where  month= '"+month+"' and groupid in ("+groupid+")";
		SQLQuery query = this.getSession().createSQLQuery(sql);    
	    String test_province_num = query.uniqueResult().toString(); 
		return test_province_num;
	}

	/**
	 * 获取本月新增用户数
	 */
	@Override
	public String getOrgnum(String month, String groupid) {
		String sql = "select IFNULL(sum(new_user_num),0) org_num from overview_kpi where  month= '"+month+"' and groupid in ("+groupid+")";
		SQLQuery query = this.getSession().createSQLQuery(sql);    
	    String org_num = query.uniqueResult().toString(); 
		return org_num;
	}
	
	/**
	 * 获取渠道数
	 */
	@Override
	public String getChannelnum(String month, String groupid) {
		String sql = "select IFNULL(sum(distribution_channel_num),0) distribution_channel_num from overview_kpi where  month= '"+month+"' and groupid in ("+groupid+")";
		SQLQuery query = this.getSession().createSQLQuery(sql);    
	    String distribution_channel_num = query.uniqueResult().toString(); 
		return distribution_channel_num;
	}

	/**
	 * 获取终端总数及增长比
	 */
	@Override
	public List<Map<String, Object>> getTerminalNum(String premonth,String thismonth, String groupid) {
		String sql = "select ifnull(sum(terminal_num),0) terminal_num,month from overview_kpi where  month in ("+premonth+","+thismonth+") and groupid in ("+groupid+") group by month order by month";
		SQLQuery query = this.getSession().createSQLQuery(sql);    
		List<Map<String, Object>> terminalList = query.setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list(); 
		return terminalList;
	}

	/**
	 * 获取累计注册用户数及增长比
	 */
	@Override
	public List<Map<String, Object>> getRegusernameNum(String premonth,String thismonth, String groupid) {
		String sql = "select ifnull(sum(regusername_num),0) regusername_num,month from overview_kpi where  month in ("+premonth+","+thismonth+") and groupid in ("+groupid+") group by month order by month";
		SQLQuery query = this.getSession().createSQLQuery(sql);    
		List<Map<String, Object>> regusernamelList = query.setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list(); 
		return regusernamelList;
	}

	/**
	 * 获取累计使用用户数及增长比
	 */
	@Override
	public List<Map<String, Object>> getCustomersNum(String premonth,String thismonth, String groupid) {
		String sql = "select sum(customers_num) customers_num ,month from overview_kpi where  month in ("+premonth+","+thismonth+") and groupid in ("+groupid+") group by month order by month";
		SQLQuery query = this.getSession().createSQLQuery(sql);    
		List<Map<String, Object>> customersNumList = query.setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list(); 
		return customersNumList;
	}

	/**
	 * 获取上月启动数及增长比
	 */
	@Override
	public List<Map<String, Object>> getNewlyIncreaseNum(String premonth,String thismonth, String groupid) {
		String sql = "select sum(newly_increase_num) newly_increase_num,month from overview_kpi where  month in ("+premonth+","+thismonth+") and groupid in ("+groupid+") group by month order by month";
		SQLQuery query = this.getSession().createSQLQuery(sql);    
		List<Map<String, Object>> increaseNumList = query.setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list(); 
		return increaseNumList;
	}

	/**
	 * 用户趋势概览
	 */
	@Override
	public List<Map<String, Object>> getUserTendencyData(String groupid) {
		String sql = "select sum(new_user_num) new_user_num,sum(accumulativ_num) accumulativ_num ,month from overview_kpi where   groupid in ("+groupid+") group by month order by month";
		SQLQuery query = this.getSession().createSQLQuery(sql);    
		List<Map<String, Object>> tendencyDataList = query.setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list(); 
		return tendencyDataList;
	}

	/**
	 * 获取测试次数
	 */
	@Override
	public List<Map<String, Object>> getThismonthTesttimes(String premonth,String month, String groupid) {
		String sql = "select sum(newly_increase_num) newly_increase_num,month from overview_kpi where  month in ("+premonth+","+month+") and groupid in ("+groupid+") group by month order by month";
		SQLQuery query = this.getSession().createSQLQuery(sql);    
		List<Map<String, Object>> increaseNumList = query.setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list(); 
		return increaseNumList;
	}

	
}
