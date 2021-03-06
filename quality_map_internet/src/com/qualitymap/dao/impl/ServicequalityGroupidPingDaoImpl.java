package com.qualitymap.dao.impl;

import java.util.List;
import java.util.Map;

import org.hibernate.SQLQuery;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.transform.Transformers;

import com.qualitymap.dao.ServicequalityGroupidPingDao;

/**
 * 
 * @author kongxiangchun
 * 
 */
public class ServicequalityGroupidPingDaoImpl implements ServicequalityGroupidPingDao {

	private SessionFactory sessionFactory;

	
	
	public void setSessionFactory(SessionFactory sessionFactory) {
		this.sessionFactory = sessionFactory;
	}

	public Session getSession() {
		return sessionFactory.getCurrentSession();
	}

	
	/**
	 * 获取本省的结果数据在全国的排名
	 */
	@Override
	public List<Map<String, Object>> getBestResult(String month,String broadType) {
		String sql = "select min(abs(ping_avg_delay)) best_delay,min(abs(ping_loss_rate)) best_loss_rate  from servicequality_groupid_ping  where month= '"+month+"' and broadband_type = '"+broadType+"' ";
		SQLQuery query = this.getSession().createSQLQuery(sql);  
		List<Map<String, Object>> queryList =query.setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return queryList;
	}
	
	/**
	 * 获取本省的结果数据在全国的排名
	 */
	@Override
	public String getDelayResultOrder(String month, String groupid,String broadType) {
		String sql = "select count(id) avg_delay_order from servicequality_groupid_ping where month= '"+month+"' and broadband_type = '"+broadType+"' and abs(ping_avg_delay) <= (select abs(ping_avg_delay) from servicequality_groupid_ping where  month= '"+month+"' and groupid = '"+groupid+"' and broadband_type = '"+broadType+"' )";
		SQLQuery query = this.getSession().createSQLQuery(sql);  
		String avg_delay_order = query.uniqueResult().toString(); 
		return avg_delay_order;
	}
	
	@Override
	public String getLossRateResultOrder(String month, String groupid,String broadType) {
		String sql = "select count(id) loss_rate_order from servicequality_groupid_ping  where month= '"+month+"' and broadband_type = '"+broadType+"' and abs(ping_loss_rate) <= (select abs(ping_loss_rate)  from servicequality_groupid_ping where  month= '"+month+"' and groupid = '"+groupid+"' and broadband_type = '"+broadType+"' )";
		SQLQuery query = this.getSession().createSQLQuery(sql);  
		String loss_rate_order = query.uniqueResult().toString(); 
		return loss_rate_order;
	}
	
	/**
	 * 获取本省的结果数据
	 */
	@Override
	public List<Map<String, Object>> getProvinceResult(String month, String groupid,String broadType) {
		String sql = "select ping_avg_delay avg_delay_data,ping_loss_rate rate_data from servicequality_groupid_ping where  month= '"+month+"' and groupid = '"+groupid+"' and broadband_type = '"+broadType+"' ";
		SQLQuery query = this.getSession().createSQLQuery(sql);  

		List<Map<String, Object>> queryList =query.setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return queryList;
	}
	
	
	/**
	 * 获取平均时延趋势
	 */
	@Override
	public List<Map<String, Object>> getAvgDelayData(String groupid) {
		String sql = " SELECT sum(ping_avg_delay*ping_test_times)/sum(ping_test_times) avg_delay ,month FROM servicequality_groupid_ping WHERE `groupid` in ("+groupid+")  GROUP BY month ";

		SQLQuery sqlQuery = this.getSession().createSQLQuery(sql);
		
		List<Map<String, Object>> queryList =sqlQuery.setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return queryList;
	}
	
/*	
	select sum(case when month='"+premonth+"' then ifnull(new_ue_num, 0) ELSE 0  end) prenew_ue_num," +
	"sum( case when month='"+thismonth+"' then ifnull(new_ue_num, 0) ELSE 0 end) new_ue_num ,appname FROM overview_app  WHERE `groupid` in ("+groupid+") GROUP BY appname order by new_ue_num desc";
	
	*/
	/**
	 * 获取时延趋势数据
	 * */
	@Override
	public List<Map<String, Object>> getAvgDelayOrder(String thismonth,String premonth,String groupid) {
		String sql = " SELECT ifnull(sum(ping_avg_delay*ping_test_times)/sum(ping_test_times),0) avg_delay ," +
				"(SELECT ifnull(sum(ping_avg_delay * ping_test_times) / sum(ping_test_times),0) preavg FROM servicequality_groupid_ping b WHERE `month` = '"+premonth+"' and b.groupid = a.groupid GROUP BY groupid )  preavg " +
				",groupname FROM servicequality_groupid_ping a WHERE `groupid` in ("+groupid+") and month = '"+thismonth+"' GROUP BY groupid order by avg_delay ";

		List<Map<String, Object>> queryList = this.getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return queryList;
	}
	
	/**
	 * 获取宽带品质监测报告详情
	 */
	@Override
	public List<Map<String, Object>> getPingReportItem(String groupid, String month, String broadband_type) {
		String sql = "select * from servicequality_groupid_ping where groupid='"+groupid+"' and month = '"+month+"' and broadband_type= '"+broadband_type+"' ";

		List<Map<String, Object>> queryList = this.getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return queryList;
	}

	@Override
	public List<Map<String, Object>> getCityList(String month, String groupid) {
		// TODO Auto-generated method stub
		return null;
	}
	
	
	/**
	 * 根据groupid获取有效样本
	 */
	@Override
	public String getValidSampleNum(String month, String groupid,String broadType) {
		String sql = "select IFNULL(sum(ping_test_times),0) ping_test_times from servicequality_groupid_ping where  month= '"+month+"' and groupid = '"+groupid+"' and broadband_type = '"+broadType+"' ";
		SQLQuery query = this.getSession().createSQLQuery(sql);    
	    String ping_test_times = query.uniqueResult().toString(); 
		return ping_test_times;
	}
	/**
	 * 获取上下月的平均时延
	 */
	@Override
	public List<Map<String, Object>> getAvgDelay(String yearMonth, String lastMonth, String groupid) {
		String sql = " SELECT sum(ping_test_times * ping_avg_delay) /SUM(testtimes) avg_delay,`month` " + " FROM servicequality_groupid_ping WHERE" + " `month` in (" + yearMonth + "," + lastMonth
				+ ") AND groupid IN (" + groupid + ") GROUP BY `month` order by month";

		List<Map<String, Object>> queryList = this.getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return queryList;
	}

	/**
	 * 获取上下月的时延达标率
	 */
	@Override
	public List<Map<String, Object>> getDelayStandardRate(String yearMonth, String lastMonth, String groupid) {
		String sql = " SELECT sum(ping_test_times * delay_standard_rate) /SUM(ping_test_times) delay_standard_rate,`month` " + " FROM servicequality_groupid_ping WHERE" + " `month` in (" + yearMonth + ","
				+ lastMonth + ") AND groupid IN (" + groupid + ") GROUP BY `month` order by month ";

		List<Map<String, Object>> queryList = this.getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return queryList;
	}
	/**
	 * 获取上下月平均ping时延(MS)
	 */
	@Override
	public List<Map<String, Object>> getAvgPingDelay(String yearMonth, String lastMonth, String groupid) {
		String sql = " SELECT sum(ping_test_times * ping_avg_delay) /SUM(ping_test_times) avg_ping_delay,`month` " + " FROM servicequality_groupid_ping WHERE" + " `month` in (" + yearMonth + "," + lastMonth
				+ ") AND groupid IN (" + groupid + ") GROUP BY `month` order by month";

		List<Map<String, Object>> queryList = this.getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return queryList;
	}
	
	/**
	 * 获取上下月95%用户ping时延
	 */
	@Override
	public List<Map<String, Object>> getTop90PingDelay(String yearMonth, String lastMonth, String groupid) {
		String sql = " SELECT sum(ping_test_times * top95_ping_delay) /SUM(ping_test_times) top90_ping_delay,`month` " + " FROM servicequality_groupid_ping WHERE" + " `month` in (" + yearMonth + "," + lastMonth
				+ ") AND groupid IN (" + groupid + ") GROUP BY `month` order by month";

		List<Map<String, Object>> queryList = this.getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return queryList;
	}
	
	/**
	 * 获取上下月ping丢包率
	 */
	@Override
	public List<Map<String, Object>> getPingLossRate(String yearMonth, String lastMonth, String groupid) {
		String sql = " SELECT sum(ping_test_times * ping_loss_rate) /SUM(ping_test_times) ping_loss_rate,`month` " + " FROM servicequality_groupid_ping WHERE" + " `month` in (" + yearMonth + "," + lastMonth
				+ ") AND groupid IN (" + groupid + ") GROUP BY `month` order by month";

		List<Map<String, Object>> queryList = this.getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return queryList;
	}
	
	/**
	 * 获取上下月95%用户ping丢包率
	 */
	@Override
	public List<Map<String, Object>> getTop90PingLossRate(String yearMonth, String lastMonth, String groupid) {
		String sql = " SELECT sum(ping_test_times * top95_ping_loss_rate) /SUM(ping_test_times) top90_ping_loss_rate,`month` " + " FROM servicequality_groupid_ping WHERE" + " `month` in (" + yearMonth + "," + lastMonth
				+ ") AND groupid IN (" + groupid + ") GROUP BY `month` order by month";

		List<Map<String, Object>> queryList = this.getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return queryList;
	}

	/**
	 * 获取质量分析 即 上下月中数据的增减情况
	 */
	@Override
	public List<Map<String, Object>> getPingServiceQualityCompare(String groupid, String month,String permonth, String broadband_type) {
		String sql = "select ifnull(round((ping_avg_delay - (select ping_avg_delay from servicequality_groupid_ping where month='"+permonth+"' and groupid='"+groupid+"' and broadband_type='"+broadband_type+"')),2),'N/A') ping_avg_delay,IFNULL(round((ping_loss_rate - (select ping_loss_rate from servicequality_groupid_ping where month='"+permonth+"' and groupid='"+groupid+"' and broadband_type='"+broadband_type+"')),2),'N/A') ping_loss_rate from  servicequality_groupid_ping where  month='"+month+"' and groupid='"+groupid+"' and broadband_type='"+broadband_type+"'";
		List<Map<String, Object>> queryList = this.getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return queryList;
	}
}
