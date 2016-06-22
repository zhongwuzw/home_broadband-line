package com.qualitymap.dao.impl;

import java.util.List;
import java.util.Map;

import org.hibernate.SQLQuery;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.transform.Transformers;

import com.qualitymap.dao.ServicequalityGroupidWebbrowsingDao;

/**
 * 
 * @author kongxiangchun
 * 
 */
public class ServicequalityGroupidWebbrowsingDaoImpl implements ServicequalityGroupidWebbrowsingDao {

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
		String sql = "select min(abs(page_avg_delay)) best_delay,max(abs(page_success_rate)) best_rate  from servicequality_groupid_webbrowsing  where month= '"+month+"' and broadband_type = '"+broadType+"' and page_avg_delay>0 and page_success_rate>0 ";
		SQLQuery query = this.getSession().createSQLQuery(sql);  
		List<Map<String, Object>> queryList =query.setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return queryList;
	}
	
	/**
	 * 获取本省的结果数据在全国的排名
	 */
	@Override
	public String getDelayResultOrder(String month, String groupid,String broadType) {
		String sql = "select count(id) avg_delay_order from servicequality_groupid_webbrowsing where month= '"+month+"' and broadband_type = '"+broadType+"' and abs(page_avg_delay) <= (select abs(page_avg_delay) from servicequality_groupid_webbrowsing where  month= '"+month+"' and groupid = '"+groupid+"' and broadband_type = '"+broadType+"' )";
		SQLQuery query = this.getSession().createSQLQuery(sql);  
		String delay_order = query.uniqueResult().toString(); 
		return delay_order;
	}
	
	@Override
	public String getSuccessRateResultOrder(String month, String groupid,String broadType) {
		String sql = "select count(id) rate_order from servicequality_groupid_webbrowsing  where month= '"+month+"' and broadband_type = '"+broadType+"' and abs(page_success_rate) >= (select abs(page_success_rate) from servicequality_groupid_webbrowsing where  month= '"+month+"' and groupid = '"+groupid+"' and broadband_type = '"+broadType+"' )";
		SQLQuery query = this.getSession().createSQLQuery(sql);  
		String rate_order = query.uniqueResult().toString(); 
		return rate_order;
	}
	
	/**
	 * 获取本省的结果数据
	 */
	@Override
	public List<Map<String, Object>> getProvinceResult(String month, String groupid,String broadType) {
		String sql = "select page_avg_delay delay_data,page_success_rate rate_data from servicequality_groupid_webbrowsing where  month= '"+month+"' and groupid = '"+groupid+"' and broadband_type = '"+broadType+"' ";
		SQLQuery query = this.getSession().createSQLQuery(sql);  

		List<Map<String, Object>> queryList =query.setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return queryList;
	}
	
	
	
	/**
	 * 获取质量分析 即 上下月中数据的增减情况
	 */
	@Override
	public List<Map<String, Object>> getWebbrowsingServiceQualityCompare(String groupid, String month,String permonth, String broadband_type) {
		String sql = "select  IFNULL(round((page_avg_delay - (select page_avg_delay from servicequality_groupid_webbrowsing where month='"+permonth+"' and groupid='"+groupid+"' and broadband_type='"+broadband_type+"')),2),'N/A') page_avg_delay, IFNULL(round((page_success_rate - (select page_success_rate from servicequality_groupid_webbrowsing where month='"+permonth+"' and groupid='"+groupid+"' and broadband_type='"+broadband_type+"')),2),'N/A') page_success_rate from  servicequality_groupid_webbrowsing where  month='"+month+"' and groupid='"+groupid+"' and broadband_type='"+broadband_type+"'";
		List<Map<String, Object>> queryList = this.getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return queryList;
	}

	/**
	 * 获取上下月的页面浏览成功率
	 */
	@Override
	public List<Map<String, Object>> getPageBrowseSuccess(String yearMonth, String lastMonth, String groupid) {
		
		String sql = "SELECT a.probetype, round(thisdata,0) thisdata, round(lastdata,0) lastdata FROM ( SELECT DISTINCT probetype FROM servicequality_groupid_webbrowsing WHERE `month` in (" + yearMonth + "," + lastMonth
				+ ") AND groupid IN (" + groupid + ") ) a " +
						"LEFT JOIN ( SELECT sum( page_test_times * success_rate ) / SUM(page_test_times) thisdata, probetype, 	MONTH FROM servicequality_groupid_webbrowsing " +
						"where month='"+yearMonth+"' AND groupid IN (" + groupid + ") GROUP BY `month`, probetype ) b ON a.probetype = b.probetype " +
						"LEFT JOIN ( SELECT sum( 	page_test_times * success_rate ) / SUM(page_test_times) lastdata, probetype, MONTH FROM servicequality_groupid_webbrowsing " +
						"where month='"+lastMonth+"' AND groupid IN (" + groupid + ") GROUP BY `month`, probetype ) c ON a.probetype = c.probetype" ;

		List<Map<String, Object>> queryList = this.getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return queryList;
	

	}
	
	/**
	 * 获取上下月的平均时延
	 */
	@Override
	public List<Map<String, Object>> getAvgDelay(String yearMonth, String lastMonth, String groupid) {
		
		String sql = "SELECT a.probetype, round(thisdata,0) thisdata, round(lastdata,0) lastdata FROM ( SELECT DISTINCT probetype FROM servicequality_groupid_webbrowsing WHERE `month` in (" + yearMonth + "," + lastMonth
				+ ") AND groupid IN (" + groupid + ") ) a " +
						"LEFT JOIN ( SELECT sum( page_test_times * page_avg_delay ) / SUM(page_test_times) thisdata, probetype, 	MONTH FROM servicequality_groupid_webbrowsing " +
						"where month='"+yearMonth+"' AND groupid IN (" + groupid + ") GROUP BY `month`, probetype ) b ON a.probetype = b.probetype " +
						"LEFT JOIN ( SELECT sum( 	page_test_times * page_avg_delay ) / SUM(page_test_times) lastdata, probetype, MONTH FROM servicequality_groupid_webbrowsing " +
						"where month='"+lastMonth+"' AND groupid IN (" + groupid + ") GROUP BY `month`, probetype ) c ON a.probetype = c.probetype" ;

		List<Map<String, Object>> queryList = this.getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return queryList;
	

	}
	
	/**
	 * 获取页面浏览访问成功率趋势数据
	 */
	@Override
	public List<Map<String, Object>> getPageBrowseSuccessData(String groupid,String probetype) {
		String sql = " SELECT sum(success_rate*page_test_times)/sum(page_test_times) success_rate ,month FROM servicequality_groupid_webbrowsing WHERE `groupid` in (" + groupid
				+ ") and  probetype='" + probetype + "' GROUP BY month order by month";
		
		List<Map<String, Object>> queryList = this.getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return queryList;
	}

	/**
	 * 获取页面浏览访问成功率的排名
	 */
	@Override
	public List<Map<String, Object>> getPageBrowseSuccessOrder(String thismonth, String premonth, String groupid,String probetype) {
		/*String sql = " SELECT ifnull(sum(page_success_rate*page_test_times)/sum(page_test_times),0) page_success_rate ,"
				+ "(SELECT ifnull(sum(page_success_rate * page_test_times) / sum(page_test_times),0)  FROM servicequality_groupid_webbrowsing b WHERE `month` = '" + premonth
				+ "' and probetype='"+probetype+"' and b.groupid = a.groupid GROUP BY groupid ) pre_page_success_rate " + ",groupname FROM servicequality_groupid_webbrowsing a WHERE `groupid` in (" + groupid + ") and month = '"
				+ thismonth + "' and probetype='"+probetype+"' GROUP BY groupid order by page_success_rate desc ";
		*/
		
	String sql = "SELECT a.groupname, success_rate, pre_success_rate FROM ( SELECT groupname FROM servicequality_groupid_webbrowsing b WHERE" +
			" probetype='"+probetype+"' AND `month` IN ('"+thismonth+"','"+premonth+"') and groupid in ("+groupid+") group by groupid ) a" +
			" LEFT JOIN ( SELECT ifnull( sum( success_rate * page_test_times ) / sum(page_test_times), 0 ) success_rate, groupname, MONTH FROM servicequality_groupid_webbrowsing" +
			" WHERE	MONTH = '"+thismonth+"' AND groupid IN ("+groupid+") and probetype='"+probetype+"' GROUP BY `month`, groupname ) b ON a.groupname = b.groupname " +
			" LEFT JOIN ( SELECT ifnull( sum( success_rate * page_test_times ) / sum(page_test_times), 0 ) pre_success_rate, groupname, MONTH FROM servicequality_groupid_webbrowsing" +
			" WHERE	MONTH = '"+premonth+"' AND groupid IN ("+groupid+") and probetype='"+probetype+"' GROUP BY `month`, groupname ) c ON a.groupname = c.groupname order by success_rate desc" ;
	
		List<Map<String, Object>> queryList = this.getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return queryList;
	}

	/**
	 * 获取页面元素打开成功率趋势数据
	 */
	@Override
	public List<Map<String, Object>> getPageSuccessData(String groupid,String probetype) {
		String sql = " SELECT sum(page_success_rate*page_test_times)/sum(page_test_times) page_success_rate ,month FROM servicequality_groupid_webbrowsing WHERE `groupid` in (" + groupid
				+ ") and  probetype='" + probetype + "' GROUP BY month order by month";
		
		List<Map<String, Object>> queryList = this.getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return queryList;
	}

	/**
	 * 获取页面元素打开成功率的排名
	 */
	@Override
	public List<Map<String, Object>> getPageSuccessOrder(String thismonth, String premonth, String groupid,String probetype) {
		/*String sql = " SELECT ifnull(sum(page_success_rate*page_test_times)/sum(page_test_times),0) page_success_rate ,"
				+ "(SELECT ifnull(sum(page_success_rate * page_test_times) / sum(page_test_times),0)  FROM servicequality_groupid_webbrowsing b WHERE `month` = '" + premonth
				+ "' and probetype='"+probetype+"' and b.groupid = a.groupid GROUP BY groupid ) pre_page_success_rate " + ",groupname FROM servicequality_groupid_webbrowsing a WHERE `groupid` in (" + groupid + ") and month = '"
				+ thismonth + "' and probetype='"+probetype+"' GROUP BY groupid order by page_success_rate desc ";
		*/
		
	String sql = "SELECT a.groupname, page_success_rate, pre_page_success_rate FROM ( SELECT groupname FROM servicequality_groupid_webbrowsing b WHERE" +
			" probetype='"+probetype+"' AND `month` IN ('"+thismonth+"','"+premonth+"') and groupid in ("+groupid+") group by groupid ) a" +
			" LEFT JOIN ( SELECT ifnull( sum( page_success_rate * page_test_times ) / sum(page_test_times), 0 ) page_success_rate, groupname, MONTH FROM servicequality_groupid_webbrowsing" +
			" WHERE	MONTH = '"+thismonth+"' AND groupid IN ("+groupid+") and probetype='"+probetype+"' GROUP BY `month`, groupname ) b ON a.groupname = b.groupname " +
			" LEFT JOIN ( SELECT ifnull( sum( page_success_rate * page_test_times ) / sum(page_test_times), 0 ) pre_page_success_rate, groupname, MONTH FROM servicequality_groupid_webbrowsing" +
			" WHERE	MONTH = '"+premonth+"' AND groupid IN ("+groupid+") and probetype='"+probetype+"' GROUP BY `month`, groupname ) c ON a.groupname = c.groupname order by page_success_rate desc" ;
	
		List<Map<String, Object>> queryList = this.getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return queryList;
	}
	
	/**
	 * 获取上下月平均页面元素打开成功率
	 */
	@Override
	public List<Map<String, Object>> getAvgPageSuccessRate(String yearMonth, String lastMonth, String groupid) {
		String sql = " SELECT sum(page_test_times * page_success_rate) /SUM(page_test_times) avg_page_success_rate,`month` " + " FROM servicequality_groupid_webbrowsing WHERE" + " `month` in (" + yearMonth + ","
				+ lastMonth + ") AND groupid IN (" + groupid + ") GROUP BY `month` order by month ";

		List<Map<String, Object>> queryList = this.getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return queryList;
	}

	/**
	 * 获取上下月页面元素打开达标率
	 */
	@Override
	public List<Map<String, Object>> getPageStandardRate(String yearMonth, String lastMonth, String groupid) {
		/*String sql = " SELECT sum(page_test_times * page_success_rate) /SUM(page_test_times) page_success_rate,`month`,probetype " + " FROM servicequality_groupid_webbrowsing WHERE" + " `month` in (" + yearMonth + "," + lastMonth
				+ ") AND groupid IN (" + groupid + ") GROUP BY `month`,probetype  order by probetype";*/
		
		String sql = "SELECT a.probetype, round(thisdata, 2) thisdata,round(lastdata, 2) lastdata FROM ( SELECT DISTINCT probetype FROM servicequality_groupid_webbrowsing WHERE `month` in (" + yearMonth + "," + lastMonth
				+ ") AND groupid IN (" + groupid + ") ) a " +
						"LEFT JOIN ( SELECT sum( page_test_times * page_success_rate ) / SUM(page_test_times) thisdata, probetype, 	MONTH FROM servicequality_groupid_webbrowsing " +
						"where month='"+yearMonth+"' AND groupid IN (" + groupid + ") GROUP BY `month`, probetype ) b ON a.probetype = b.probetype " +
						"LEFT JOIN ( SELECT sum( 	page_test_times * page_success_rate ) / SUM(page_test_times) lastdata, probetype, MONTH FROM servicequality_groupid_webbrowsing " +
						"where month='"+lastMonth+"' AND groupid IN (" + groupid + ") GROUP BY `month`, probetype ) c ON a.probetype = c.probetype" +
						"";

		List<Map<String, Object>> queryList = this.getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return queryList;
	}

	/**
	 * 获取上下月95%用户页面时延
	 */
	@Override
	public List<Map<String, Object>> getTop90PageDelay(String yearMonth, String lastMonth, String groupid) {
		String sql = " SELECT sum(page_test_times * top95_page_delay) /SUM(page_test_times) top90_page_delay,`month` " + " FROM servicequality_groupid_webbrowsing WHERE" + " `month` in (" + yearMonth + ","
				+ lastMonth + ") AND groupid IN (" + groupid + ") GROUP BY `month` order by month ";

		List<Map<String, Object>> queryList = this.getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return queryList;
	}
	
	/**
	 * 获取上下月90%用户页面元素打开成功率
	 */
	@Override
	public List<Map<String, Object>> getTop90PageSuccessRate(String yearMonth, String lastMonth, String groupid) {
		String sql = " SELECT sum(page_test_times * top95_page_success_rate) /SUM(page_test_times) top90_page_success_rate,`month` " + " FROM servicequality_groupid_webbrowsing WHERE" + " `month` in (" + yearMonth + "," + lastMonth
				+ ") AND groupid IN (" + groupid + ") GROUP BY `month` order by month";

		List<Map<String, Object>> queryList = this.getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return queryList;
	}
	/**
	 * 获取页面显示时延趋势数据
	 */
	@Override
	public List<Map<String, Object>> getPageDelayData(String groupid, String probetype) {
		String sql = " SELECT sum(page_avg_delay*page_test_times)/sum(page_test_times) page_avg_delay ,month FROM servicequality_groupid_webbrowsing WHERE `groupid` in (" + groupid
				+ ") and  probetype='" + probetype + "' GROUP BY month order by month";
		
		List<Map<String, Object>> queryList = this.getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return queryList;
	}
	/**
	 * 获取页面显示时延的排名
	 */
	@Override
	public List<Map<String, Object>> getPageDelayOrder(String thismonth, String premonth, String groupid, String probetype) {
		/*String sql = " SELECT ifnull(sum(page_avg_delay*page_test_times)/sum(page_test_times),0) page_avg_delay ,"
				+ "(SELECT ifnull(sum(page_avg_delay * page_test_times) / sum(page_test_times),0)  FROM servicequality_groupid_webbrowsing b WHERE `month` = '" + premonth
				+ "' and probetype='"+probetype+"' and b.groupid = a.groupid GROUP BY groupid ) pre_page_avg_delay " + ",groupname FROM servicequality_groupid_webbrowsing a WHERE `groupid` in (" + groupid + ") and month = '"
				+ thismonth + "' and probetype='"+probetype+"' GROUP BY groupid order by page_avg_delay asc ";
		*/
		
		String sql = "SELECT a.groupname,cast(ifnull(page_avg_delay,999999) as signed)  page_avg_delay, cast(pre_page_avg_delay as SIGNED) pre_page_avg_delay  FROM ( SELECT groupname FROM servicequality_groupid_webbrowsing b WHERE" +
				" probetype='"+probetype+"' AND `month` IN ('"+thismonth+"','"+premonth+"') and groupid in ("+groupid+") group by groupid ) a" +
				" LEFT JOIN ( SELECT ifnull( sum( page_avg_delay * page_test_times ) / sum(page_test_times), 0 ) page_avg_delay, groupname, MONTH FROM servicequality_groupid_webbrowsing" +
				" WHERE	MONTH = '"+thismonth+"' AND groupid IN ("+groupid+") and probetype='"+probetype+"' GROUP BY `month`, groupname ) b ON a.groupname = b.groupname " +
				" LEFT JOIN ( SELECT ifnull( sum( page_avg_delay * page_test_times ) / sum(page_test_times), 0 ) pre_page_avg_delay, groupname, MONTH FROM servicequality_groupid_webbrowsing" +
				" WHERE	MONTH = '"+premonth+"' AND groupid IN ("+groupid+") and probetype='"+probetype+"' GROUP BY `month`, groupname ) c ON a.groupname = c.groupname order by page_avg_delay asc" ;
		List<Map<String, Object>> queryList = this.getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return queryList;
	}
	
}
