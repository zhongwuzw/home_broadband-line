package com.qualitymap.dao.impl;

import java.util.List;
import java.util.Map;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.transform.Transformers;

import com.qualitymap.dao.ServicequalityGroupidHttpUploadDao;

/**
 * 
 * @author kongxiangchun
 * 
 */
public class ServicequalityGroupidHttpUploadDaoImpl implements ServicequalityGroupidHttpUploadDao {

	private SessionFactory sessionFactory;

	public void setSessionFactory(SessionFactory sessionFactory) {
		this.sessionFactory = sessionFactory;
	}

	public Session getSession() {
		return sessionFactory.getCurrentSession();
	}

	/**
	 * 获取上下月的打开时延
	 */
	@Override
	public List<Map<String, Object>> getHttpUploadRate(String yearMonth, String lastMonth, String groupid) {

		String sql = "SELECT a.probetype, round(thisdata,2) thisdata, round(lastdata,2) lastdata FROM ( SELECT DISTINCT probetype FROM servicequality_groupid_httpupload WHERE `month` in ("
				+ yearMonth + "," + lastMonth + ") AND groupid IN (" + groupid + ") ) a "
				+ "LEFT JOIN ( SELECT sum( upload_test_times * avg_upload_rate ) / SUM(upload_test_times) thisdata, probetype, 	MONTH FROM servicequality_groupid_httpupload " + "where month='"
				+ yearMonth + "' AND groupid IN (" + groupid + ")  GROUP BY `month`, probetype ) b ON a.probetype = b.probetype "
				+ "LEFT JOIN ( SELECT sum( 	upload_test_times * avg_upload_rate ) / SUM(upload_test_times) lastdata, probetype, MONTH FROM servicequality_groupid_httpupload " + "where month='"
				+ lastMonth + "' AND groupid IN (" + groupid + ") GROUP BY `month`, probetype ) c ON a.probetype = c.probetype";

		List<Map<String, Object>> queryList = this.getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return queryList;

	}

	/**
	 * 获取上传速率趋势数据、
	 */
	@Override
	public List<Map<String, Object>> getUploadRateData(String groupid, String probetype) {
		String sql = " SELECT round(sum(avg_upload_rate*upload_test_times)/sum(upload_test_times),2) avg_upload_rate ,month FROM servicequality_groupid_httpupload WHERE `groupid` in (" + groupid
				+ ") and  probetype='" + probetype + "' GROUP BY month order by month";

		List<Map<String, Object>> queryList = this.getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return queryList;
	}

	/**
	 * 获取上传速率的排名
	 */
	@Override
	public List<Map<String, Object>> getUploadRateOrder(String thismonth, String premonth, String groupid, String probetype) {
		/*String sql = " SELECT ifnull(sum(avg_upload_rate*upload_test_times)/sum(upload_test_times),0) avg_upload_rate ,"
				+ "(SELECT ifnull(sum(avg_upload_rate * upload_test_times) / sum(upload_test_times),0)  FROM servicequality_groupid_httpupload b WHERE `month` = '" + premonth + "' and probetype='"
				+ probetype + "' and b.groupid = a.groupid GROUP BY groupid ) pre_avg_upload_rate " + ",groupname FROM servicequality_groupid_httpupload a WHERE `groupid` in (" + groupid
				+ ") and month = '" + thismonth + "' and probetype='" + probetype + "' GROUP BY groupid order by avg_upload_rate desc ";
				
*/
		String sql = "SELECT a.groupname, avg_upload_rate, pre_avg_upload_rate FROM ( SELECT groupname FROM servicequality_groupid_httpupload b WHERE" +
				" probetype='"+probetype+"' AND `month` IN ('"+thismonth+"','"+premonth+"') and groupid in ("+groupid+") group by groupid ) a" +
				" LEFT JOIN ( SELECT ifnull( sum( avg_upload_rate * upload_test_times ) / sum(upload_test_times), 0 ) avg_upload_rate, groupname, MONTH FROM servicequality_groupid_httpupload" +
				" WHERE	MONTH = '"+thismonth+"' AND groupid IN ("+groupid+") and probetype='"+probetype+"' GROUP BY `month`, groupname ) b ON a.groupname = b.groupname " +
				" LEFT JOIN ( SELECT ifnull( sum( avg_upload_rate * upload_test_times ) / sum(upload_test_times), 0 ) pre_avg_upload_rate, groupname, MONTH FROM servicequality_groupid_httpupload" +
				" WHERE	MONTH = '"+premonth+"' AND groupid IN ("+groupid+") and probetype='"+probetype+"' GROUP BY `month`, groupname ) c ON a.groupname = c.groupname order by avg_upload_rate desc " ;
		
		
		List<Map<String, Object>> queryList = this.getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return queryList;
	}

}
