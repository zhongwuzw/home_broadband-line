package com.qualitymap.dao.impl;

import java.util.List;
import java.util.Map;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.transform.Transformers;

import com.qualitymap.dao.ServicequalityGroupidVideoDao;

/**
 * 
 * @author kongxiangchun
 * 
 */
public class ServicequalityGroupidVideoDaoImpl implements ServicequalityGroupidVideoDao {

	private SessionFactory sessionFactory;

	public void setSessionFactory(SessionFactory sessionFactory) {
		this.sessionFactory = sessionFactory;
	}

	public Session getSession() {
		return sessionFactory.getCurrentSession();
	}

	/**
	 * 获取上下月的播放成功率
	 */
	@Override
	public List<Map<String, Object>> getVideoPlaySuccess(String yearMonth, String lastMonth, String groupid) {

		String sql = "SELECT a.probetype, round(thisdata,2)*100 thisdata, round(lastdata,2)*100 lastdata FROM ( SELECT DISTINCT probetype FROM servicequality_groupid_video WHERE `month` in (" + yearMonth
				+ "," + lastMonth + ") AND groupid IN (" + groupid + ") ) a "
				+ "LEFT JOIN ( SELECT sum( video_test_times * success_rate ) / SUM(video_test_times) thisdata, probetype, 	MONTH FROM servicequality_groupid_video " + "where month='" + yearMonth
				+ "' AND groupid IN (" + groupid + ") GROUP BY `month`, probetype ) b ON a.probetype = b.probetype "
				+ "LEFT JOIN ( SELECT sum( 	video_test_times * success_rate ) / SUM(video_test_times) lastdata, probetype, MONTH FROM servicequality_groupid_video " + "where month='" + lastMonth
				+ "' AND groupid IN (" + groupid + ") GROUP BY `month`, probetype ) c ON a.probetype = c.probetype";

		List<Map<String, Object>> queryList = this.getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return queryList;

	}
	
	/**
	 * 获取上下月的打开时延
	 */
	@Override
	public List<Map<String, Object>> getVideoDelay(String yearMonth, String lastMonth, String groupid) {

		String sql = "SELECT a.probetype, round(thisdata,0) thisdata, round(lastdata,0) lastdata FROM ( SELECT DISTINCT probetype FROM servicequality_groupid_video WHERE `month` in (" + yearMonth
				+ "," + lastMonth + ") AND groupid IN (" + groupid + ") ) a "
				+ "LEFT JOIN ( SELECT sum( video_test_times * avg_video_delay ) / SUM(video_test_times) thisdata, probetype, 	MONTH FROM servicequality_groupid_video " + "where month='" + yearMonth
				+ "' AND groupid IN (" + groupid + ") GROUP BY `month`, probetype ) b ON a.probetype = b.probetype "
				+ "LEFT JOIN ( SELECT sum( 	video_test_times * avg_video_delay ) / SUM(video_test_times) lastdata, probetype, MONTH FROM servicequality_groupid_video " + "where month='" + lastMonth
				+ "' AND groupid IN (" + groupid + ") GROUP BY `month`, probetype ) c ON a.probetype = c.probetype";

		List<Map<String, Object>> queryList = this.getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return queryList;

	}

	/**
	 * 获取视频卡顿次数上下月数据
	 */
	@Override
	public List<Map<String, Object>> getCacheCount(String yearMonth, String lastMonth, String groupid) {

		String sql = "SELECT a.probetype, ROUND(thisdata,1) thisdata,ROUND(lastdata,1) lastdata FROM ( SELECT DISTINCT probetype FROM servicequality_groupid_video WHERE `month` in (" + yearMonth + "," + lastMonth
				+ ") AND groupid IN (" + groupid + ") ) a " + "LEFT JOIN ( SELECT sum(  video_cache_count )  thisdata, probetype, 	MONTH FROM servicequality_groupid_video " + "where month='"
				+ yearMonth + "' AND groupid IN (" + groupid + ") GROUP BY `month`, probetype ) b ON a.probetype = b.probetype "
				+ "LEFT JOIN ( SELECT sum( video_cache_count ) lastdata, probetype, MONTH FROM servicequality_groupid_video " + "where month='" + lastMonth + "' AND groupid IN (" + groupid
				+ ") GROUP BY `month`, probetype ) c ON a.probetype = c.probetype" + "";

		List<Map<String, Object>> queryList = this.getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return queryList;
	}
	
		/**
	 * 获取视频播放成功率趋势数据
	 */
	@Override
	public List<Map<String, Object>> getVideoPlaySuccessData(String groupid, String probetype) {
		String sql = " SELECT sum(success_rate*video_test_times)/sum(video_test_times) success_rate ,month FROM servicequality_groupid_video WHERE `groupid` in (" + groupid
				+ ") and  probetype='" + probetype + "' GROUP BY month order by month";

		List<Map<String, Object>> queryList = this.getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return queryList;
	}

	/**
	 * 获取视频播放成功率的排名
	 */
	@Override
	public List<Map<String, Object>> getVideoPlaySuccessOrder(String thismonth, String premonth, String groupid, String probetype) {
		/*String sql = " SELECT ifnull(sum(avg_video_delay*video_test_times)/sum(video_test_times),0) avg_video_delay ,"
				+ "(SELECT ifnull(sum(avg_video_delay * video_test_times) / sum(video_test_times),0)  FROM servicequality_groupid_video b WHERE `month` = '" + premonth + "' and probetype='"
				+ probetype + "' and b.groupid = a.groupid GROUP BY groupid ) pre_avg_video_delay " + ",groupname FROM servicequality_groupid_video a WHERE `groupid` in (" + groupid
				+ ") and month = '" + thismonth + "' and probetype='" + probetype + "' GROUP BY groupid order by avg_video_delay asc ";
*/
		String sql = "SELECT a.groupname,  cast(ifnull(success_rate,999999) as signed) success_rate, cast(pre_success_rate as SIGNED) pre_success_rate FROM ( SELECT groupname FROM servicequality_groupid_video b WHERE" +
				" probetype='"+probetype+"' AND `month` IN ('"+thismonth+"','"+premonth+"') and groupid in ("+groupid+") group by groupid ) a" +
				" LEFT JOIN ( SELECT ifnull( sum( success_rate * video_test_times ) / sum(video_test_times), 0 ) success_rate, groupname, MONTH FROM servicequality_groupid_video" +
				" WHERE	MONTH = '"+thismonth+"' AND groupid IN ("+groupid+") and probetype='"+probetype+"' GROUP BY `month`, groupname ) b ON a.groupname = b.groupname " +
				" LEFT JOIN ( SELECT ifnull( sum( success_rate * video_test_times ) / sum(video_test_times), 0 ) pre_success_rate, groupname, MONTH FROM servicequality_groupid_video" +
				" WHERE	MONTH = '"+premonth+"' AND groupid IN ("+groupid+") and probetype='"+probetype+"' GROUP BY `month`, groupname ) c ON a.groupname = c.groupname order by success_rate asc" ;
		
		List<Map<String, Object>> queryList = this.getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return queryList;
	}
	
	/**
	 * 平均视频缓冲时长占比
	 */
	@Override
	public List<Map<String, Object>> getAvgBufferProportion(String yearMonth, String lastMonth, String groupid) {

		String sql = "SELECT a.probetype, round(thisdata,2) thisdata, round(lastdata,2) lastdata FROM ( SELECT DISTINCT probetype FROM servicequality_groupid_video WHERE `month` in (" + yearMonth
				+ "," + lastMonth + ") AND groupid IN (" + groupid + ") ) a "
				+ "LEFT JOIN ( SELECT sum( video_test_times * avg_buffer_proportion ) / SUM(video_test_times)*100 thisdata, probetype, 	MONTH FROM servicequality_groupid_video " + "where month='" + yearMonth
				+ "' AND groupid IN (" + groupid + ") GROUP BY `month`, probetype ) b ON a.probetype = b.probetype "
				+ "LEFT JOIN ( SELECT sum( 	video_test_times * avg_buffer_proportion ) / SUM(video_test_times)*100 lastdata, probetype, MONTH FROM servicequality_groupid_video " + "where month='" + lastMonth
				+ "' AND groupid IN (" + groupid + ") GROUP BY `month`, probetype ) c ON a.probetype = c.probetype";

		List<Map<String, Object>> queryList = this.getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return queryList;
	}
	
	/**
	 * 获取视频加载时长趋势数据
	 */
	@Override
	public List<Map<String, Object>> getVideoDelayData(String groupid, String probetype) {
		String sql = " SELECT sum(avg_video_delay*video_test_times)/sum(video_test_times) avg_video_delay ,month FROM servicequality_groupid_video WHERE `groupid` in (" + groupid
				+ ") and  probetype='" + probetype + "' GROUP BY month order by month";

		List<Map<String, Object>> queryList = this.getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return queryList;
	}

	/**
	 * 获取视频加载时长的排名
	 */
	@Override
	public List<Map<String, Object>> getVideoDelayOrder(String thismonth, String premonth, String groupid, String probetype) {
		/*String sql = " SELECT ifnull(sum(avg_video_delay*video_test_times)/sum(video_test_times),0) avg_video_delay ,"
				+ "(SELECT ifnull(sum(avg_video_delay * video_test_times) / sum(video_test_times),0)  FROM servicequality_groupid_video b WHERE `month` = '" + premonth + "' and probetype='"
				+ probetype + "' and b.groupid = a.groupid GROUP BY groupid ) pre_avg_video_delay " + ",groupname FROM servicequality_groupid_video a WHERE `groupid` in (" + groupid
				+ ") and month = '" + thismonth + "' and probetype='" + probetype + "' GROUP BY groupid order by avg_video_delay asc ";
*/
		String sql = "SELECT a.groupname,  cast(ifnull(avg_video_delay,999999) as signed) avg_video_delay, cast(pre_avg_video_delay as SIGNED) pre_avg_video_delay FROM ( SELECT groupname FROM servicequality_groupid_video b WHERE" +
				" probetype='"+probetype+"' AND `month` IN ('"+thismonth+"','"+premonth+"') and groupid in ("+groupid+") group by groupid ) a" +
				" LEFT JOIN ( SELECT ifnull( sum( avg_video_delay * video_test_times ) / sum(video_test_times), 0 ) avg_video_delay, groupname, MONTH FROM servicequality_groupid_video" +
				" WHERE	MONTH = '"+thismonth+"' AND groupid IN ("+groupid+") and probetype='"+probetype+"' GROUP BY `month`, groupname ) b ON a.groupname = b.groupname " +
				" LEFT JOIN ( SELECT ifnull( sum( avg_video_delay * video_test_times ) / sum(video_test_times), 0 ) pre_avg_video_delay, groupname, MONTH FROM servicequality_groupid_video" +
				" WHERE	MONTH = '"+premonth+"' AND groupid IN ("+groupid+") and probetype='"+probetype+"' GROUP BY `month`, groupname ) c ON a.groupname = c.groupname order by avg_video_delay asc" ;
		
		List<Map<String, Object>> queryList = this.getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return queryList;
	}

	/**
	 * 获取视频卡顿次数数据
	 */
	@Override
	public List<Map<String, Object>> getVideoCacheCountData(String groupid, String probetype) {
		String sql = " SELECT sum(video_cache_count*video_test_times)/sum(video_test_times) video_cache_count ,month FROM servicequality_groupid_video WHERE `groupid` in (" + groupid
				+ ") and  probetype='" + probetype + "' GROUP BY month order by month";

		List<Map<String, Object>> queryList = this.getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return queryList;
	}

	/**
	 * 获取视频卡顿次数的排名
	 */
	@Override
	public List<Map<String, Object>> getVideoCacheCountOrder(String thismonth, String premonth, String groupid, String probetype) {
	/*	String sql = " SELECT ifnull(sum(video_cache_count*video_test_times)/sum(video_test_times),0) video_cache_count ,"
				+ "(SELECT ifnull(sum(video_cache_count * video_test_times) / sum(video_test_times),0)  FROM servicequality_groupid_video b WHERE `month` = '" + premonth + "' and probetype='"
				+ probetype + "' and b.groupid = a.groupid GROUP BY groupid ) pre_video_cache_count " + ",groupname FROM servicequality_groupid_video a WHERE `groupid` in (" + groupid
				+ ") and month = '" + thismonth + "' and probetype='" + probetype + "' GROUP BY groupid order by video_cache_count asc ";
*/
		String sql = "SELECT a.groupname, ROUND(video_cache_count,1) video_cache_count, ROUND(pre_video_cache_count,1) pre_video_cache_count  FROM ( SELECT groupname FROM servicequality_groupid_video b WHERE" +
				" probetype='"+probetype+"' AND `month` IN ('"+thismonth+"','"+premonth+"') and groupid in ("+groupid+") group by groupid ) a" +
				" LEFT JOIN ( SELECT ifnull( sum( video_cache_count * video_test_times ) / sum(video_test_times), 0 ) video_cache_count, groupname, MONTH FROM servicequality_groupid_video" +
				" WHERE	MONTH = '"+thismonth+"' AND groupid IN ("+groupid+") and probetype='"+probetype+"' GROUP BY `month`, groupname ) b ON a.groupname = b.groupname " +
				" LEFT JOIN ( SELECT ifnull( sum( video_cache_count * video_test_times ) / sum(video_test_times), 0 ) pre_video_cache_count, groupname, MONTH FROM servicequality_groupid_video" +
				" WHERE	MONTH = '"+premonth+"' AND groupid IN ("+groupid+") and probetype='"+probetype+"' GROUP BY `month`, groupname ) c ON a.groupname = c.groupname order by video_cache_count asc " ;
		
		List<Map<String, Object>> queryList = this.getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return queryList;
	}
	
	/**
	 * 全国家庭宽带平均视频总缓冲时长占比数据
	 */
	@Override
	public List<Map<String, Object>> getVideoBufferProportionData(String groupid, String probetype) {
		String sql = " SELECT sum(ifnull(avg_buffer_proportion,0)*video_test_times)/sum(video_test_times)*100 avg_buffer_proportion ,month FROM servicequality_groupid_video WHERE `groupid` in (" + groupid
				+ ") and  probetype='" + probetype + "' GROUP BY month order by month";

		List<Map<String, Object>> queryList = this.getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return queryList;
	}

	/**
	 * 全国家庭宽带平均视频总缓冲时长占比排名
	 */
	@Override
	public List<Map<String, Object>> getVideoBufferProportionOrder(String thismonth, String premonth, String groupid, String probetype) {
		String sql = "SELECT a.groupname, ROUND(avg_buffer_proportion,2) avg_buffer_proportion, ROUND(pre_avg_buffer_proportion,2) pre_avg_buffer_proportion  FROM ( SELECT groupname FROM servicequality_groupid_video b WHERE" +
				" probetype='"+probetype+"' AND `month` IN ('"+thismonth+"','"+premonth+"') and groupid in ("+groupid+") group by groupid ) a" +
				" LEFT JOIN ( SELECT ifnull( sum( avg_buffer_proportion * video_test_times ) / sum(video_test_times)*100, 0 ) avg_buffer_proportion, groupname, MONTH FROM servicequality_groupid_video" +
				" WHERE	MONTH = '"+thismonth+"' AND groupid IN ("+groupid+") and probetype='"+probetype+"' GROUP BY `month`, groupname ) b ON a.groupname = b.groupname " +
				" LEFT JOIN ( SELECT ifnull( sum( avg_buffer_proportion * video_test_times ) / sum(video_test_times)*100, 0 ) pre_avg_buffer_proportion, groupname, MONTH FROM servicequality_groupid_video" +
				" WHERE	MONTH = '"+premonth+"' AND groupid IN ("+groupid+") and probetype='"+probetype+"' GROUP BY `month`, groupname ) c ON a.groupname = c.groupname order by avg_buffer_proportion asc " ;
		
		List<Map<String, Object>> queryList = this.getSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
		return queryList;
	}

}
