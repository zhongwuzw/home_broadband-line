package com.qualitymap.dao;

import java.util.List;
import java.util.Map;

/**
 * 
 * @author kongxiangchun
 *
 */
public interface ServicequalityGroupidHttpDownloadDao {

	
	/**
	 * 获取上下月的下载速率
	 */
	public List<Map<String, Object>> getHttpDownloadRate(String yearMonth, String lastMonth, String groupid);
	
	/**
	 * 获取下载速率趋势数据
	 * @param groupid
	 * @return
	 * @return List<Map<String,Object>>
	 */
	List<Map<String, Object>> getDownloadRateData(String groupid,String probetype);
	
	/**
	 * 获取下载速率的排名
	 * @param thismonth
	 * @param premonth
	 * @param groupid
	 * @return
	 * @return List<Map<String,Object>>
	 */
	List<Map<String, Object>> getDownloadRateOrder(String thismonth,String premonth,String groupid,String probetype);
	
}
