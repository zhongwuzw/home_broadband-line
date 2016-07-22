package com.qualitymap.dao;

import java.util.List;
import java.util.Map;

/**
 * 
 * @author kongxiangchun
 *
 */
public interface ServicequalityGroupidHttpUploadDao {

	
	/**
	 * 获取上下月的平均时延
	 */
	public List<Map<String, Object>> getHttpUploadRate(String yearMonth, String lastMonth, String groupid);
	
	/**
	 * 获取上传速率趋势数据
	 * @param groupid
	 * @return
	 * @return List<Map<String,Object>>
	 */
	List<Map<String, Object>> getUploadRateData(String groupid,String probetype);
	
	/**
	 * 获取上传速率的排名
	 * @param thismonth
	 * @param premonth
	 * @param groupid
	 * @return
	 * @return List<Map<String,Object>>
	 */
	List<Map<String, Object>> getUploadRateOrder(String thismonth,String premonth,String groupid,String probetype);
	
}
