package com.qualitymap.dao;

import java.util.List;
import java.util.Map;

/**
 * 
 * 分时段统计
 * @author kongxiangchun
 *
 */
public interface ServicequalityperiodDao {

	/**
	 * 获取分地域指标统计
	 * @param groupid
	 * @param month
	 * @param broadband_type
	 * @return
	 */
	List<Map<String, Object>> getPeriodrateHttpdownload(String groupid, String month, String broadband_type);
	
	List<Map<String, Object>> getPeriodNinetyDelayWebbrowsing(String groupid, String month, String broadband_type);
	List<Map<String, Object>> getPeriodAvgDelayWebbrowsing(String groupid, String month, String broadband_type);
	
	List<Map<String, Object>> getPeriodVideoDelayVideo(String groupid, String month, String broadband_type);
	List<Map<String, Object>> getPeriodcacheCountVideo(String groupid, String month, String broadband_type);
	List<Map<String, Object>> getPeriodBufferproportionVideo(String groupid, String month, String broadband_type);
	

}
