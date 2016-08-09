package com.qualitymap.dao;

import java.util.List;
import java.util.Map;

public interface OverviewServicequalityDao {

	/**
	 * 获取用户指标统计
	 * @param groupid
	 * @param month
	 * @param broadband_type
	 * @return
	 */
	List<Map<String, Object>> getHttpDownloadUserIndicator(String groupid, String month, String broadband_type);
	List<Map<String, Object>> getHttpDownloadSuccessUserIndicator(String groupid, String month, String broadband_type);
	List<Map<String, Object>> getHttpUploadUserIndicator(String groupid, String month, String broadband_type);
	List<Map<String, Object>> getVideoProportionUserIndicator(String groupid, String month, String broadband_type);
	List<Map<String, Object>> getVideoDelayUserIndicator(String groupid, String month, String broadband_type);
	List<Map<String, Object>> getVideoCache_countUserIndicator(String groupid, String month, String broadband_type);
	List<Map<String, Object>> getVideoPaly_successUserIndicator(String groupid, String month, String broadband_type);
	//List<Map<String, Object>> getVideoUserIndicator(String groupid, String month, String broadband_type);
	List<Map<String, Object>> getWebbrowsingAvgdelayUserIndicator(String groupid, String month, String broadband_type);
	List<Map<String, Object>> getWebbrowsingninetydelayUserIndicator(String groupid, String month, String broadband_type);
	List<Map<String, Object>> getWebbrowsingsuccessUserIndicator(String groupid, String month, String broadband_type);
	List<Map<String, Object>> getWebbrowsingVisitSuccessRate(String groupid, String month, String broadband_type);
	
	
	/**
	 * 获取地域级指标数据统计
	 * @param groupid
	 * @param month
	 * @param broadband_type
	 * @return
	 */
	List<Map<String, Object>> getTerritoryHttpDownload(String groupid, String month, String broadband_type);
	List<Map<String, Object>> getTerritoryHttpUpload(String groupid, String month, String broadband_type);
	List<Map<String, Object>> getTerritoryVideo(String groupid, String month, String broadband_type);
	List<Map<String, Object>> getTerritoryWebbrowsing(String groupid, String month, String broadband_type);
	
	
	
	/**
	 * 获取分地域指标统计
	 * @param groupid
	 * @param month
	 * @param broadband_type
	 * @return
	 */
	List<Map<String, Object>> getCityrateHttpdownload(String groupid, String month, String broadband_type);
	List<Map<String, Object>> getCitysuccessHttpdownload(String groupid, String month, String broadband_type);
	
	List<Map<String, Object>> getCityNinetyDelayWebbrowsing(String groupid, String month, String broadband_type);
	List<Map<String, Object>> getCityAvgDelayWebbrowsing(String groupid, String month, String broadband_type);
	List<Map<String, Object>> getCityPageSuccessRateWebbrowsing(String groupid, String month, String broadband_type);
	List<Map<String, Object>> getCitySuccessWebbrowsing(String groupid, String month, String broadband_type);
	
	List<Map<String, Object>> getCityVideoDelayVideo(String groupid, String month, String broadband_type);
	List<Map<String, Object>> getCitycacheCountVideo(String groupid, String month, String broadband_type);
	List<Map<String, Object>> getCityPlaySuccessVideo(String groupid, String month, String broadband_type);
	List<Map<String, Object>> getCityBufferproportionVideo(String groupid, String month, String broadband_type);
	
}
