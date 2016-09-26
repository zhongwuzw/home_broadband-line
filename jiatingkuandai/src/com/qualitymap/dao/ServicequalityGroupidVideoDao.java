package com.qualitymap.dao;

import java.util.List;
import java.util.Map;

/**
 * 
 * @author kongxiangchun
 *
 */
public interface ServicequalityGroupidVideoDao {

	/**
	 * 获取上下月的播放成功率
	 */
	public List<Map<String, Object>> getVideoPlaySuccess(String yearMonth, String lastMonth, String groupid);
	
	/**
	 * 获取上下月的平均时延
	 */
	public List<Map<String, Object>> getVideoDelay(String yearMonth, String lastMonth, String groupid);
	
	/**
	 * 获取上下月的首次缓冲时延
	 */
	public List<Map<String, Object>> getVideoFistBufferDelay(String yearMonth, String lastMonth, String groupid);
	
	
	/**
	 * 视频卡顿次数上下月数据
	 */
	public List<Map<String, Object>> getCacheCount(String yearMonth, String lastMonth, String groupid);
	
	/** 获取视频播放成功率趋势数据
	 * @param groupid
	 * @return
	 * @return List<Map<String,Object>>
	 */
	List<Map<String, Object>> getVideoPlaySuccessData(String groupid,String probetype);
	
	/**
	 * 获取视频视频播放成功率的排名
	 * @param thismonth
	 * @param premonth
	 * @param groupid
	 * @return
	 * @return List<Map<String,Object>>
	 */
	List<Map<String, Object>> getVideoPlaySuccessOrder(String thismonth,String premonth,String groupid,String probetype);
	
	/**
	 * 平均视频缓冲时长占比
	 */
	public List<Map<String, Object>> getAvgBufferProportion(String yearMonth, String lastMonth, String groupid);
	
	/**
	 * 获取视频加载时长趋势数据
	 * @param groupid
	 * @return
	 * @return List<Map<String,Object>>
	 */
	List<Map<String, Object>> getVideoDelayData(String groupid,String probetype);
	
	/**
	 * 获取视频加载时长的排名
	 * @param thismonth
	 * @param premonth
	 * @param groupid
	 * @return
	 * @return List<Map<String,Object>>
	 */
	List<Map<String, Object>> getVideoDelayOrder(String thismonth,String premonth,String groupid,String probetype);
	/**
	 * 获取视频卡顿次数数据
	 * @param groupid
	 * @return
	 * @return List<Map<String,Object>>
	 */
	List<Map<String, Object>> getVideoCacheCountData(String groupid,String probetype);
	/**
	 * 获取视频首次缓冲时延数据
	 * @param groupid
	 * @return
	 * @return List<Map<String,Object>>
	 */
	List<Map<String, Object>> getVideoFirstBufferDelayData(String groupid,String probetype);
	/**
	 * 获取视频首次缓冲时延排名
	 * @param groupid
	 * @return
	 * @return List<Map<String,Object>>
	 */
	List<Map<String, Object>> getVideoFirstBufferDelayOrder(String thismonth, String premonth, String groupid, String probetype);
	
	/**
	 * 获取视频卡顿次数的排名
	 * @param thismonth
	 * @param premonth
	 * @param groupid
	 * @return
	 * @return List<Map<String,Object>>
	 */
	List<Map<String, Object>> getVideoCacheCountOrder(String thismonth,String premonth,String groupid,String probetype);
	
	/**
	 * 全国家庭宽带平均视频总缓冲时长占比数据
	 * @param groupid
	 * @return
	 * @return List<Map<String,Object>>
	 */
	List<Map<String, Object>> getVideoBufferProportionData(String groupid,String probetype);
	
	/**
	 * 全国家庭宽带平均视频总缓冲时长占比排名
	 * @param thismonth
	 * @param premonth
	 * @param groupid
	 * @return
	 * @return List<Map<String,Object>>
	 */
	List<Map<String, Object>> getVideoBufferProportionOrder(String thismonth,String premonth,String groupid,String probetype);
	
}
