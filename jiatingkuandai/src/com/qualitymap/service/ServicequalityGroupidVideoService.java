package com.qualitymap.service;

import java.util.List;
import java.util.Map;


/**
 * 
 * @author kongxiangchun
 *
 */
public interface ServicequalityGroupidVideoService {
	
	
	/**
	 * 获取上下月的平均时延
	 * @param yearMonth
	 * @param groupid
	 * @return
	 * @return String
	 */
	String getVideoDelay(String yearMonth,String groupid);
	

	/**
	/**
	 * 视频卡顿次数上下月数据
	 * @param yearMonth
	 * @param groupid
	 * @return
	 * @return String
	 */
	String getCacheCount(String yearMonth,String groupid);
	
	/**
	 * 获取视频播放成功率趋势数据
	 * @param groupid
	 * @return
	 * @return String
	 */
	String getVideoPlaySuccessData(String groupid,String probetype);
	
	/**
	 * 获取视频播放成功率的排名
	 * @param thismonth
	 * @param premonth
	 * @param groupid
	 * @return
	 * @return String
	 */
	String getVideoPlaySuccessOrder(String thismonth,String groupid,String probetype);
	
	/**
	 * 获取视频加载时长趋势数据
	 * @param groupid
	 * @return
	 * @return String
	 */
	String getVideoDelayData(String groupid,String probetype);
	
	/**
	 * 获取视频加载时长的排名
	 * @param thismonth
	 * @param premonth
	 * @param groupid
	 * @return
	 * @return String
	 */
	String getVideoDelayOrder(String thismonth,String groupid,String probetype);

	/**
	 * 获取视频卡顿次数数据
	 * @param groupid
	 * @return
	 * @return String
	 */
	String getVideoCacheCountData(String groupid,String probetype);
	
	/**
	 * 获取视频卡顿次数的排名
	 * @param thismonth
	 * @param premonth
	 * @param groupid
	 * @return
	 * @return String
	 */
	String getVideoCacheCountOrder(String thismonth,String groupid,String probetype);
	
}
