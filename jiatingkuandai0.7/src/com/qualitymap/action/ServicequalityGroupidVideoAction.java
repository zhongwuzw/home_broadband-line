package com.qualitymap.action;

import javax.annotation.Resource;

import com.qualitymap.base.BaseAction;
import com.qualitymap.service.ServicequalityGroupidVideoService;

public class ServicequalityGroupidVideoAction extends BaseAction {

	@Resource
	ServicequalityGroupidVideoService groupidVideoService;

	
	/**
	 * 获取平均时延上下月数据
	 */
	public void getVideoDelay() {

		try {
			String yearMonth = this.servletRequest.getParameter("month");
			String uuid = this.servletRequest.getParameter("key");
			String groupid = this.getUserGroup(uuid);
			//String groupid = "50091,50067";
			String avgdata = groupidVideoService.getVideoDelay(yearMonth, groupid);
			printWriter(avgdata);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
	
	/**
	 * 获取视频播放上下月数据
	 */
	public void getVideoPlaySuccess() {

		try {
			String yearMonth = this.servletRequest.getParameter("month");
			String uuid = this.servletRequest.getParameter("key");
			String groupid = this.getUserGroup(uuid);
			//String groupid = "50091,50067";
			String avgdata = groupidVideoService.getVideoPlaySuccess(yearMonth, groupid);
			printWriter(avgdata);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}


	/**
	 * 视频卡顿次数上下月数据
	 */
	public void getCacheCount() {

		try {
			String yearMonth = this.servletRequest.getParameter("month");
			String uuid = this.servletRequest.getParameter("key");
			String groupid = this.getUserGroup(uuid);
			//String groupid = "50091,50067";
			String msg = groupidVideoService.getCacheCount(yearMonth, groupid);
			printWriter(msg);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
		/**
	 * 获取视频播放成功率趋势数据
	 * 
	 * @return void
	 */
	public void getVideoPlaySuccessData() {

		try {
			String probetype = this.servletRequest.getParameter("probetype");
			String uuid = this.servletRequest.getParameter("key");
			String groupid = this.getUserGroup(uuid);
			//String groupid = "50071,50074,50067";
			String avgdelayData = groupidVideoService.getVideoPlaySuccessData(groupid, probetype);
			printWriter(avgdelayData);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 获取视频播放成功率的排名
	 * 
	 * @return void
	 */
	public void getVideoPlaySuccessOrder() {

		try {
			String probetype = this.servletRequest.getParameter("probetype");
			String month = this.servletRequest.getParameter("month");
			String uuid = this.servletRequest.getParameter("key");
			String groupid = this.getUserGroup(uuid);
			//String groupid = "50071,50074,50067";
			String delayOrder = groupidVideoService.getVideoPlaySuccessOrder(month, groupid, probetype);
			printWriter(delayOrder);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	/**
	 * 平均视频缓冲时长占比
	 */
	public void getAvgBufferProportion() {

		try {
			String yearMonth = this.servletRequest.getParameter("month");
			String uuid = this.servletRequest.getParameter("key");
			String groupid = this.getUserGroup(uuid);
			String msg = groupidVideoService.getAvgBufferProportion(yearMonth, groupid);
			printWriter(msg);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 获取视频加载时长趋势数据
	 * 
	 * @return void
	 */
	public void getVideoDelayData() {

		try {
			String probetype = this.servletRequest.getParameter("probetype");
			String uuid = this.servletRequest.getParameter("key");
			String groupid = this.getUserGroup(uuid);
			//String groupid = "50071,50074,50067";
			String avgdelayData = groupidVideoService.getVideoDelayData(groupid, probetype);
			printWriter(avgdelayData);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 获取视频加载时长的排名
	 * 
	 * @return void
	 */
	public void getVideoDelayOrder() {

		try {
			String probetype = this.servletRequest.getParameter("probetype");
			String month = this.servletRequest.getParameter("month");
			String uuid = this.servletRequest.getParameter("key");
			String groupid = this.getUserGroup(uuid);
			//String groupid = "50071,50074,50067";
			String delayOrder = groupidVideoService.getVideoDelayOrder(month, groupid, probetype);
			printWriter(delayOrder);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	/**
	 * 获取视频卡顿次数数据
	 * 
	 * @return void
	 */
	public void getVideoCacheCountData() {
		
		try {
			String probetype = this.servletRequest.getParameter("probetype");
			String uuid = this.servletRequest.getParameter("key");
			String groupid = this.getUserGroup(uuid);
			//String groupid = "50071,50074,50067";
			String avgdelayData = groupidVideoService.getVideoCacheCountData(groupid, probetype);
			printWriter(avgdelayData);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 获取视频卡顿次数的排名
	 * 
	 * @return void
	 */
	public void getVideoCacheCountOrder() {
		
		try {
			String probetype = this.servletRequest.getParameter("probetype");
			String month = this.servletRequest.getParameter("month");
			String uuid = this.servletRequest.getParameter("key");
			String groupid = this.getUserGroup(uuid);
			//String groupid = "50071,50074,50067";
			String delayOrder = groupidVideoService.getVideoCacheCountOrder(month, groupid, probetype);
			printWriter(delayOrder);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 全国家庭宽带平均视频总缓冲时长占比趋势
	 * 
	 * @return void
	 */
	public void getVideoBufferProportionData() {
		
		try {
			String probetype = this.servletRequest.getParameter("probetype");
			String uuid = this.servletRequest.getParameter("key");
			String groupid = this.getUserGroup(uuid);
			String avgdelayData = groupidVideoService.getVideoBufferProportionData(groupid, probetype);
			printWriter(avgdelayData);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 全国家庭宽带平均视频总缓冲时长占比排名
	 * 
	 * @return void
	 */
	public void getVideoBufferProportionOrder() {
		
		try {
			String probetype = this.servletRequest.getParameter("probetype");
			String month = this.servletRequest.getParameter("month");
			String uuid = this.servletRequest.getParameter("key");
			String groupid = this.getUserGroup(uuid);
			String delayOrder = groupidVideoService.getVideoBufferProportionOrder(month, groupid, probetype);
			printWriter(delayOrder);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
}
