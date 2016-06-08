package com.qualitymap.action;

import javax.annotation.Resource;

import com.qualitymap.base.BaseAction;
import com.qualitymap.service.NetworkTypeService;

/**
 * 获取网络制式数据统计
 * 
 * @author：kxc
 * @date：Apr 11, 2016
 */
public class NetworkTypeAction extends BaseAction {

	/**
	 * serialVersionUID long NetworkTypeAction.java
	 */
	private static final long serialVersionUID = 1L;
	@Resource
	private NetworkTypeService typeService;

	/**
	 * 获取联网方式信息
	 * 
	 * @return void
	 */
	public void getNetTypeData() {

		try {
			String month = this.servletRequest.getParameter("month");
			String uuid = this.servletRequest.getParameter("key");
			String groupid = this.getUserGroup(uuid);

			String nettypeData = typeService.getNetworkTypeData(month, groupid);
			printWriter(nettypeData);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
