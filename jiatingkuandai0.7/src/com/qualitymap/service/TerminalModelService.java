package com.qualitymap.service;


/**
 * 终端分析
 * @author：kxc
 * @date：Apr 12, 2016
 */
public interface TerminalModelService {

	/**
	 * 获取终端分布统计数据
	 * @param month
	 * @param groupid
	 * @param terminalType
	 * @return
	 * @return String
	 */
	String getTerminalModelData(String month,String groupid,String terminalType);
}
