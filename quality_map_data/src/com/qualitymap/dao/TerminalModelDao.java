package com.qualitymap.dao;

import java.util.List;
import java.util.Map;

import com.qualitymap.vo.TerminalModel;


/**
 * 终端分布数据统计
 * @author：kxc
 * @date：Apr 12, 2016
 */
public interface TerminalModelDao {

	
	/**
	 * 获取终端分布统计数据
	 * @param month
	 * @param groupid
	 * @param terminalType
	 * @return
	 * @return String
	 */
	List<Map<String, Object>> getTerminalModelData(String month,String groupid,String terminalType);
	
	
	void saveModel(TerminalModel terminalModel);

	void deleteqModel(TerminalModel terminalModel);

	List<TerminalModel> findModel();

	void updateModel(TerminalModel terminalModel);

}
