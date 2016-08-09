package com.qualitymap.dao;

import java.util.List;
import java.util.Map;

import com.qualitymap.vo.NetworkType;

/**
 * 网络制式分析统计
 * @author：kxc
 * @date：Apr 11, 2016
 */
public interface NetworkTypeDao {
	
	/**
	 * 获取联网方式分组的详细信息
	 * @param month
	 * @param groupid
	 * @return
	 * @return List<Map<String,Object>>
	 */
	List<Map<String, Object>> getNetTypeData(String month,String groupid);
	
	void saveType(NetworkType nettype);

	void deleteType(NetworkType nettype);

	List<NetworkType> find();

	void updateType(NetworkType nettype);

}
