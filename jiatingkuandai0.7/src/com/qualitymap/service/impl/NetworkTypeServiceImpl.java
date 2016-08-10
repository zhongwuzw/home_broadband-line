package com.qualitymap.service.impl;

import java.util.List;
import java.util.Map;

import javax.annotation.Resource;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import com.qualitymap.dao.NetworkTypeDao;
import com.qualitymap.service.NetworkTypeService;
import com.qualitymap.vo.NetworkType;

/**
 * 获取网络制式数据统计
 * 
 * @author：kxc
 * @date：Apr 11, 2016
 */
public class NetworkTypeServiceImpl implements NetworkTypeService {

	@Resource
	NetworkTypeDao typeDao;

	@Override
	public List<NetworkType> savetype() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void save(NetworkType networkType) {
		// TODO Auto-generated method stub
		typeDao.saveType(networkType);
	}

	@Override
	public void deletenet(NetworkType networkType) {
		// TODO Auto-generated method stub
		typeDao.deleteType(networkType);
	}

	/**
	 * 获取联网方式详细信息
	 */

	@Override
	public String getNetworkTypeData(String month, String groupid) {
		// TODO Auto-generated method stub

		JSONObject dataJSON = new JSONObject();
		JSONArray dataarray = new JSONArray();
		if (!"".equals(groupid)) {
			List<Map<String, Object>> queryList = typeDao.getNetTypeData(month, groupid);

			dataarray = JSONArray.fromObject(queryList);
			dataJSON.put("data", dataarray);
		} else {
			dataJSON.put("data", dataarray);
		}
		return dataJSON.toString();
	}

}