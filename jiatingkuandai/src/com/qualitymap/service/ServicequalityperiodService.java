package com.qualitymap.service;


/**
 * 
 * @author kongxiangchun
 *
 */
public interface ServicequalityperiodService {

	/**
	 * 分时段统计
	 * @param groupid
	 * @param month
	 * @param broadband_type
	 * @return
	 */
	String getperiodData(String groupid, String month, String broadband_type);
}
