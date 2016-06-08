package com.qualitymap.dao;

import java.util.List;
import java.util.Map;

import com.qualitymap.service.OverviewServicequalityProvinceService;
import com.qualitymap.vo.OverviewServicequalityProvince;

/**
 * 业务质量数据统计
 * @author：kxc
 * @date：Apr 11, 2016
 */
public interface OverviewServicequalityProvinceDao {


	
	

	void saveQualityProvince(OverviewServicequalityProvince serqualityprovince);

	void deleteqQualityProvinc(OverviewServicequalityProvince serqualityprovince);

	List<OverviewServicequalityProvince> findQualityProvinc();

	void updateQualityProvinc(OverviewServicequalityProvince serqualityprovince);



}
