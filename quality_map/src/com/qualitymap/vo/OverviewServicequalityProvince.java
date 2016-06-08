package com.qualitymap.vo;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

// default package

/**
 * OverviewServicequalityProvince entity. @author MyEclipse Persistence Tools
 */
@Entity
@Table(name = "overview_servicequality_province")
public class OverviewServicequalityProvince {

	@Id
	private Integer id;
	private String groupid;
	private String province;
	private Double avg_delay;
	private Double page_success_rate;
	private Integer testtimes;
	private String month;

	// Constructors

	/** default constructor */
	public OverviewServicequalityProvince() {
	}

	public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
	}

	public String getGroupid() {
		return groupid;
	}

	public void setGroupid(String groupid) {
		this.groupid = groupid;
	}

	public String getProvince() {
		return province;
	}

	public void setProvince(String province) {
		this.province = province;
	}

	public Double getAvg_delay() {
		return avg_delay;
	}

	public void setAvg_delay(Double avg_delay) {
		this.avg_delay = avg_delay;
	}

	public Double getPage_success_rate() {
		return page_success_rate;
	}

	public void setPage_success_rate(Double page_success_rate) {
		this.page_success_rate = page_success_rate;
	}

	public Integer getTesttimes() {
		return testtimes;
	}

	public void setTesttimes(Integer testtimes) {
		this.testtimes = testtimes;
	}

	public String getMonth() {
		return month;
	}

	public void setMonth(String month) {
		this.month = month;
	}

}
