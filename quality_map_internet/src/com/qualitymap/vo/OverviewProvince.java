package com.qualitymap.vo;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

// default package

/**
 * OverviewProvince entity. @author MyEclipse Persistence Tools
 */
@Entity
@Table(name = "overview_province")
public class OverviewProvince {

	@Id
	private Integer id;
	private String groupid;
	private String province;
	private String broadband_type;
	private Integer testtimes;
	private Integer new_user_num;

	// Constructors

	/** default constructor */
	public OverviewProvince() {
	}

	public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
	}

	public Integer getNew_user_num() {
		return new_user_num;
	}

	public void setNew_user_num(Integer new_user_num) {
		this.new_user_num = new_user_num;
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

	public String getBroadband_type() {
		return broadband_type;
	}

	public void setBroadband_type(String broadband_type) {
		this.broadband_type = broadband_type;
	}

	public Integer getTesttimes() {
		return testtimes;
	}

	public void setTesttimes(Integer testtimes) {
		this.testtimes = testtimes;
	}

}
