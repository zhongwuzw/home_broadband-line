package com.qualitymap.vo;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

// default package

/**
 * OverviewKpi entity. @author MyEclipse Persistence Tools
 */
@Entity
@Table(name = "overview_kpi_api")
public class OverviewKpiApi {

	@Id
	private Integer id;
	private String month;
	private String username;
	private String reg_user_num;
	private String accumulativ_user_num;
	public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
	}

	public String getMonth() {
		return month;
	}

	public void setMonth(String month) {
		this.month = month;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getReg_user_num() {
		return reg_user_num;
	}

	public void setReg_user_num(String reg_user_num) {
		this.reg_user_num = reg_user_num;
	}

	public String getAccumulativ_user_num() {
		return accumulativ_user_num;
	}

	public void setAccumulativ_user_num(String accumulativ_user_num) {
		this.accumulativ_user_num = accumulativ_user_num;
	}

	// Constructors

	/** default constructor */
	public OverviewKpiApi() {
	}

	

}
