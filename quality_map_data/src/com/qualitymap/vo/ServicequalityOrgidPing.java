package com.qualitymap.vo;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * ServicequalityOrgidPing entity. @author MyEclipse Persistence Tools
 */
@Entity
@Table(name = "servicequality_orgid_ping")
public class ServicequalityOrgidPing {

	@Id
	private Integer id;
	private String month;
	private String groupid;
	private String groupname;
	private String orgid;
	private String orgname;
	private String ping_test_times;
	private String broadband_type;
	private String ping_delay;
	private String ping_loss_rate;
	private String best_ping_delay;
	private String best_ping_loss_rate;

	public String getMonth() {
		return month;
	}

	public void setMonth(String month) {
		this.month = month;
	}

	public String getGroupid() {
		return groupid;
	}

	public void setGroupid(String groupid) {
		this.groupid = groupid;
	}

	public String getGroupname() {
		return groupname;
	}

	public void setGroupname(String groupname) {
		this.groupname = groupname;
	}

	public String getOrgid() {
		return orgid;
	}

	public void setOrgid(String orgid) {
		this.orgid = orgid;
	}

	public String getOrgname() {
		return orgname;
	}

	public void setOrgname(String orgname) {
		this.orgname = orgname;
	}

	public String getPing_test_times() {
		return ping_test_times;
	}

	public void setPing_test_times(String ping_test_times) {
		this.ping_test_times = ping_test_times;
	}

	public String getBroadband_type() {
		return broadband_type;
	}

	public void setBroadband_type(String broadband_type) {
		this.broadband_type = broadband_type;
	}

	public String getPing_delay() {
		return ping_delay;
	}

	public void setPing_delay(String ping_delay) {
		this.ping_delay = ping_delay;
	}

	public String getPing_loss_rate() {
		return ping_loss_rate;
	}

	public void setPing_loss_rate(String ping_loss_rate) {
		this.ping_loss_rate = ping_loss_rate;
	}

	public String getBest_ping_delay() {
		return best_ping_delay;
	}

	public void setBest_ping_delay(String best_ping_delay) {
		this.best_ping_delay = best_ping_delay;
	}

	public String getBest_ping_loss_rate() {
		return best_ping_loss_rate;
	}

	public void setBest_ping_loss_rate(String best_ping_loss_rate) {
		this.best_ping_loss_rate = best_ping_loss_rate;
	}

	public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
	}
	
}
