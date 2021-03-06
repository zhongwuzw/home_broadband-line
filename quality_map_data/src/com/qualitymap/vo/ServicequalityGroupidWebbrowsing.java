package com.qualitymap.vo;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table(name = "servicequality_groupid_webbrowsing")
public class ServicequalityGroupidWebbrowsing {

	@Id
	private String id;
	private String month;
	private String groupid;
	private String groupname;
	private String page_test_times;
	private String broadband_type;
	private String page_avg_delay;
	private String page_success_rate;
	private String best_page_delay;
	private String best_page_success_rate;
	private String top95_page_success_rate;
	private String top85_page_success_rate;
	private String top75_page_success_rate;
	private String top95_page_delay;
	private String top85_page_delay;
	private String top75_page_delay;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

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

	public String getPage_test_times() {
		return page_test_times;
	}

	public void setPage_test_times(String page_test_times) {
		this.page_test_times = page_test_times;
	}

	public String getBroadband_type() {
		return broadband_type;
	}

	public void setBroadband_type(String broadband_type) {
		this.broadband_type = broadband_type;
	}

	public String getPage_avg_delay() {
		return page_avg_delay;
	}

	public void setPage_avg_delay(String page_avg_delay) {
		this.page_avg_delay = page_avg_delay;
	}

	public String getPage_success_rate() {
		return page_success_rate;
	}

	public void setPage_success_rate(String page_success_rate) {
		this.page_success_rate = page_success_rate;
	}

	public String getBest_page_delay() {
		return best_page_delay;
	}

	public void setBest_page_delay(String best_page_delay) {
		this.best_page_delay = best_page_delay;
	}

	public String getBest_page_success_rate() {
		return best_page_success_rate;
	}

	public void setBest_page_success_rate(String best_page_success_rate) {
		this.best_page_success_rate = best_page_success_rate;
	}

	public String getTop95_page_success_rate() {
		return top95_page_success_rate;
	}

	public void setTop95_page_success_rate(String top95_page_success_rate) {
		this.top95_page_success_rate = top95_page_success_rate;
	}

	public String getTop85_page_success_rate() {
		return top85_page_success_rate;
	}

	public void setTop85_page_success_rate(String top85_page_success_rate) {
		this.top85_page_success_rate = top85_page_success_rate;
	}

	public String getTop75_page_success_rate() {
		return top75_page_success_rate;
	}

	public void setTop75_page_success_rate(String top75_page_success_rate) {
		this.top75_page_success_rate = top75_page_success_rate;
	}

	public String getTop95_page_delay() {
		return top95_page_delay;
	}

	public void setTop95_page_delay(String top95_page_delay) {
		this.top95_page_delay = top95_page_delay;
	}

	public String getTop85_page_delay() {
		return top85_page_delay;
	}

	public void setTop85_page_delay(String top85_page_delay) {
		this.top85_page_delay = top85_page_delay;
	}

	public String getTop75_page_delay() {
		return top75_page_delay;
	}

	public void setTop75_page_delay(String top75_page_delay) {
		this.top75_page_delay = top75_page_delay;
	}

}
