package com.cmri.ots.sso.bean;

import java.io.Serializable;
import java.util.Date;

public class UserStatues implements Serializable{

	private Integer id;
	private Integer user_id;
	private String name;
	private Integer create_id;
	private Date create_time;
	private Integer update_id;
	private Date update_time;
	private String username;
	private Date last_time;
	private String last_ip;
	private Integer count;
	private Integer user_agent_id;
	private String description;
	private String last_login_host;
	public Integer getId() {
		return id;
	}
	public void setId(Integer id) {
		this.id = id;
	}
	public Integer getUser_id() {
		return user_id;
	}
	public void setUser_id(Integer user_id) {
		this.user_id = user_id;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public Integer getCreate_id() {
		return create_id;
	}
	public void setCreate_id(Integer create_id) {
		this.create_id = create_id;
	}
	public Integer getUpdate_id() {
		return update_id;
	}
	public void setUpdate_id(Integer update_id) {
		this.update_id = update_id;
	}
	public String getUsername() {
		return username;
	}
	public void setUsername(String username) {
		this.username = username;
	}
	
	public Date getLast_time() {
		return last_time;
	}
	public void setLast_time(Date last_time) {
		this.last_time = last_time;
	}
	public String getLast_ip() {
		return last_ip;
	}
	public void setLast_ip(String last_ip) {
		this.last_ip = last_ip;
	}
	public Integer getCount() {
		return count;
	}
	public void setCount(Integer count) {
		this.count = count;
	}
	public Integer getUser_agent_id() {
		return user_agent_id;
	}
	public void setUser_agent_id(Integer user_agent_id) {
		this.user_agent_id = user_agent_id;
	}
	public String getDescription() {
		return description;
	}
	public void setDescription(String description) {
		this.description = description;
	}
	public String getLast_login_host() {
		return last_login_host;
	}
	public void setLast_login_host(String last_login_host) {
		this.last_login_host = last_login_host;
	}
	public Date getCreate_time() {
		return create_time;
	}
	public void setCreate_time(Date create_time) {
		this.create_time = create_time;
	}
	public Date getUpdate_time() {
		return update_time;
	}
	public void setUpdate_time(Date update_time) {
		this.update_time = update_time;
	}
	
}
